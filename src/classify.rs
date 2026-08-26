use std::str::FromStr;

use regex::Regex;

use crate::{event::EventLevel, ingest::LogRecord};

#[derive(Clone)]
pub struct Classifier {
    fatal: Regex,
    error: Regex,
    warning: Regex,
    continuation: Regex,
    min_level: EventLevel,
    stderr_as_error: bool,
    ignore: Option<Regex>,
}

impl Classifier {
    pub fn new(min_level: EventLevel, stderr_as_error: bool, ignore: Option<Regex>) -> Self {
        Self {
            fatal: Regex::new(
                r"(?i)(^|\b)(fatal|panic(?:ked)?|critical|segmentation fault|out of memory|oom killed)(\b|:)",
            )
            .unwrap(),
            error: Regex::new(
                r"(?i)(^|\b)(error|exception|unhandled(?: rejection)?|failed|failure|traceback)(\b|:)",
            )
            .unwrap(),
            warning: Regex::new(r"(?i)(^|\b)(warn(?:ing)?|deprecated)(\b|:)").unwrap(),
            continuation: Regex::new(
                r"(?x)
                ^\s+(at\s|File\s|in\s|\.{3}|\^)|
                ^(at\s|Caused\s+by:|Traceback\s|stack\s+backtrace:|goroutine\s|\.{3}\s)|
                ^[A-Za-z0-9_.:$<>]+\([^)]*\)\s*$
                ",
            )
            .unwrap(),
            min_level,
            stderr_as_error,
            ignore,
        }
    }

    pub fn classify(&self, record: &LogRecord) -> EventLevel {
        if let Some(level) = record.explicit_level.as_deref() {
            if let Ok(level) = EventLevel::from_str(level) {
                return level;
            }
        }
        if self.fatal.is_match(&record.message) {
            EventLevel::Fatal
        } else if self.error.is_match(&record.message) {
            EventLevel::Error
        } else if self.warning.is_match(&record.message) {
            EventLevel::Warning
        } else if self.stderr_as_error && record.stream.as_deref() == Some("stderr") {
            EventLevel::Error
        } else {
            EventLevel::Info
        }
    }

    pub fn should_emit(&self, record: &LogRecord) -> Option<EventLevel> {
        if self
            .ignore
            .as_ref()
            .is_some_and(|regex| regex.is_match(&record.message))
        {
            return None;
        }
        let level = self.classify(record);
        (level >= self.min_level).then_some(level)
    }

    pub fn is_continuation(&self, message: &str) -> bool {
        self.continuation.is_match(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::LogRecord;
    use serde_json::json;

    #[test]
    fn explicit_levels_win_over_message_heuristics() {
        let classifier = Classifier::new(EventLevel::Error, false, None);
        let record = LogRecord::from_value(json!({
            "log": "routine request failed over to replica",
            "level": "info"
        }))
        .unwrap();
        assert_eq!(classifier.classify(&record), EventLevel::Info);
        assert!(classifier.should_emit(&record).is_none());
    }

    #[test]
    fn recognizes_stack_trace_continuations() {
        let classifier = Classifier::new(EventLevel::Error, false, None);
        assert!(classifier.is_continuation("    at handler (/app/index.js:4:2)"));
        assert!(classifier.is_continuation("Caused by: connection reset"));
        assert!(!classifier.is_continuation("server started on port 8080"));
    }
}
