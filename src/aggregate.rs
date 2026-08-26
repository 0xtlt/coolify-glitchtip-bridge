use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::{
    classify::Classifier,
    config::Config,
    event::{BridgeEvent, EventLevel},
    ingest::LogRecord,
    redact::Redactor,
    sink::EventSink,
};

#[derive(Default)]
pub struct BridgeStats {
    received_records: AtomicU64,
    received_webhooks: AtomicU64,
    filtered_records: AtomicU64,
    emitted_events: AtomicU64,
    dropped_submissions: AtomicU64,
    sink_failures: AtomicU64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct BridgeStatsSnapshot {
    pub received_records: u64,
    pub received_webhooks: u64,
    pub filtered_records: u64,
    pub emitted_events: u64,
    pub dropped_submissions: u64,
    pub sink_failures: u64,
}

impl BridgeStats {
    pub fn snapshot(&self) -> BridgeStatsSnapshot {
        BridgeStatsSnapshot {
            received_records: self.received_records.load(Ordering::Relaxed),
            received_webhooks: self.received_webhooks.load(Ordering::Relaxed),
            filtered_records: self.filtered_records.load(Ordering::Relaxed),
            emitted_events: self.emitted_events.load(Ordering::Relaxed),
            dropped_submissions: self.dropped_submissions.load(Ordering::Relaxed),
            sink_failures: self.sink_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct BridgeHandle {
    sender: mpsc::Sender<Command>,
    stats: Arc<BridgeStats>,
}

impl BridgeHandle {
    pub fn submit_logs(&self, records: Vec<LogRecord>) -> Result<(), SubmitError> {
        self.sender
            .try_send(Command::Logs(records))
            .map_err(|error| {
                self.stats
                    .dropped_submissions
                    .fetch_add(1, Ordering::Relaxed);
                match error {
                    mpsc::error::TrySendError::Full(_) => SubmitError::Full,
                    mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
                }
            })
    }

    pub fn submit_webhook(&self, payload: Value) -> Result<(), SubmitError> {
        self.sender
            .try_send(Command::Webhook(payload))
            .map_err(|error| {
                self.stats
                    .dropped_submissions
                    .fetch_add(1, Ordering::Relaxed);
                match error {
                    mpsc::error::TrySendError::Full(_) => SubmitError::Full,
                    mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
                }
            })
    }

    pub fn stats(&self) -> Arc<BridgeStats> {
        self.stats.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    Full,
    Closed,
}

pub struct BridgeRuntime {
    pub handle: BridgeHandle,
    sender: mpsc::Sender<Command>,
    task: tokio::task::JoinHandle<()>,
}

impl BridgeRuntime {
    pub fn spawn(config: &Config, sink: Arc<dyn EventSink>) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let stats = Arc::new(BridgeStats::default());
        let handle = BridgeHandle {
            sender: sender.clone(),
            stats: stats.clone(),
        };
        let worker = Worker::new(config, sink, stats);
        let task = tokio::spawn(worker.run(receiver));
        Self {
            handle,
            sender,
            task,
        }
    }

    pub async fn shutdown(self) {
        let (finished_tx, finished_rx) = oneshot::channel();
        let _ = self.sender.send(Command::Shutdown(finished_tx)).await;
        let _ = finished_rx.await;
        let _ = self.task.await;
    }
}

enum Command {
    Logs(Vec<LogRecord>),
    Webhook(Value),
    Shutdown(oneshot::Sender<()>),
}

struct Worker {
    aggregator: Aggregator,
    sink: Arc<dyn EventSink>,
    stats: Arc<BridgeStats>,
    redactor: Redactor,
    webhook_include_success: bool,
    max_event_bytes: usize,
}

impl Worker {
    fn new(config: &Config, sink: Arc<dyn EventSink>, stats: Arc<BridgeStats>) -> Self {
        let redactor = Redactor::default();
        Self {
            aggregator: Aggregator::new(config),
            sink,
            stats,
            redactor,
            webhook_include_success: config.webhook_include_success,
            max_event_bytes: config.max_event_bytes,
        }
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<Command>) {
        let tick_every = self
            .aggregator
            .timeout
            .min(Duration::from_millis(500))
            .max(Duration::from_millis(25));
        let mut tick = tokio::time::interval(tick_every);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let events = self.aggregator.flush_expired(Instant::now());
                    self.emit_all(events);
                }
                command = receiver.recv() => {
                    match command {
                        Some(Command::Logs(records)) => {
                            self.stats.received_records.fetch_add(records.len() as u64, Ordering::Relaxed);
                            for mut record in records {
                                record.redact(&self.redactor);
                                let before = self.aggregator.filtered;
                                let events = self.aggregator.ingest(record, Instant::now());
                                let filtered = self.aggregator.filtered.saturating_sub(before);
                                self.stats.filtered_records.fetch_add(filtered, Ordering::Relaxed);
                                self.emit_all(events);
                            }
                        }
                        Some(Command::Webhook(mut payload)) => {
                            self.stats.received_webhooks.fetch_add(1, Ordering::Relaxed);
                            self.redactor.value(&mut payload);
                            match webhook_event(payload, self.webhook_include_success, self.max_event_bytes) {
                                Ok(Some(event)) => self.emit(event),
                                Ok(None) => {
                                    self.stats.filtered_records.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(error) => {
                                    self.stats.filtered_records.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(%error, "discarded invalid Coolify webhook");
                                }
                            }
                        }
                        Some(Command::Shutdown(finished)) => {
                            let events = self.aggregator.drain();
                            self.emit_all(events);
                            let _ = finished.send(());
                            return;
                        }
                        None => {
                            let events = self.aggregator.drain();
                            self.emit_all(events);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn emit_all(&self, events: Vec<BridgeEvent>) {
        for event in events {
            self.emit(event);
        }
    }

    fn emit(&self, event: BridgeEvent) {
        match self.sink.capture(event) {
            Ok(()) => {
                self.stats.emitted_events.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                self.stats.sink_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(%error, "failed to enqueue event for GlitchTip");
            }
        }
    }
}

struct Aggregator {
    pending: HashMap<String, PendingLog>,
    classifier: Classifier,
    timeout: Duration,
    max_bytes: usize,
    max_lines: usize,
    filtered: u64,
}

struct PendingLog {
    record: LogRecord,
    level: EventLevel,
    lines: Vec<String>,
    bytes: usize,
    updated_at: Instant,
}

impl Aggregator {
    fn new(config: &Config) -> Self {
        Self {
            pending: HashMap::new(),
            classifier: Classifier::new(
                config.min_level,
                config.stderr_as_error,
                config.ignore_regex.clone(),
            ),
            timeout: config.multiline_timeout,
            max_bytes: config.max_event_bytes,
            max_lines: config.max_event_lines,
            filtered: 0,
        }
    }

    fn ingest(&mut self, record: LogRecord, now: Instant) -> Vec<BridgeEvent> {
        let key = record.source_key();
        let mut events = Vec::new();
        let is_multiline = record.message.contains('\n');

        if is_multiline {
            if let Some(pending) = self.pending.remove(&key) {
                events.push(pending.into_event(self.max_bytes));
            }
            if let Some(level) = self.classifier.should_emit(&record) {
                events.push(PendingLog::new(record, level, now).into_event(self.max_bytes));
            } else {
                self.filtered += 1;
            }
            return events;
        }

        if self.classifier.is_continuation(&record.message)
            && let Some(pending) = self.pending.get_mut(&key)
        {
            if pending.lines.len() >= self.max_lines
                || pending.bytes + record.message.len() + 1 > self.max_bytes
            {
                let pending = self.pending.remove(&key).unwrap();
                events.push(pending.into_event(self.max_bytes));
            } else {
                pending.bytes += record.message.len() + 1;
                pending.lines.push(record.message);
                pending.updated_at = now;
                return events;
            }
        }

        if let Some(pending) = self.pending.remove(&key) {
            events.push(pending.into_event(self.max_bytes));
        }

        if let Some(level) = self.classifier.should_emit(&record) {
            self.pending
                .insert(key, PendingLog::new(record, level, now));
        } else {
            self.filtered += 1;
        }
        events
    }

    fn flush_expired(&mut self, now: Instant) -> Vec<BridgeEvent> {
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, pending)| now.duration_since(pending.updated_at) >= self.timeout)
            .map(|(key, _)| key.clone())
            .collect();
        expired
            .into_iter()
            .filter_map(|key| self.pending.remove(&key))
            .map(|pending| pending.into_event(self.max_bytes))
            .collect()
    }

    fn drain(&mut self) -> Vec<BridgeEvent> {
        self.pending
            .drain()
            .map(|(_, pending)| pending.into_event(self.max_bytes))
            .collect()
    }
}

impl PendingLog {
    fn new(record: LogRecord, level: EventLevel, now: Instant) -> Self {
        let lines: Vec<String> = record.message.lines().map(str::to_owned).collect();
        let bytes = record.message.len();
        Self {
            record,
            level,
            lines,
            bytes,
            updated_at: now,
        }
    }

    fn into_event(self, max_bytes: usize) -> BridgeEvent {
        let full_log = truncate_utf8(&self.lines.join("\n"), max_bytes);
        let first_line = full_log
            .lines()
            .next()
            .unwrap_or("Coolify application error");
        let message = truncate_utf8(first_line, 1024);
        let tags = log_tags(&self.record);
        let fingerprint = stable_fingerprint(
            "coolify-log",
            &[
                self.record.app_name.as_deref().unwrap_or("unknown"),
                &normalize_message(first_line),
            ],
        );
        let mut extra = BTreeMap::new();
        extra.insert("log".into(), Value::String(full_log));
        extra.insert("record".into(), self.record.raw);
        if let Some(timestamp) = self.record.timestamp {
            extra.insert("source_timestamp".into(), Value::String(timestamp));
        }

        BridgeEvent {
            message,
            level: self.level,
            tags,
            extra,
            fingerprint: vec![fingerprint],
        }
    }
}

fn log_tags(record: &LogRecord) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::from([("source".into(), "coolify-log-drain".into())]);
    insert_tag(&mut tags, "coolify.app", record.app_name.as_deref());
    insert_tag(&mut tags, "coolify.project", record.project_name.as_deref());
    insert_tag(
        &mut tags,
        "coolify.environment",
        record.environment_name.as_deref(),
    );
    insert_tag(&mut tags, "coolify.server", record.server_name.as_deref());
    insert_tag(&mut tags, "coolify.server_ip", record.server_ip.as_deref());
    insert_tag(&mut tags, "container", record.container_name.as_deref());
    tags
}

fn webhook_event(
    payload: Value,
    include_success: bool,
    max_event_bytes: usize,
) -> Result<Option<BridgeEvent>> {
    let object = payload
        .as_object()
        .context("webhook payload must be a JSON object")?;
    let event_name = object
        .get("event")
        .and_then(Value::as_str)
        .context("webhook payload is missing event")?
        .to_owned();
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .context("webhook payload is missing message")?
        .to_owned();
    let success = object
        .get("success")
        .and_then(Value::as_bool)
        .context("webhook payload is missing success")?;

    let operational_warning = event_name == "container_restarted"
        || event_name.contains("warning")
        || event_name.contains("unreachable")
        || event_name.contains("outdated");
    if success && !include_success && !operational_warning {
        return Ok(None);
    }

    let level = if !success {
        EventLevel::Error
    } else if operational_warning {
        EventLevel::Warning
    } else {
        EventLevel::Info
    };
    let mut tags = BTreeMap::from([
        ("source".into(), "coolify-webhook".into()),
        ("coolify.event".into(), truncate_utf8(&event_name, 200)),
        ("coolify.success".into(), success.to_string()),
    ]);
    for (field, tag) in [
        ("application_name", "coolify.app"),
        ("application_uuid", "coolify.app_uuid"),
        ("project", "coolify.project"),
        ("environment", "coolify.environment"),
        ("database_name", "coolify.database"),
        ("server_name", "coolify.server"),
        ("container_name", "container"),
        ("task_name", "coolify.task"),
    ] {
        insert_tag(&mut tags, tag, object.get(field).and_then(Value::as_str));
    }

    let resource = [
        "application_uuid",
        "database_uuid",
        "task_uuid",
        "server_uuid",
        "container_name",
    ]
    .iter()
    .find_map(|field| object.get(*field).and_then(Value::as_str))
    .unwrap_or("global")
    .to_owned();
    let error_output = object
        .get("error_output")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let fingerprint = stable_fingerprint("coolify-webhook", &[&event_name, &resource]);
    let serialized_payload = serde_json::to_string(&payload).unwrap_or_default();
    let payload_extra = if serialized_payload.len() > max_event_bytes {
        json!({
            "truncated": true,
            "preview": truncate_utf8(&serialized_payload, max_event_bytes),
        })
    } else {
        payload
    };
    let mut extra = BTreeMap::from([("payload".into(), payload_extra)]);
    if let Some(error_output) = error_output {
        extra.insert(
            "error_output".into(),
            Value::String(truncate_utf8(&error_output, max_event_bytes)),
        );
    }

    Ok(Some(BridgeEvent {
        message: truncate_utf8(&format!("Coolify {event_name}: {message}"), 2048),
        level,
        tags,
        extra,
        fingerprint: vec![fingerprint],
    }))
}

fn insert_tag(tags: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        tags.insert(key.to_owned(), truncate_utf8(value, 200));
    }
}

fn stable_fingerprint(namespace: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace.as_bytes());
    for part in parts {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    format!("{namespace}:{:x}", hasher.finalize())
}

fn normalize_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut in_digits = false;
    for character in message.chars() {
        if character.is_ascii_digit() {
            if !in_digits {
                output.push('#');
            }
            in_digits = true;
        } else {
            in_digits = false;
            output.extend(character.to_lowercase());
        }
    }
    output
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let suffix = "\n… [truncated by coolify-glitchtip-bridge]";
    let mut end = max_bytes.saturating_sub(suffix.len()).min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &value[..end], suffix)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::Config;
    use serde_json::json;

    fn config() -> Config {
        Config::from_map(HashMap::from([
            ("AUTH_TOKEN".into(), "0123456789abcdef".into()),
            (
                "GLITCHTIP_DSN".into(),
                "https://public@example.invalid/42".into(),
            ),
            ("MULTILINE_TIMEOUT_MS".into(), "10".into()),
        ]))
        .unwrap()
    }

    #[test]
    fn groups_multiline_stack_traces_into_one_event() {
        let mut aggregator = Aggregator::new(&config());
        let now = Instant::now();
        let first = LogRecord::from_value(json!({
            "log": "Error: database unavailable",
            "coolify.app_name": "api"
        }))
        .unwrap();
        let frame = LogRecord::from_value(json!({
            "log": "    at connect (/app/db.js:12:4)",
            "coolify.app_name": "api"
        }))
        .unwrap();

        assert!(aggregator.ingest(first, now).is_empty());
        assert!(aggregator.ingest(frame, now).is_empty());
        let events = aggregator.flush_expired(now + Duration::from_millis(11));
        assert_eq!(events.len(), 1);
        assert!(
            events[0].extra["log"]
                .as_str()
                .unwrap()
                .contains("at connect")
        );
        assert_eq!(events[0].tags["coolify.app"], "api");
    }

    #[test]
    fn ignores_info_logs_by_default() {
        let mut aggregator = Aggregator::new(&config());
        let record = LogRecord::from_value(json!({"log": "server listening on 8080"})).unwrap();
        assert!(aggregator.ingest(record, Instant::now()).is_empty());
        assert!(aggregator.drain().is_empty());
    }

    #[test]
    fn forwards_failed_and_restart_webhooks_but_not_successes() {
        let failed = webhook_event(
            json!({
                "success": false,
                "event": "deployment_failed",
                "message": "Deployment failed",
                "application_uuid": "app-1"
            }),
            false,
            65536,
        )
        .unwrap();
        assert_eq!(failed.unwrap().level, EventLevel::Error);

        let success = webhook_event(
            json!({
                "success": true,
                "event": "deployment_success",
                "message": "Deployed"
            }),
            false,
            65536,
        )
        .unwrap();
        assert!(success.is_none());

        let restarted = webhook_event(
            json!({
                "success": true,
                "event": "container_restarted",
                "message": "Restarted",
                "container_name": "api"
            }),
            false,
            65536,
        )
        .unwrap();
        assert_eq!(restarted.unwrap().level, EventLevel::Warning);
    }

    #[test]
    fn produces_stable_fingerprints_without_high_cardinality_numbers() {
        assert_eq!(
            normalize_message("request 123 failed on port 5432"),
            "request # failed on port #"
        );
        assert_eq!(
            stable_fingerprint("test", &["api", "error #"]),
            stable_fingerprint("test", &["api", "error #"])
        );
    }

    #[test]
    fn truncates_on_utf8_boundaries() {
        let value = "é".repeat(100);
        let truncated = truncate_utf8(&value, 50);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.contains("truncated"));
    }
}
