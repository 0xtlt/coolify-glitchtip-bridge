use std::borrow::Cow;

use anyhow::Result;

use crate::event::{BridgeEvent, EventLevel};

pub trait EventSink: Send + Sync {
    fn capture(&self, event: BridgeEvent) -> Result<()>;
}

#[derive(Default)]
pub struct SentrySink;

impl EventSink for SentrySink {
    fn capture(&self, event: BridgeEvent) -> Result<()> {
        let extra = event.extra.into_iter().collect();
        let sentry_event = sentry::protocol::Event {
            message: Some(event.message),
            level: sentry_level(event.level),
            logger: Some("coolify-glitchtip-bridge".into()),
            tags: event.tags,
            extra,
            fingerprint: Cow::Owned(event.fingerprint.into_iter().map(Cow::Owned).collect()),
            ..Default::default()
        };
        sentry::capture_event(sentry_event);
        Ok(())
    }
}

fn sentry_level(level: EventLevel) -> sentry::Level {
    match level {
        EventLevel::Debug => sentry::Level::Debug,
        EventLevel::Info => sentry::Level::Info,
        EventLevel::Warning => sentry::Level::Warning,
        EventLevel::Error => sentry::Level::Error,
        EventLevel::Fatal => sentry::Level::Fatal,
    }
}
