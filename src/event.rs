use std::{collections::BTreeMap, str::FromStr};

use anyhow::bail;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventLevel {
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

impl FromStr for EventLevel {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "trace" | "debug" => Ok(Self::Debug),
            "info" | "notice" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warning),
            "err" | "error" => Ok(Self::Error),
            "crit" | "critical" | "alert" | "emerg" | "emergency" | "fatal" => Ok(Self::Fatal),
            _ => bail!("unknown event level"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BridgeEvent {
    pub message: String,
    pub level: EventLevel,
    pub tags: BTreeMap<String, String>,
    pub extra: BTreeMap<String, Value>,
    pub fingerprint: Vec<String>,
}
