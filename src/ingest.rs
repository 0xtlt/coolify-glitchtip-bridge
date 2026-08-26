use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use crate::redact::Redactor;

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub message: String,
    pub explicit_level: Option<String>,
    pub stream: Option<String>,
    pub app_name: Option<String>,
    pub project_name: Option<String>,
    pub environment_name: Option<String>,
    pub server_name: Option<String>,
    pub server_ip: Option<String>,
    pub container_name: Option<String>,
    pub container_id: Option<String>,
    pub timestamp: Option<String>,
    pub raw: Value,
}

impl LogRecord {
    pub fn from_value(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("each log record must be a JSON object")?;
        let message = first_string(object, &["log", "message", "msg"])
            .or_else(|| nested_string(object, "event", "message"))
            .context("log record has no string log, message, or msg field")?;

        Ok(Self {
            message,
            explicit_level: first_string(
                object,
                &["level", "severity", "severity_text", "log.level"],
            ),
            stream: first_string(object, &["stream", "source"])
                .map(|value| value.to_ascii_lowercase()),
            app_name: first_string(
                object,
                &[
                    "coolify.app_name",
                    "COOLIFY_APP_NAME",
                    "application_name",
                    "service_name",
                ],
            )
            .or_else(|| nested_string(object, "coolify", "app_name")),
            project_name: first_string(
                object,
                &["coolify.project_name", "COOLIFY_PROJECT_NAME", "project"],
            )
            .or_else(|| nested_string(object, "coolify", "project_name")),
            environment_name: first_string(
                object,
                &[
                    "coolify.environment_name",
                    "COOLIFY_ENVIRONMENT_NAME",
                    "environment",
                ],
            )
            .or_else(|| nested_string(object, "coolify", "environment_name")),
            server_name: first_string(object, &["coolify.server_name", "server_name", "host"])
                .or_else(|| nested_string(object, "coolify", "server_name")),
            server_ip: first_string(object, &["coolify.server_ip", "COOLIFY_SERVER_IP"])
                .or_else(|| nested_string(object, "coolify", "server_ip")),
            container_name: first_string(
                object,
                &["container_name", "container", "docker.container_name"],
            ),
            container_id: first_string(
                object,
                &["container_id", "container_id_full", "docker.container_id"],
            ),
            timestamp: first_string(object, &["timestamp", "@timestamp", "_time", "time"]),
            raw: value,
        })
    }

    pub fn source_key(&self) -> String {
        [
            self.container_id.as_deref(),
            self.container_name.as_deref(),
            self.app_name.as_deref(),
            self.server_name.as_deref(),
        ]
        .into_iter()
        .flatten()
        .next()
        .unwrap_or("unknown")
        .to_owned()
    }

    pub fn redact(&mut self, redactor: &Redactor) {
        self.message = redactor.text(&self.message);
        redactor.value(&mut self.raw);
    }
}

pub fn parse_log_batch(body: &[u8], max_records: usize) -> Result<Vec<LogRecord>> {
    let body = std::str::from_utf8(body).context("request body must be UTF-8")?;
    let trimmed = body.trim();
    if trimmed.is_empty() {
        bail!("request body is empty");
    }

    let values = match serde_json::from_str::<Value>(trimmed) {
        Ok(Value::Array(values)) => values,
        Ok(Value::Object(mut object)) => {
            if let Some(Value::Array(values)) = object.remove("records") {
                values
            } else {
                vec![Value::Object(object)]
            }
        }
        Ok(_) => bail!("JSON payload must be an object or array"),
        Err(json_error) => {
            let mut values = Vec::new();
            for line in trimmed.lines().filter(|line| !line.trim().is_empty()) {
                values
                    .push(serde_json::from_str::<Value>(line).with_context(|| {
                        format!("invalid JSON or NDJSON payload: {json_error}")
                    })?);
            }
            values
        }
    };

    if values.len() > max_records {
        bail!("payload contains more than {max_records} log records");
    }
    values.into_iter().map(LogRecord::from_value).collect()
}

fn first_string(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str).map(str::to_owned))
}

fn nested_string(object: &Map<String, Value>, parent: &str, child: &str) -> Option<String> {
    object
        .get(parent)
        .and_then(Value::as_object)
        .and_then(|value| value.get(child))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fluent_bit_json_batches() {
        let records = parse_log_batch(
            br#"[{"log":"Error: boom","coolify.app_name":"api"},{"log":"    at main"}]"#,
            10,
        )
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].app_name.as_deref(), Some("api"));
    }

    #[test]
    fn parses_ndjson() {
        let records = parse_log_batch(
            b"{\"message\":\"Error one\"}\n{\"message\":\"Error two\"}\n",
            10,
        )
        .unwrap();
        assert_eq!(records.len(), 2);
    }
}
