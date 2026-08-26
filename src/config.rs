use std::{collections::HashMap, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use regex::Regex;

use crate::event::EventLevel;

pub struct Config {
    pub bind_addr: SocketAddr,
    pub glitchtip_dsn: String,
    pub auth_token: String,
    pub environment: String,
    pub release: Option<String>,
    pub min_level: EventLevel,
    pub stderr_as_error: bool,
    pub webhook_include_success: bool,
    pub multiline_timeout: Duration,
    pub max_event_bytes: usize,
    pub max_event_lines: usize,
    pub max_request_bytes: usize,
    pub max_records_per_request: usize,
    pub queue_capacity: usize,
    pub shutdown_timeout: Duration,
    pub ignore_regex: Option<Regex>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(values: HashMap<String, String>) -> Result<Self> {
        let required = |name: &str| -> Result<String> {
            values
                .get(name)
                .filter(|value| !value.trim().is_empty())
                .cloned()
                .with_context(|| format!("missing required environment variable {name}"))
        };
        let parse = |name: &str, default: &str| -> Result<String> {
            Ok(values
                .get(name)
                .cloned()
                .unwrap_or_else(|| default.to_owned()))
        };

        let auth_token = required("AUTH_TOKEN")?;
        if auth_token.len() < 16 {
            bail!("AUTH_TOKEN must contain at least 16 characters");
        }

        let glitchtip_dsn = required("GLITCHTIP_DSN")?;
        glitchtip_dsn
            .parse::<sentry::types::Dsn>()
            .context("GLITCHTIP_DSN is not a valid Sentry-compatible DSN")?;

        let ignore_regex = values
            .get("IGNORE_REGEX")
            .filter(|value| !value.is_empty())
            .map(|value| Regex::new(value).context("IGNORE_REGEX is invalid"))
            .transpose()?;

        let config = Self {
            bind_addr: parse_value(&parse("BIND_ADDR", "0.0.0.0:8080")?, "BIND_ADDR")?,
            glitchtip_dsn,
            auth_token,
            environment: parse("ENVIRONMENT", "production")?,
            release: values
                .get("RELEASE")
                .filter(|value| !value.is_empty())
                .cloned(),
            min_level: EventLevel::from_str(&parse("MIN_LEVEL", "error")?)
                .context("MIN_LEVEL must be debug, info, warning, error, or fatal")?,
            stderr_as_error: parse_bool(&parse("STDERR_AS_ERROR", "false")?, "STDERR_AS_ERROR")?,
            webhook_include_success: parse_bool(
                &parse("WEBHOOK_INCLUDE_SUCCESS", "false")?,
                "WEBHOOK_INCLUDE_SUCCESS",
            )?,
            multiline_timeout: Duration::from_millis(parse_value(
                &parse("MULTILINE_TIMEOUT_MS", "1500")?,
                "MULTILINE_TIMEOUT_MS",
            )?),
            max_event_bytes: parse_value(&parse("MAX_EVENT_BYTES", "65536")?, "MAX_EVENT_BYTES")?,
            max_event_lines: parse_value(&parse("MAX_EVENT_LINES", "128")?, "MAX_EVENT_LINES")?,
            max_request_bytes: parse_value(
                &parse("MAX_REQUEST_BYTES", "1048576")?,
                "MAX_REQUEST_BYTES",
            )?,
            max_records_per_request: parse_value(
                &parse("MAX_RECORDS_PER_REQUEST", "1000")?,
                "MAX_RECORDS_PER_REQUEST",
            )?,
            queue_capacity: parse_value(&parse("QUEUE_CAPACITY", "2048")?, "QUEUE_CAPACITY")?,
            shutdown_timeout: Duration::from_secs(parse_value(
                &parse("SHUTDOWN_TIMEOUT_SECONDS", "5")?,
                "SHUTDOWN_TIMEOUT_SECONDS",
            )?),
            ignore_regex,
        };
        for (name, value) in [
            ("MAX_EVENT_BYTES", config.max_event_bytes),
            ("MAX_EVENT_LINES", config.max_event_lines),
            ("MAX_REQUEST_BYTES", config.max_request_bytes),
            ("MAX_RECORDS_PER_REQUEST", config.max_records_per_request),
            ("QUEUE_CAPACITY", config.queue_capacity),
        ] {
            if value == 0 {
                bail!("{name} must be greater than zero");
            }
        }
        Ok(config)
    }
}

fn parse_value<T>(value: &str, name: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse::<T>()
        .with_context(|| format!("{name} has an invalid value"))
}

fn parse_bool(value: &str, name: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_required_secrets_without_exposing_them() {
        let error = match Config::from_map(HashMap::new()) {
            Ok(_) => panic!("configuration unexpectedly succeeded"),
            Err(error) => error.to_string(),
        };
        assert_eq!(error, "missing required environment variable AUTH_TOKEN");
    }

    #[test]
    fn accepts_a_glitchtip_compatible_dsn() {
        let config = Config::from_map(HashMap::from([
            ("AUTH_TOKEN".into(), "0123456789abcdef".into()),
            (
                "GLITCHTIP_DSN".into(),
                "https://public@example.invalid/42".into(),
            ),
        ]))
        .unwrap();

        assert_eq!(config.bind_addr.port(), 8080);
        assert_eq!(config.min_level, EventLevel::Error);
    }

    #[test]
    fn rejects_a_zero_capacity_queue() {
        let error = Config::from_map(HashMap::from([
            ("AUTH_TOKEN".into(), "0123456789abcdef".into()),
            (
                "GLITCHTIP_DSN".into(),
                "https://public@example.invalid/42".into(),
            ),
            ("QUEUE_CAPACITY".into(), "0".into()),
        ]))
        .err()
        .unwrap();
        assert_eq!(
            error.to_string(),
            "QUEUE_CAPACITY must be greater than zero"
        );
    }
}
