use regex::{Captures, Regex};
use serde_json::Value;

#[derive(Clone)]
pub struct Redactor {
    bearer: Regex,
    named_secret: Regex,
    url_password: Regex,
}

impl Default for Redactor {
    fn default() -> Self {
        Self {
            bearer: Regex::new(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+").unwrap(),
            named_secret: Regex::new(
                r#"(?i)\b(authorization|api[_-]?key|access[_-]?token|token|password|passwd|secret|dsn)(\s*[:=]\s*)([^\s,;]+)"#,
            )
            .unwrap(),
            url_password: Regex::new(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s:/@]+):([^\s/@]+)@")
                .unwrap(),
        }
    }
}

impl Redactor {
    pub fn text(&self, input: &str) -> String {
        let value = self.bearer.replace_all(input, "Bearer [REDACTED]");
        let value = self
            .named_secret
            .replace_all(&value, |captures: &Captures<'_>| {
                format!("{}{}[REDACTED]", &captures[1], &captures[2])
            });
        self.url_password
            .replace_all(&value, "$1:[REDACTED]@")
            .into_owned()
    }

    pub fn value(&self, value: &mut Value) {
        match value {
            Value::String(text) => *text = self.text(text),
            Value::Array(values) => values.iter_mut().for_each(|value| self.value(value)),
            Value::Object(values) => {
                for (key, value) in values {
                    if is_secret_key(key) {
                        *value = Value::String("[REDACTED]".into());
                    } else {
                        self.value(value);
                    }
                }
            }
            _ => {}
        }
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "api_key",
        "apikey",
        "access_token",
        "token",
        "password",
        "passwd",
        "secret",
        "dsn",
    ]
    .iter()
    .any(|secret| key == *secret || key.ends_with(&format!("_{secret}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_common_secret_shapes() {
        let redactor = Redactor::default();
        let result =
            redactor.text("Authorization: Bearer abc.def token=shh postgres://user:pass@db/app");
        assert!(!result.contains("abc.def"));
        assert!(!result.contains("shh"));
        assert!(!result.contains(":pass@"));
    }

    #[test]
    fn redacts_nested_secret_fields() {
        let redactor = Redactor::default();
        let mut value = json!({"context": {"api_key": "secret-value", "safe": "ok"}});
        redactor.value(&mut value);
        assert_eq!(value["context"]["api_key"], "[REDACTED]");
        assert_eq!(value["context"]["safe"], "ok");
    }
}
