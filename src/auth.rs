use axum::http::{HeaderMap, header};
use subtle::ConstantTimeEq;

pub fn is_authorized(headers: &HeaderMap, query_token: Option<&str>, expected: &str) -> bool {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_bearer);
    let custom = headers
        .get("x-bridge-token")
        .and_then(|value| value.to_str().ok());

    bearer
        .or(custom)
        .or(query_token)
        .is_some_and(|candidate| constant_time_eq(candidate, expected))
}

fn parse_bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.trim().split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

fn constant_time_eq(candidate: &str, expected: &str) -> bool {
    candidate.len() == expected.len() && bool::from(candidate.as_bytes().ct_eq(expected.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn accepts_bearer_custom_header_and_query_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer 0123456789abcdef"),
        );
        assert!(is_authorized(&headers, None, "0123456789abcdef"));

        assert!(is_authorized(
            &HeaderMap::new(),
            Some("0123456789abcdef"),
            "0123456789abcdef"
        ));
        assert!(!is_authorized(
            &HeaderMap::new(),
            Some("wrong"),
            "0123456789abcdef"
        ));
    }
}
