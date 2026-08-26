use std::{sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Request, State},
    http::{HeaderName, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tower_http::{
    decompression::RequestDecompressionLayer, limit::RequestBodyLimitLayer,
    sensitive_headers::SetSensitiveRequestHeadersLayer,
};

use crate::{
    aggregate::{BridgeHandle, SubmitError},
    auth::is_authorized,
    config::Config,
    ingest::parse_log_batch,
};

#[derive(Clone)]
struct AppState {
    bridge: BridgeHandle,
    auth_token: Arc<str>,
    max_records_per_request: usize,
    started_at: Instant,
}

pub fn app(config: Arc<Config>, bridge: BridgeHandle) -> Router {
    let state = AppState {
        bridge,
        auth_token: Arc::from(config.auth_token.as_str()),
        max_records_per_request: config.max_records_per_request,
        started_at: Instant::now(),
    };

    let protected = Router::new()
        .route("/v1/logs", post(logs))
        .route("/v1/webhooks/coolify", post(webhook))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));

    Router::new()
        .route("/healthz", get(health))
        .merge(protected)
        .layer(SetSensitiveRequestHeadersLayer::new([
            header::AUTHORIZATION,
            HeaderName::from_static("x-bridge-token"),
        ]))
        .layer(RequestBodyLimitLayer::new(config.max_request_bytes))
        .layer(RequestDecompressionLayer::new())
        .with_state(state)
}

async fn authorize(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let query_token = request.uri().query().and_then(|query| {
        query
            .split('&')
            .find_map(|part| part.strip_prefix("token="))
    });
    if !is_authorized(request.headers(), query_token, &state.auth_token) {
        return api_error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    next.run(request).await
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "stats": state.bridge.stats().snapshot(),
    }))
}

async fn logs(State(state): State<AppState>, body: Bytes) -> Response {
    let records = match parse_log_batch(&body, state.max_records_per_request) {
        Ok(records) => records,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let accepted = records.len();
    match state.bridge.submit_logs(records) {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({"accepted": accepted}))).into_response(),
        Err(error) => submission_error(error),
    }
}

async fn webhook(State(state): State<AppState>, body: Bytes) -> Response {
    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(object)) => Value::Object(object),
        Ok(_) => return api_error(StatusCode::BAD_REQUEST, "payload must be a JSON object"),
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {error}")),
    };
    match state.bridge.submit_webhook(payload) {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({"accepted": 1}))).into_response(),
        Err(error) => submission_error(error),
    }
}

fn submission_error(error: SubmitError) -> Response {
    match error {
        SubmitError::Full => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ingest queue is full; retry later",
        ),
        SubmitError::Closed => {
            api_error(StatusCode::SERVICE_UNAVAILABLE, "bridge is shutting down")
        }
    }
}

fn api_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::{aggregate::BridgeRuntime, event::BridgeEvent, sink::EventSink};

    #[derive(Default)]
    struct MemorySink(Mutex<Vec<BridgeEvent>>);

    impl EventSink for MemorySink {
        fn capture(&self, event: BridgeEvent) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn config() -> Arc<Config> {
        Arc::new(
            Config::from_map(HashMap::from([
                ("AUTH_TOKEN".into(), "0123456789abcdef".into()),
                (
                    "GLITCHTIP_DSN".into(),
                    "https://public@example.invalid/42".into(),
                ),
                ("MULTILINE_TIMEOUT_MS".into(), "10".into()),
            ]))
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn health_is_public_but_ingest_is_authenticated() {
        let config = config();
        let runtime = BridgeRuntime::spawn(&config, Arc::new(MemorySink::default()));
        let app = app(config, runtime.handle.clone());

        let health = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let unauthorized = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"log":"Error: boom"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn accepts_logs_with_a_bearer_token() {
        let config = config();
        let sink = Arc::new(MemorySink::default());
        let runtime = BridgeRuntime::spawn(&config, sink.clone());
        let app = app(config, runtime.handle.clone());

        let response = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"log":"Error: boom"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["accepted"],
            1
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        runtime.shutdown().await;
        assert_eq!(sink.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn accepts_webhook_query_token_for_coolify() {
        let config = config();
        let sink = Arc::new(MemorySink::default());
        let runtime = BridgeRuntime::spawn(&config, sink.clone());
        let app = app(config, runtime.handle.clone());

        let response = app
            .oneshot(
                Request::post("/v1/webhooks/coolify?token=0123456789abcdef")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"success":false,"event":"deployment_failed","message":"Deployment failed"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        runtime.shutdown().await;
        assert_eq!(sink.0.lock().unwrap().len(), 1);
    }
}
