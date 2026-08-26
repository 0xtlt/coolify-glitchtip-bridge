# coolify-glitchtip-bridge

[![CI](https://github.com/0xtlt/coolify-glitchtip-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/0xtlt/coolify-glitchtip-bridge/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A small Rust service that forwards two kinds of Coolify signals to a
self-hosted GlitchTip project:

- application/container errors received from a Coolify custom Fluent Bit Log Drain;
- deployment, backup, container, scheduled-task, and server failures received from Coolify webhooks.

It uses GlitchTip's Sentry-compatible DSN and the official Rust Sentry SDK.

```text
Coolify application logs ── Fluent Bit ──► /v1/logs ───────────┐
                                                               ├──► GlitchTip
Coolify operational events ── webhook ──► /v1/webhooks/coolify ┘
```

This bridge is a catch-all. For applications you control, installing a Sentry
SDK inside the application still gives better stack frames, request context,
breadcrumbs, and release information.

## Features

- accepts Fluent Bit JSON arrays, JSON objects, `{"records": [...]}`, and NDJSON;
- rebuilds multiline stack traces before sending one GlitchTip event;
- uses explicit log levels first, then conservative error/fatal heuristics;
- filters successful webhooks by default while retaining restarts and warnings;
- adds Coolify app, project, environment, server, and container tags when present;
- generates stable fingerprints with variable numbers normalized;
- redacts common Authorization, token, password, DSN, and credential URL shapes;
- accepts gzip-compressed Log Drain requests;
- uses a bounded queue, request/event limits, graceful shutdown, and a public health check;
- ships as a non-root multi-stage Docker image.

## Deploy on Coolify

### 1. Create the bridge resource

In GlitchTip, create a project for Coolify and copy its DSN. Then generate a
separate bridge authentication token:

```bash
openssl rand -hex 32
```

In Coolify, create a **Public Repository** resource from:

```text
https://github.com/0xtlt/coolify-glitchtip-bridge
```

Use the repository `Dockerfile`, expose port `8080`, and set:

```env
GLITCHTIP_DSN=https://public-key@glitchtip.example.com/1
AUTH_TOKEN=the-64-character-value-generated-above
ENVIRONMENT=production
```

Assign an HTTPS domain such as `coolify-errors.example.com`. The health check
is `GET /healthz`.

**Do not enable Drain Logs on the bridge resource itself.** Forwarding the
bridge's own transport errors can create a feedback loop. The sample Fluent Bit
configuration also excludes records whose `COOLIFY_APP_NAME` is
`coolify-glitchtip-bridge` as a second guard.

### 2. Configure the application Log Drain

Coolify supports a custom Fluent Bit configuration under **Server → Log
Drains**. Paste [`coolify/fluent-bit.conf`](coolify/fluent-bit.conf) into the
custom configuration field and [`coolify/parsers.conf`](coolify/parsers.conf)
into the parser field.

Replace:

- `REPLACE_WITH_BRIDGE_HOSTNAME` with the hostname only, without `https://`;
- `REPLACE_WITH_AUTH_TOKEN` with the same `AUTH_TOKEN` used by the bridge;
- `REPLACE_WITH_COOLIFY_SERVER_NAME` with a stable server name.

Enable the custom Log Drain on the server. On every resource to monitor, enable
**Drain Logs** in its Advanced settings and restart the resource so Docker
applies the logging configuration.

For stable app/project/environment tags, add these environment variables to
each monitored resource when useful:

```env
COOLIFY_APP_NAME=api
COOLIFY_PROJECT_NAME=my-project
COOLIFY_ENVIRONMENT_NAME=production
```

Coolify's current Log Drain metadata can vary between application and Compose
resources. The bridge still accepts container metadata when those variables are
not available.

### 3. Configure Coolify operational webhooks

Under **Notifications → Webhook**, set:

```text
https://coolify-errors.example.com/v1/webhooks/coolify?token=YOUR_AUTH_TOKEN
```

Coolify's webhook UI accepts a URL but does not provide a custom request-header
field, so this endpoint accepts the token as a query parameter. The bridge never
logs query strings. Prefer a randomly generated hexadecimal token so the URL
does not require percent-encoding.

Enable the events you want. Failed events are forwarded automatically.
`container_restarted`, warning, unreachable, and outdated events are also kept
even when their `success` field is true. Set `WEBHOOK_INCLUDE_SUCCESS=true` to
forward all success notifications as informational events.

### 4. Test the complete path

Test log ingestion directly:

```bash
curl --fail-with-body \
  --header 'Authorization: Bearer YOUR_AUTH_TOKEN' \
  --header 'Content-Type: application/json' \
  --data '{"log":"Error: bridge test","coolify.app_name":"test-app"}' \
  https://coolify-errors.example.com/v1/logs
```

The response is `202 Accepted`. The event appears after the default 1.5-second
multiline window. Then use Coolify's **Send Test Notification** button. Success
test notifications only appear when `WEBHOOK_INCLUDE_SUCCESS=true`; a real or
synthetic failed event is the better final verification.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `GLITCHTIP_DSN` | required | Sentry-compatible project DSN from GlitchTip. |
| `AUTH_TOKEN` | required | Shared ingest token, minimum 16 characters. |
| `BIND_ADDR` | `0.0.0.0:8080` | HTTP listen address. |
| `ENVIRONMENT` | `production` | Environment attached to GlitchTip events. |
| `RELEASE` | unset | Optional bridge release value. |
| `MIN_LEVEL` | `error` | `debug`, `info`, `warning`, `error`, or `fatal`. |
| `STDERR_AS_ERROR` | `false` | Treat every stderr record as an error. Usually too noisy. |
| `WEBHOOK_INCLUDE_SUCCESS` | `false` | Forward routine successful Coolify webhooks. |
| `MULTILINE_TIMEOUT_MS` | `1500` | Idle window before a buffered stack trace is sent. |
| `MAX_EVENT_BYTES` | `65536` | Maximum assembled log size. |
| `MAX_EVENT_LINES` | `128` | Maximum lines in one assembled event. |
| `MAX_REQUEST_BYTES` | `1048576` | Maximum decompressed HTTP request size. |
| `MAX_RECORDS_PER_REQUEST` | `1000` | Maximum records in one batch. |
| `QUEUE_CAPACITY` | `2048` | Bounded ingest command queue size. |
| `IGNORE_REGEX` | unset | Optional Rust regex for logs that must never be forwarded. |
| `SHUTDOWN_TIMEOUT_SECONDS` | `5` | Sentry transport drain timeout. |
| `RUST_LOG` | bridge info | Standard Rust tracing filter. |

## HTTP API

All ingest endpoints require one of:

```text
Authorization: Bearer <AUTH_TOKEN>
X-Bridge-Token: <AUTH_TOKEN>
?token=<AUTH_TOKEN>
```

Use the query form only for Coolify webhooks. Use the Authorization header for
Fluent Bit and manual requests.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Public health, version, uptime, and non-sensitive counters. |
| `POST` | `/v1/logs` | Fluent Bit application/container log batches. |
| `POST` | `/v1/webhooks/coolify` | Coolify notification webhook payloads. |

The ingest endpoints return `202` after the bounded in-memory queue accepts the
payload. A `503` tells Fluent Bit to retry. The supplied config uses unlimited
Fluent Bit retries, so size the service and queue for the expected log volume.

## Local development

```bash
cp .env.example .env
# Edit .env, then:
set -a && source .env && set +a
cargo run
```

Or run it with Docker Compose:

```bash
docker compose up --build
```

Run the complete local checks:

```bash
cargo fmt --all -- --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
docker build --tag coolify-glitchtip-bridge:test .
```

## Operational notes

- Buffers are in memory and intentionally short-lived. Fluent Bit owns durable
  retries before acceptance; the Sentry SDK owns transport retries afterward.
- A log drain only sees what the process printed. It cannot reconstruct the
  full application exception context that an in-process SDK can capture.
- Redaction is defense in depth. Avoid writing credentials and personal data to
  application logs in the first place.
- Coolify requires a resource restart after enabling its Log Drain.

The integration follows Coolify's official [Drain Logs](https://coolify.io/docs/knowledge-base/drain-logs),
[Notifications](https://coolify.io/docs/knowledge-base/notifications/), and
[Webhook Payloads](https://coolify.io/docs/knowledge-base/webhook-payloads)
documentation.

## License

[MIT](LICENSE)
