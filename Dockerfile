FROM rust:1.88.0-alpine3.21 AS builder

RUN apk add --no-cache build-base
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM alpine:3.21

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 bridge \
    && adduser -S -D -H -u 10001 -G bridge bridge

COPY --from=builder /build/target/release/coolify-glitchtip-bridge /usr/local/bin/coolify-glitchtip-bridge

USER bridge
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/coolify-glitchtip-bridge"]

