# kafka-proxy

A transparent Kafka proxy written in pure Rust. It lets Java/Rust/Go/Python clients
access any Kafka cluster **without modifying the client code** — just point
`bootstrap.servers` at the proxy. It handles all upstream authentication
mechanisms (GSSAPI/Kerberos, SASL/PLAIN, SASL/SCRAM, mTLS, plaintext) transparently,
so clients can always connect with `security.protocol=PLAINTEXT`.

- **Transparent**: clients connect with `security.protocol=PLAINTEXT`; the proxy
  performs the required authentication (Kerberos, SASL, mTLS, etc.) to the real
  brokers upstream.
- **Pure Rust**: built on Tokio; `kafka_client` + `krb5-gss` provide a
  pure-Rust Kerberos implementation (no system `libkrb5` needed) for clusters that
  require GSSAPI.
- **Port-per-broker**: one listening port per bootstrap broker; metadata advertised
  to clients is rewritten so they reconnect through the proxy.
- **Connection pooling**: multi-plexed upstream connections with correlation-id
  remapping, or 1:1 stream forwarding for maximum stability.
- **Low memory**: designed to run in 512 MB / 1 GB environments.
- **Built-in web API**: health check, Prometheus metrics, and doctor
  endpoints (inspect consumers, read latest messages, view active connections).

> 📖 [中文版本 (Chinese Version)](./README_zh.md)

## How it works

```
  Client (Java/Rust/Go, PLAINTEXT)          Real Kafka cluster (any auth)
       │                                              │
       │  security.protocol = PLAINTEXT              │  Kerberos / SCRAM / PLAINTEXT
       ▼                                              ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                        kafka-proxy                          │
  │  ┌──────────┐   ┌────────────┐   ┌───────────────────────┐  │
  │  │ listener │──▶│  rewriter  │──▶│ upstream auth + pool  │──▶ broker
  │  │ (per     │   │ (metadata, │   │ (GSSAPI/SASL/PLAIN,   │  │
  │  │  broker) │   │  coord,    │   │  cid remap, pool)     │  │
  │  │          │◀──│  endpoints)│◀──│                       │◀── broker
  │  └──────────┘   └────────────┘   └───────────────────────┘  │
  │  ┌──────────────────────────────────────────────────────┐  │
  │  │ Web API (axum): /health /metrics /doctor/*           │  │
  │  └──────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────┘
```

On startup the proxy connects to the real cluster with the configured upstream
authentication, fetches metadata, and learns each bootstrap broker's `node_id`.
It then binds one downstream listening port per bootstrap broker (in order) and
builds a rewrite map. When a client connects, the proxy transparently forwards
Kafka frames, rewriting `MetadataResponse` / `FindCoordinatorResponse` so the
client always reconnects through the proxy instead of reaching the real brokers
directly.

## CI

[![Build (x86 + ARM)](https://github.com/sixinyiyu/kafka-proxy/actions/workflows/build.yml/badge.svg)](https://github.com/sixinyiyu/kafka-proxy/actions/workflows/build.yml)

Pre-built binaries for **x86_64** and **aarch64** (ARM64) are produced by the
GitHub Actions pipeline. Push a tag (`v*`) to publish a release; see
[`.github/workflows/build.yml`](.github/workflows/build.yml). Since the proxy and
all its dependencies (including `krb5-gss` / `rustls`) are pure Rust with no C
FFI, the binaries are fully static-friendly and need no system libraries.


## Quick start

### 1. Build

```bash
cargo build --release
```

> The project pins its toolchain via `rust-toolchain.toml` for reproducibility.
> You can also download a pre-built binary from the [releases page](https://github.com/sixinyiyu/kafka-proxy/releases)

> (choose `kafka-proxy-<version>-x86_64.tar.gz` or `-aarch64.tar.gz`).


### 2. Configure

Copy the example and edit it:

```bash
cp kafka-proxy.toml.example config.toml
```

A minimal config for a Kerberos cluster:

```toml
[cluster]
bootstrap_servers = ["broker1:9092", "broker2:9092", "broker3:9092"]

[proxy]
# One downstream address per bootstrap broker (in order).
# advertise_host:port  →  host written into rewritten metadata; port bound on listen_bind.
bootstrap_server_mapping = ["10.0.0.5:19092", "10.0.0.5:19093", "10.0.0.5:19094"]
listen_bind = "0.0.0.0"

[upstream.auth]
mechanism = "gssapi"
kerberos_principal = "dayu@HADOOP.COM"
kerberos_keytab    = "/home/dayu/dayukb.keytab"
kerberos_kdc       = "kdc.example.com:88"
kerberos_realm     = "HADOOP.COM"

[downstream.auth]
mechanism = "none"
```

### 3. Run

```bash
./target/release/kafka-proxy -c config.toml
```

### 4. Point your client at the proxy

Only change `bootstrap.servers` and force plaintext — everything else stays the same:

```properties
# Java client
bootstrap.servers=10.0.0.5:19092,10.0.0.5:19093,10.0.0.5:19094
security.protocol=PLAINTEXT
# No sasl.* / kerberos.* settings needed on the client side
```

```python
# confluent-kafka-python
conf = {
    "bootstrap.servers": "10.0.0.5:19092,10.0.0.5:19093,10.0.0.5:19094",
    "security.protocol": "PLAINTEXT",
}
```

## Command-line usage

```
kafka-proxy -c <config-file>

Options:
  -c, --config <config>   Configuration file path [default: config.toml]
```

Logging is controlled by the `RUST_LOG` environment variable (e.g.
`RUST_LOG=info,kafka_client=debug`).

## Configuration reference

The config file is TOML. Top-level sections:

| Section | Description |
|---------|-------------|
| `[cluster]` | Real cluster bootstrap brokers (upstream connection + metadata discovery). |
| `[proxy]` | Downstream listeners, advertise addresses, connection limits. |
| `[upstream.auth]` | Authentication to real brokers. |
| `[downstream.auth]` | Authentication expected from clients (usually `none`). |
| `[pool]` | Connection pool & forwarding mode. |
| `[api]` | Web API (health / metrics / doctor endpoints). |
| `[ha]` | High-availability mode. |

### `[cluster]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bootstrap_servers` | `Vec<String>` | *(required)* | Real broker addresses, e.g. `["b1:9092","b2:9092"]`. Order maps 1:1 to `proxy.bootstrap_server_mapping`. Only a subset is needed; the proxy discovers all brokers via metadata. |

### `[proxy]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `advertise_host` | `Option<String>` | *(none)* | Fallback hostname written into rewritten metadata when a `bootstrap_server_mapping` entry omits its host. |
| `listen_bind` | `String` | `0.0.0.0` | Bind address for downstream listeners. |
| `max_downstream_connections` | `usize` | `10000` | Global cap on concurrent client connections (memory protection; rejects new connections when reached). |
| `client_idle_timeout` | `Option<Duration>` | *(none)* | Idle timeout for client connections — if no frame is received within this duration the proxy actively closes the connection and logs a warning. Accepts strings like `"5m"`, `"300s"`, or an integer (seconds). Omit for no timeout. |
| `bootstrap_server_mapping` | `Vec<String>` | *(required)* | Downstream listen addresses, one per `bootstrap_servers` entry (same order). Each item is `advertise_host:port`: the host is advertised to clients in rewritten metadata, the port is bound on `listen_bind`. Length must equal `bootstrap_servers`. You may omit the host (`:19092`) to fall back to `advertise_host`. |

**Mapping example** (port N ↔ broker N, in bootstrap order):

```toml
bootstrap_servers        = ["b1:9092", "b2:9092", "b3:9092"]
bootstrap_server_mapping = ["proxy:19092", "proxy:19093", "proxy:19094"]
# port 19092 → broker1, 19093 → broker2, 19094 → broker3
```

No need to know the real brokers' `node_id`s — the proxy reverse-looks them up
from metadata at startup.

### `[upstream.auth]` / `[downstream.auth]`

The `mechanism` field is an enum (snake_case / kebab-case) — typos fail at parse
time rather than at runtime.

| `mechanism` | Required fields | Notes |
|-------------|-----------------|-------|
| `none` *(default)* | — | Plaintext. |
| `plain` | `username`, `password` | SASL/PLAIN. |
| `scram-sha256` | `username`, `password` | SASL/SCRAM-SHA-256. |
| `scram-sha512` | `username`, `password` | SASL/SCRAM-SHA-512. |
| `gssapi` | `kerberos_principal`, `kerberos_keytab`, `kerberos_kdc`, (`kerberos_realm`?) | Kerberos via GSSAPI. Pure-Rust; no `libkrb5`. |
| `mtls` | — | mTLS (downstream only; upstream not yet implemented). |

Additional fields:

| Key | Applies to | Description |
|-----|-----------|-------------|
| `kerberos_principal` | gssapi | Client principal, e.g. `dayu@HADOOP.COM`. |
| `kerberos_keytab` | gssapi | Path to the keytab file. |
| `kerberos_kdc` | gssapi | KDC address as `host:port` (e.g. `kdc:88`). |
| `kerberos_realm` | gssapi | Optional realm override. |
| `ca_file` | tls/mtls | CA certificate file path. |
| `server_name` | tls/mtls | TLS SNI / server name. |
| `verify` | tls/mtls | Whether to verify the peer cert (default `true`). |

### `[pool]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | `pooled` \| `per_connection` | `pooled` | Forwarding mode (see below). |
| `max_per_broker` | `usize` | `16` | Max upstream connections per broker. Excess requests queue (backpressure). |
| `min_idle` | `usize` | `2` | Minimum idle connections kept warm (avoids cold-start handshake). |
| `idle_timeout` | `Duration` | `5m` | Idle connection reclamation. |
| `acquire_timeout` | `Duration` | `5s` | How long a request waits for a pooled connection before failing. |
| `health_check` | `Duration` | `30s` | Upstream connection liveness probe interval. |
| `max_in_flight` | `usize` | `100000` | correlation_id remap table cap (backpressure threshold). |
| `max_rss_bytes` | `u64` | `0` | RSS circuit breaker (bytes). When RSS reaches this, new connections are rejected to prevent OOM. `0` = unlimited. |

**Forwarding modes:**

- `per_connection` — each client connection gets a dedicated upstream authenticated
  connection (1:1). Simplest and most robust; no correlation-id remapping needed.
- `pooled` — multiple client connections multiplex over a small pool of upstream
  connections, with correlation-id remapping. Reduces handshake overhead (GSSAPI/TLS)
  at the cost of more complex routing.

Durations accept strings like `"5s"`, `"10m"`, `"1h"`, `"500ms"`, or a bare integer
(seconds).

### `[api]`

A single HTTP server hosts all endpoints (health / metrics / doctor) on one port,
started by default.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `listen` | `String` | `127.0.0.1:9100` | Web API listen address. |
| `default_count` | `usize` | `5` | Default record count for `GET /doctor/messages/{topic}`. |
| `metrics_enabled` | `bool` | `false` | Whether to collect performance metrics. When `false`, the `/metrics` endpoint still works but all counters read 0 and the forwarding hot path skips atomic ops to reduce overhead. Enable only when you need observability. |

### `[ha]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | `stateless_replicas` \| `dns_round_robin` | `stateless_replicas` | HA strategy. The proxy itself is stateless; rely on k8s Service load balancing or DNS round-robin for multiple replicas. No external HA component (keepalived) or consensus algorithm (Raft) required. |

## Web API endpoints

All endpoints return JSON (or Prometheus text for `/metrics`). No frontend page.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness/readiness check — returns `{"status":"ok"}`. |
| `GET` | `/metrics` | Prometheus text-format metrics (see below). |
| `GET` | `/doctor/topics` | List all topics (name / partitions / internal). |
| `POST` | `/doctor/topics` | Create a topic. Body: `{"name","num_partitions?","replication_factor?","configs?":[["k","v"]]}`. |
| `GET` | `/doctor/consumers/{topic}` | Consumer groups on a topic (members / partitions / lag). |
| `GET` | `/doctor/messages/{topic}?count=5` | Latest messages on a topic (default count from config, max 1000). |
| `POST` | `/doctor/messages/{topic}` | Produce a message. Body: `{"value","key?","partition?"}`. |
| `GET` | `/doctor/connections` | Currently connected clients (peer / node_id / duration). |

### Prometheus metrics

Available at `GET /metrics`. When `metrics_enabled = false` (default) all
performance counters read 0 to avoid overhead; connection-management gauges
(`kafka_proxy_downstream_connections`) are always active because they back the
`max_downstream_connections` safety limit.

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_proxy_frames_total{dir}` | counter | Frames forwarded (dir=downstream\|upstream). |
| `kafka_proxy_bytes_total{dir}` | counter | Bytes forwarded. |
| `kafka_proxy_requests_in_flight` | gauge | In-flight requests awaiting upstream response. |
| `kafka_proxy_downstream_connections` | gauge | Active client connections. |
| `kafka_proxy_upstream_connections` | gauge | Active upstream pooled connections. |
| `kafka_proxy_pool_hits_total` | counter | Pool acquire hits (reused idle connection). |
| `kafka_proxy_pool_misses_total` | counter | Pool acquire misses (new connection created). |
| `kafka_proxy_pool_evictions_total` | counter | Pool connection evictions (timeout/error). |
| `kafka_proxy_pool_waiters` | gauge | Requests waiting for a pooled connection. |
| `kafka_proxy_pool_hit_rate` | gauge | Derived hit rate (hits/(hits+misses)). |
| `kafka_proxy_auth_failures_total` | counter | Upstream authentication failures. |
| `kafka_proxy_acquire_duration_seconds` | histogram | Time waiting to acquire an upstream connection. |
| `kafka_proxy_request_duration_seconds` | histogram | End-to-end request latency. |
| `kafka_proxy_handshake_duration_seconds` | histogram | Upstream GSSAPI/TLS handshake duration. |
| `kafka_proxy_process_resident_memory_bytes` | gauge | Process RSS (from /proc). |
| `kafka_proxy_cid_map_entries` | gauge | correlation_id remap table entries. |

## Architecture

```
src/
├── main.rs      — CLI entry: load config, start proxy
├── lib.rs       — orchestration: bootstrap → bind → accept → forward
├── config.rs    — TOML config parsing + validation (enum mechanisms)
├── upstream.rs  — upstream auth (Plaintext/SASL/GSSAPI) → framed stream
├── pool.rs      — connection pool + correlation_id remap (pooled mode)
├── relay.rs     — frame forwarding (per_connection 1:1 + pooled multiplex)
├── rewrite.rs   — metadata / FindCoordinator response rewriting
├── metrics.rs   — atomic counters + Prometheus export (enable-gated)
└── api.rs       — unified web API (axum): health/metrics/doctor routes
```

The proxy depends on [`kafka_client`](https://crates.io/crates/kafka_client)
(v0.8, from crates.io), which provides the `build_framed()` /
`send_raw_frame()` raw-frame API used for transparent 1:1 frame forwarding.
Earlier in-development versions vendored a patched `kafka_client` 0.5.2 to fix
a per-broker Kerberos service-principal hostname bug (GSSAPI error 58 on
multi-broker clusters) and to expose the raw-frame API; both are now merged
upstream (per-broker principal in v0.7.0 / commit `d536afb0`; raw-frame API in
v0.8.0), so the vendor copy is no longer needed.

The `krb5-gss` dependency (v0.2.0) is also obtained directly from crates.io;
its upstream author has already fixed the `EncAPRepPart` ASN.1 tag bug.

## Supported Kerberos encryption types

AES-128/256-CTS-HMAC-SHA1-96 (RFC 3962) and AES-128/256-CTS-HMAC-SHA256/384
(RFC 8009).


## Testing

```bash
cargo test --lib
```

Unit tests cover config parsing, metadata rewriting, cid remap backpressure,
metrics enable/disable behavior, and upstream auth construction. Integration
examples that require a live cluster live under `examples/`.

## Requirements

- Rust (pinned via `rust-toolchain.toml`)
- Kafka 1.0.0+ (GSSAPI requires `SaslHandshake` v1)

## License

Licensed under the same terms as its dependencies. The upstream `kafka_client`
(Apache-2.0 OR MIT) and `krb5-gss` licenses apply transitively via crates.io.
