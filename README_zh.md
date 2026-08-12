# kafka-proxy

一个纯 Rust 编写的透明 Kafka 代理。它让 Java/Rust/Go/Python 客户端无需修改客户端代码，即可访问任意 Kafka 集群——只需将 `bootstrap.servers` 指向代理即可。代理透明地处理所有上游认证机制（GSSAPI/Kerberos、SASL/PLAIN、SASL/SCRAM、mTLS、明文），客户端始终可以以 `security.protocol=PLAINTEXT` 连接。

- **透明无感**：客户端以 `security.protocol=PLAINTEXT`（明文）连接；代理负责向上游真实 broker 完成所需的认证（Kerberos、SASL、mTLS 等）。
- **纯 Rust 实现**：基于 Tokio 构建；内置的 `kafka_client` + `krb5-gss` 提供了纯 Rust 的 Kerberos 实现（无需系统 `libkrb5`），适用于需要 GSSAPI 的集群。
- **端口对应 Broker**：每个 bootstrap broker 对应一个监听端口；返回给客户端的元数据会被改写，确保客户端始终通过代理重连。
- **连接池复用**：支持多路复用上游连接（配合 correlation-id 重映射），或 1:1 流转发以获得最大稳定性。
- **低内存占用**：专为 512 MB / 1 GB 的低内存环境设计。
- **内置 Web API**：提供健康检查、Prometheus 指标和诊断（doctor）端点（查看消费者、读取最新消息、查看活跃连接）。

> 📖 [English Version (英文版本)](./README.md)

## 工作原理

```
  客户端 (Java/Rust/Go, PLAINTEXT)         真实 Kafka 集群 (任意认证方式)
       │                                              │
       │  security.protocol = PLAINTEXT              │  Kerberos / SCRAM / PLAINTEXT
       ▼                                              ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                        kafka-proxy                          │
  │  ┌──────────┐   ┌────────────┐   ┌───────────────────────┐  │
  │  │ listener │──▶│  rewriter  │──▶│ upstream auth + pool  │──▶ broker
  │  │ (每个     │   │ (元数据、    │   │ (GSSAPI/SASL/PLAIN、  │  │
  │  │  broker) │   │  协调器、   │   │  cid 重映射、连接池)   │  │
  │  │          │◀──│  端点地址)  │◀──│                       │◀── broker
  │  └──────────┘   └────────────┘   └───────────────────────┘  │
  │  ┌──────────────────────────────────────────────────────┐  │
  │  │ Web API (axum): /health /metrics /doctor/*           │  │
  │  └──────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────┘
```

启动时，代理使用配置的上游认证方式连接到真实集群，拉取元数据，并获取每个 bootstrap broker 的 `node_id`。然后，它为每个 bootstrap broker（按顺序）绑定一个下游监听端口，并构建改写映射表。当客户端连接时，代理透明地转发 Kafka 帧，同时改写 `MetadataResponse` / `FindCoordinatorResponse`，确保客户端始终通过代理重连，而不是直接访问真实 broker。

## CI 持续集成

[![Build (x86 + ARM)](https://github.com/sixinyiyu/kafka-proxy/actions/workflows/build.yml/badge.svg)](https://github.com/sixinyiyu/kafka-proxy/actions/workflows/build.yml)

GitHub Actions 流水线会自动构建 **x86_64** 和 **aarch64**（ARM64）可执行安装包。
推送 tag（`v*`）即触发构建并发布 Release；详见
[`.github/workflows/build.yml`](.github/workflows/build.yml)。由于代理及其全部依赖
（包括 `krb5-gss` / `rustls`）均为纯 Rust，无 C FFI，构建产物无需任何系统库。
代码提交时会自动触发 `cargo fmt` + `cargo clippy` + `cargo check` + `cargo test` 检查。

## 快速开始


### 1. 编译

```bash
cargo build --release
```

> 项目通过 `rust-toolchain.toml` 固定工具链版本，以保证可复现构建。

### 2. 配置

复制示例配置并编辑：

```bash
cp kafka-proxy.toml.example config.toml
```

一个适用于 Kerberos 集群的最小配置：

```toml
[cluster]
bootstrap_servers = ["broker1:9092", "broker2:9092", "broker3:9092"]

[proxy]
# 每个 bootstrap broker 对应一个下游地址（按顺序）。
# advertise_host:port → host 写入改写后的元数据；port 在 listen_bind 上绑定监听。
bootstrap_server_mapping = ["10.0.0.5:19092", "10.0.0.5:19093", "10.0.0.5:19094"]
listen_bind = "0.0.0.0"

[upstream.auth]
mechanism = "gssapi"
kerberos_principal = "user@EXAMPLE.COM"
kerberos_keytab    = "/home/dayu/dayukb.keytab"
kerberos_kdc       = "kdc.example.com:88"
kerberos_realm     = "HADOOP.COM"

[downstream.auth]
mechanism = "none"
```

### 3. 运行

```bash
./target/release/kafka-proxy -c config.toml
```

### 4. 让客户端连接到代理

只需修改 `bootstrap.servers` 并强制使用明文协议——其他配置保持不变：

```properties
# Java 客户端
bootstrap.servers=10.0.0.5:19092,10.0.0.5:19093,10.0.0.5:19094
security.protocol=PLAINTEXT
# 客户端无需配置任何 sasl.* / kerberos.* 参数
```

```python
# confluent-kafka-python
conf = {
    "bootstrap.servers": "10.0.0.5:19092,10.0.0.5:19093,10.0.0.5:19094",
    "security.protocol": "PLAINTEXT",
}
```

## 命令行用法

```
kafka-proxy -c <config-file>

选项：
  -c, --config <config>   配置文件路径 [默认: config.toml]
```

日志通过 `RUST_LOG` 环境变量控制（例如 `RUST_LOG=info,kafka_client=debug`）。

## 配置参考

配置文件格式为 TOML。顶层配置段：

| 配置段 | 描述 |
|---------|-------------|
| `[cluster]` | 真实集群 bootstrap broker（上游连接与元数据发现）。 |
| `[proxy]` | 下游监听器、对外广告地址、连接限制。 |
| `[upstream.auth]` | 访问真实 broker 的认证方式。 |
| `[downstream.auth]` | 客户端连接时代理期望的认证方式（通常为 `none`）。 |
| `[pool]` | 连接池与转发模式。 |
| `[api]` | Web API（health / metrics / doctor 端点）。 |
| `[ha]` | 高可用模式。 |

### `[cluster]`

| 键 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| `bootstrap_servers` | `Vec<String>` | *(必填)* | 真实 broker 地址，例如 `["b1:9092","b2:9092"]`。顺序与 `proxy.bootstrap_server_mapping` 一一对应。只需填写部分 broker 即可，代理会通过元数据发现所有 broker。 |

### `[proxy]`

| 键 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| `advertise_host` | `Option<String>` | *(无)* | 当 `bootstrap_server_mapping` 某项省略主机名时，用作回退的主机名写入改写后的元数据。 |
| `listen_bind` | `String` | `0.0.0.0` | 下游监听器的绑定地址。 |
| `max_downstream_connections` | `usize` | `10000` | 并发客户端连接的全局上限（内存保护；达到上限后拒绝新连接）。 |
| `client_idle_timeout` | `Option<Duration>` | *(无)* | 客户端连接的空闲超时——如果在此时间内未收到任何帧，代理将主动关闭连接并记录警告日志。接受字符串格式如 `"5m"`、`"300s"`，或整数（秒）。不设置则无超时。 |
| `bootstrap_server_mapping` | `Vec<String>` | *(必填)* | 下游监听地址，与 `bootstrap_servers` 一一对应（顺序相同）。每项格式为 `advertise_host:port`：host 作为对外广告地址写入改写后的元数据，port 在 `listen_bind` 上绑定。数组长度必须等于 `bootstrap_servers`。可以省略 host（如 `:19092`）以回退到 `advertise_host`。 |

**映射示例**（端口 N ↔ broker N，按 bootstrap 顺序）：

```toml
bootstrap_servers        = ["b1:9092", "b2:9092", "b3:9092"]
bootstrap_server_mapping = ["proxy:19092", "proxy:19093", "proxy:19094"]
# 端口 19092 → broker1, 19093 → broker2, 19094 → broker3
```

无需知道真实 broker 的 `node_id`——代理在启动时通过元数据自动反查。

### `[upstream.auth]` / `[downstream.auth]`

`mechanism` 字段为枚举类型（支持 snake_case / kebab-case）——拼写错误会在解析阶段就被捕获，而非运行时才报错。

| `mechanism` | 必填字段 | 说明 |
|-------------|-----------------|-------|
| `none` *(默认)* | — | 明文传输。 |
| `plain` | `username`、`password` | SASL/PLAIN 认证。 |
| `scram-sha256` | `username`、`password` | SASL/SCRAM-SHA-256 认证。 |
| `scram-sha512` | `username`、`password` | SASL/SCRAM-SHA-512 认证。 |
| `gssapi` | `kerberos_principal`、`kerberos_keytab`、`kerberos_kdc`、（`kerberos_realm` 可选） | 通过 GSSAPI 实现 Kerberos 认证。纯 Rust 实现，无需 `libkrb5`。 |
| `mtls` | — | mTLS（仅下游支持；上游尚未实现）。 |

附加字段：

| 键 | 适用于 | 描述 |
|-----|-----------|-------------|
| `kerberos_principal` | gssapi | 客户端 principal，例如 `user@EXAMPLE.COM`。 |
| `kerberos_keytab` | gssapi | keytab 文件路径。 |
| `kerberos_kdc` | gssapi | KDC 地址，格式为 `host:port`（例如 `kdc:88`）。 |
| `kerberos_realm` | gssapi | 可选 realm 覆盖。 |
| `ca_file` | tls/mtls | CA 证书文件路径。 |
| `server_name` | tls/mtls | TLS SNI / 服务器名称。 |
| `verify` | tls/mtls | 是否验证对端证书（默认 `true`）。 |

### `[pool]`

| 键 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| `mode` | `pooled` \| `per_connection` | `pooled` | 转发模式（见下方说明）。 |
| `max_per_broker` | `usize` | `16` | 每个 broker 的上游连接上限。超出的请求将排队（背压）。 |
| `min_idle` | `usize` | `2` | 保持的最小空闲连接数（避免冷启动握手开销）。 |
| `idle_timeout` | `Duration` | `5m` | 空闲连接回收超时。 |
| `acquire_timeout` | `Duration` | `5s` | 请求等待获取池化连接的最长时间，超时则失败。 |
| `health_check` | `Duration` | `30s` | 上游连接存活探测间隔。 |
| `max_in_flight` | `usize` | `100000` | correlation_id 重映射表容量上限（背压阈值）。 |
| `max_rss_bytes` | `u64` | `0` | RSS 熔断阈值（字节）。当 RSS 达到此值时拒绝新连接以防止 OOM。`0` = 不限制。 |

**转发模式：**

- `per_connection` — 每条客户端连接独占一条上游认证连接（1:1）。最简单可靠；无需 correlation-id 重映射。
- `pooled` — 多条客户端连接通过少量上游连接池进行多路复用，配合 correlation-id 重映射。减少握手开销（GSSAPI/TLS），但路由逻辑更复杂。

时长字段接受字符串格式如 `"5s"`、`"10m"`、`"1h"`、`"500ms"`，或纯整数（秒）。

### `[api]`

一个统一的 HTTP 服务器承载所有端点（health / metrics / doctor），默认启动。

| 键 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| `listen` | `String` | `127.0.0.1:9100` | Web API 监听地址。 |
| `default_count` | `usize` | `5` | `GET /doctor/messages/{topic}` 默认返回的记录条数。 |
| `metrics_enabled` | `bool` | `false` | 是否启用性能指标采集。关闭时 `/metrics` 端点仍然可访问，但所有计数器均为 0，且转发热路径会跳过原子操作以降低开销。仅当需要可观测性时开启。 |

### `[ha]`

| 键 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| `mode` | `stateless_replicas` \| `dns_round_robin` | `stateless_replicas` | 高可用策略。代理本身是无状态的；依靠 k8s Service 负载均衡或 DNS 轮询实现多副本部署。无需引入外部高可用组件（如 keepalived）或共识算法（如 Raft）。 |

## Web API 端点

所有端点均返回 JSON（`/metrics` 返回 Prometheus 文本格式）。暂不提供前端页面。

| 方法 | 路径 | 描述 |
|--------|------|-------------|
| `GET` | `/health` | 存活/就绪检查 — 返回 `{"status":"ok"}`。 |
| `GET` | `/metrics` | Prometheus 文本格式指标（详见下文）。 |
| `GET` | `/doctor/topics` | 列出所有 topic（名称 / 分区数 / 是否内部 topic）。 |
| `POST` | `/doctor/topics` | 创建 topic。请求体：`{"name","num_partitions?","replication_factor?","configs?":[["k","v"]]}`。 |
| `GET` | `/doctor/consumers/{topic}` | 查询某 topic 的消费者组（成员 / 分区 / 消费滞后）。 |
| `GET` | `/doctor/messages/{topic}?count=5` | 查看某 topic 的最新消息（默认条数来自配置，最大 1000）。 |
| `POST` | `/doctor/messages/{topic}` | 发送消息。请求体：`{"value","key?","partition?"}`。 |
| `GET` | `/doctor/connections` | 当前已连接的客户端（对端地址 / node_id / 持续时间）。 |

### Prometheus 指标

可通过 `GET /metrics` 获取。当 `metrics_enabled = false`（默认）时，所有性能计数器均为 0 以避免开销；连接管理类 gauge（`kafka_proxy_downstream_connections`）始终保持活跃，因为它们用于支撑 `max_downstream_connections` 安全限制。

| 指标 | 类型 | 描述 |
|--------|------|-------------|
| `kafka_proxy_frames_total{dir}` | counter | 转发的帧数（dir=downstream\|upstream）。 |
| `kafka_proxy_bytes_total{dir}` | counter | 转发的字节数。 |
| `kafka_proxy_requests_in_flight` | gauge | 正在等待上游响应的在途请求数。 |
| `kafka_proxy_downstream_connections` | gauge | 活跃的客户端连接数。 |
| `kafka_proxy_upstream_connections` | gauge | 活跃的上游池化连接数。 |
| `kafka_proxy_pool_hits_total` | counter | 连接池命中次数（复用空闲连接）。 |
| `kafka_proxy_pool_misses_total` | counter | 连接池未命中次数（新建连接）。 |
| `kafka_proxy_pool_evictions_total` | counter | 连接池驱逐次数（超时/错误）。 |
| `kafka_proxy_pool_waiters` | gauge | 等待获取池化连接的请求数。 |
| `kafka_proxy_pool_hit_rate` | gauge | 计算得出的命中率（hits/(hits+misses)）。 |
| `kafka_proxy_auth_failures_total` | counter | 上游认证失败次数。 |
| `kafka_proxy_acquire_duration_seconds` | histogram | 等待获取上游连接的耗时。 |
| `kafka_proxy_request_duration_seconds` | histogram | 端到端请求延迟。 |
| `kafka_proxy_handshake_duration_seconds` | histogram | 上游 GSSAPI/TLS 握手耗时。 |
| `kafka_proxy_process_resident_memory_bytes` | gauge | 进程 RSS 内存（来自 /proc）。 |
| `kafka_proxy_cid_map_entries` | gauge | correlation_id 重映射表条目数。 |

## 架构

```
src/
├── main.rs      — CLI 入口：加载配置、启动代理
├── lib.rs       — 编排调度：bootstrap → 绑定监听 → accept → 转发
├── config.rs    — TOML 配置解析与验证（枚举认证机制）
├── upstream.rs  — 上游认证（Plaintext/SASL/GSSAPI）→ 帧流
├── pool.rs      — 连接池 + correlation_id 重映射（pooled 模式）
├── relay.rs     — 帧转发（per_connection 1:1 + pooled 多路复用）
├── rewrite.rs   — 元数据 / FindCoordinator 响应改写
├── metrics.rs   — 原子计数器 + Prometheus 导出（按启用开关控制）
└── api.rs       — 统一 Web API（axum）：health/metrics/doctor 路由
```

代理依赖 [`kafka_client`](https://crates.io/crates/kafka_client)（v0.8，来自 crates.io），它提供了 `build_framed()` / `send_raw_frame()` raw-frame API，用于透明 1:1 帧转发。早期开发版本曾 vendor 一份打了补丁的 `kafka_client` 0.5.2，以修复按 broker 区分 Kerberos 服务主体主机名的 bug（多 broker 集群下的 GSSAPI 错误 58）并暴露 raw-frame API；这两项现已合并上游（per-broker principal 在 v0.7.0 / commit `d536afb0`；raw-frame API 在 v0.8.0），因此不再需要 vendor 副本。

`krb5-gss` 依赖（v0.2.0）同样直接从 crates.io 获取，其上游作者已修复 `EncAPRepPart` ASN.1 标签问题。


## 支持的 Kerberos 加密类型

AES-128/256-CTS-HMAC-SHA1-96（RFC 3962）和 AES-128/256-CTS-HMAC-SHA256/384（RFC 8009）。

## 测试

```bash
cargo test --lib
```

单元测试覆盖配置解析、元数据改写、cid 重映射背压、metrics 启用/禁用行为以及上游认证构建。需要真实集群的集成示例位于 `examples/` 目录下。

## 运行要求

- Rust（通过 `rust-toolchain.toml` 固定版本）
- Kafka 1.0.0+（GSSAPI 需要 `SaslHandshake` v1）

## 许可证

遵循与其依赖相同的许可条款。上游 `kafka_client`（Apache-2.0 OR MIT）与 `krb5-gss` 的许可证通过 crates.io 传递生效。
