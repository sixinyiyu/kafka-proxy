//! 上游(到真实 broker)与下游(到客户端)认证独立配置，可 none/plain/scram/gssapi/tls。
//! 连接池(§3.6)由 `[pool]` 段配置，Web API(health/metrics/debug)由 `[api]` 段配置。

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// 顶层配置。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    pub cluster: ClusterConfig,
    pub proxy: ProxySection,
    #[serde(default)]
    pub upstream: UpstreamSection,
    #[serde(default)]
    pub downstream: DownstreamSection,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub ha: HaConfig,
}

/// 真实集群 bootstrap（用于上游连接与元数据发现）。
#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    pub bootstrap_servers: Vec<String>,
}

/// 下游监听与对外地址。
#[derive(Debug, Deserialize)]
pub struct ProxySection {
    /// 写进改写后元数据的对外主机名(k8s Service DNS / VIP / 域名)。
    ///
    /// 作为 `bootstrap_server_mapping` 每项主机名缺省时的回退；当 mapping
    /// 每项都带主机名时，以 mapping 里的主机名为准(每 broker 可不同)。
    #[serde(default)]
    pub advertise_host: Option<String>,
    /// 下游监听绑定地址，默认 0.0.0.0。
    #[serde(default = "default_bind")]
    pub listen_bind: String,
    /// 下游连接上限(内存保护)，默认 10000。
    #[serde(default = "default_max_downstream")]
    pub max_downstream_connections: usize,
    /// 客户端空闲超时：下游连接在此时间内未发送任何帧则主动关闭。
    /// 不配置则无限等待(兼容旧行为)。如 `"5m"` / `"300s"`。
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub client_idle_timeout: Option<Duration>,
    /// 按 `[cluster].bootstrap_servers` 顺序一一对应的下游监听地址数组，
    /// 每项形如 `advertise_host:port`：主机名用于改写元数据(告诉客户端连哪)，
    /// 端口用于在 `listen_bind` 上绑定监听。
    ///
    /// 例：bootstrap_servers 有 3 个 broker，则配 3 项：
    ///   bootstrap_server_mapping = ["proxy.svc:19092", "proxy.svc:19093", "proxy.svc:19094"]
    /// 端口1(19092) 对应 broker1，端口2(19093) 对应 broker2，依此类推。
    /// 用户无需知道原 broker 的 node_id —— proxy 启动时自动反查。
    /// (见 .clinerules/Configuration：按 bootstrap 顺序自动对应)
    #[serde(default)]
    pub bootstrap_server_mapping: Vec<String>,
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}
fn default_max_downstream() -> usize {
    10000
}

/// 上游认证段。
#[derive(Debug, Default, Deserialize)]
pub struct UpstreamSection {
    #[serde(default)]
    pub auth: AuthConfig,
}

/// 下游认证段。
#[derive(Debug, Default, Deserialize)]
pub struct DownstreamSection {
    #[serde(default)]
    pub auth: AuthConfig,
}

/// 认证机制枚举：用枚举匹配，避免字符串拼写错误(见 .clinerules Code Style)。
///
/// TOML 用 snake_case / 连字符：`none`/`plain`/`scram-sha256`/`scram-sha512`/
/// `gssapi`/`mtls`。缺省(空)视为 `none`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMechanism {
    /// 无认证(明文)。
    #[default]
    #[serde(alias = "")]
    None,
    /// SASL/PLAIN 用户名密码。
    Plain,
    /// SASL/SCRAM-SHA-256。
    ScramSha256,
    /// SASL/SCRAM-SHA-512。
    ScramSha512,
    /// SASL/GSSAPI(Kerberos)。
    Gssapi,
    /// 双向 TLS(下游可选，上游暂未实现)。
    Mtls,
}

impl AuthMechanism {
    /// 是否需要 Kerberos 凭证。
    pub fn is_gssapi(self) -> bool {
        matches!(self, Self::Gssapi)
    }
}

impl std::fmt::Display for AuthMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Plain => "plain",
            Self::ScramSha256 => "scram-sha256",
            Self::ScramSha512 => "scram-sha512",
            Self::Gssapi => "gssapi",
            Self::Mtls => "mtls",
        };
        f.write_str(s)
    }
}

/// 认证配置：上游/下游通用。`mechanism` 决定走哪条路径。
#[derive(Debug, Default, Deserialize)]
pub struct AuthConfig {
    /// 认证机制(枚举)，缺省 none。
    #[serde(default)]
    pub mechanism: AuthMechanism,
    // plain/scram 用户名密码
    pub username: Option<String>,
    pub password: Option<String>,
    // gssapi
    pub kerberos_principal: Option<String>,
    pub kerberos_keytab: Option<String>,
    pub kerberos_kdc: Option<String>,
    pub kerberos_realm: Option<String>,
    // mtls / tls
    pub ca_file: Option<String>,
    pub server_name: Option<String>,
    #[serde(default = "default_true")]
    pub verify: bool,
}

fn default_true() -> bool {
    true
}

/// 连接池配置(§3.6)。
#[derive(Debug, Deserialize)]
pub struct PoolConfig {
    /// `pooled`(多路复用) 或 `per_connection`(1:1)。默认 pooled。
    #[serde(default = "default_pool_mode")]
    pub mode: PoolMode,
    /// 单 broker 池上限；超过则排队(背压)。默认 16。
    #[serde(default = "default_max_per_broker")]
    pub max_per_broker: usize,
    /// 最小空闲，预热避免冷启动握手。默认 2。
    #[serde(default = "default_min_idle")]
    pub min_idle: usize,
    /// 空闲连接超时回收。默认 5m。
    #[serde(
        default = "default_idle_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub idle_timeout: Duration,
    /// 借用等待超时。默认 5s。
    #[serde(
        default = "default_acquire_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub acquire_timeout: Duration,
    /// 探活间隔。默认 30s。
    #[serde(
        default = "default_health_check",
        deserialize_with = "deserialize_duration"
    )]
    pub health_check: Duration,
    /// correlation_id 映射表上限(背压阈值)。默认 100000。
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    /// RSS 熔断阈值(字节)，触顶拒新连接(防 OOM)。0 表示不限制。默认 0。
    #[serde(default)]
    pub max_rss_bytes: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            mode: default_pool_mode(),
            max_per_broker: default_max_per_broker(),
            min_idle: default_min_idle(),
            idle_timeout: default_idle_timeout(),
            acquire_timeout: default_acquire_timeout(),
            health_check: default_health_check(),
            max_in_flight: default_max_in_flight(),
            max_rss_bytes: 0,
        }
    }
}

/// 连接模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolMode {
    /// 多路复用池：多条下游连接复用少量上游连接 + cid 重映射。
    Pooled,
    /// 1:1 流转发：每条下游连接独占一条上游连接，最稳。
    PerConnection,
}

fn default_pool_mode() -> PoolMode {
    PoolMode::Pooled
}
fn default_max_per_broker() -> usize {
    16
}
fn default_min_idle() -> usize {
    2
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(300)
}
fn default_acquire_timeout() -> Duration {
    Duration::from_secs(5)
}
fn default_health_check() -> Duration {
    Duration::from_secs(30)
}
fn default_max_in_flight() -> usize {
    100_000
}

/// Web API 配置(§7 + 辅助调试)。
///
/// 统一 HTTP 服务，默认启动：`/health` 健康检查、`/metrics` Prometheus 指标、
/// `/doctor/*` 调试接口(查询消费者组/消费最新消息/发送消息)，均复用同一端口，
/// 按路径分发(见 .clinerules：倾向默认启动 web 端口，health/metrics/doctor
/// 统一作为 router 添加，移除单独启动 tcp 端口的代码)。
///
/// 端点(均返回 JSON / Prometheus 文本，暂不实现前端页面)：
/// - `GET  /health`                  存活检查(k8s liveness/readiness)
/// - `GET  /metrics`                 Prometheus 文本格式指标
/// - `GET  /doctor/consumers/{topic}` 查询某 topic 的消费者组(成员/分区/lag)
/// - `GET  /doctor/messages/{topic}`  查看某 topic 最新数据(默认前 5 条)
/// - `POST /doctor/messages/{topic}`  给某 topic 发送数据
#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    /// Web 端口监听地址。默认 127.0.0.1:9100（默认启动）。
    #[serde(default = "default_api_listen")]
    pub listen: String,

    /// `GET /debug/messages/{topic}` 默认返回的记录数。默认 5。
    #[serde(default = "default_debug_count")]
    pub default_count: usize,

    /// 是否启用 metrics 采集。默认 false（关闭以降低性能开销）。
    #[serde(default)]
    pub metrics_enabled: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: default_api_listen(),
            default_count: default_debug_count(),
            metrics_enabled: false,
        }
    }
}

fn default_api_listen() -> String {
    "127.0.0.1:9100".to_string()
}
fn default_debug_count() -> usize {
    5
}

/// 高可用模式(§5)：用枚举匹配，避免字符串拼写错误(见 .clinerules Code Style)。
///
/// TOML 用 snake_case：`stateless_replicas`(k8s) / `dns_round_robin`(传统部署)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaMode {
    /// k8s 无状态多副本：靠 Service 负载均衡，proxy 本身无状态。
    #[default]
    #[serde(alias = "stateless-replicas")]
    StatelessReplicas,
    /// 传统部署 DNS 轮询：多个 proxy 实例注册到同一 DNS A 记录。
    #[serde(alias = "dns-round-robin")]
    DnsRoundRobin,
}

impl std::fmt::Display for HaMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::StatelessReplicas => "stateless-replicas",
            Self::DnsRoundRobin => "dns-round-robin",
        };
        f.write_str(s)
    }
}

/// 高可用配置(§5)。
#[derive(Debug, Default, Deserialize)]
pub struct HaConfig {
    /// 高可用模式(枚举)，默认 stateless_replicas。
    #[serde(default)]
    pub mode: HaMode,
}

impl ProxyConfig {
    /// 从 TOML 文件加载。
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let text =
            std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Read(e.to_string()))?;
        Self::from_str(&text)
    }

    /// 从 TOML 字符串解析。
    pub fn from_str(text: &str) -> Result<Self, ConfigError> {
        let cfg: ProxyConfig =
            toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.cluster.bootstrap_servers.is_empty() {
            return Err(ConfigError::Invalid(
                "cluster.bootstrap_servers 不能为空".into(),
            ));
        }
        if self.proxy.bootstrap_server_mapping.is_empty() {
            return Err(ConfigError::Invalid(
                "proxy.bootstrap_server_mapping 不能为空(按 bootstrap_servers 顺序\
                 配置下游监听地址，如 [\"proxy.svc:19092\", ...])"
                    .into(),
            ));
        }
        if self.proxy.bootstrap_server_mapping.len() != self.cluster.bootstrap_servers.len() {
            return Err(ConfigError::Invalid(format!(
                "proxy.bootstrap_server_mapping 长度({})必须与 cluster.bootstrap_servers\
                 长度({})相等，按顺序一一对应",
                self.proxy.bootstrap_server_mapping.len(),
                self.cluster.bootstrap_servers.len()
            )));
        }
        if self.pool.min_idle > self.pool.max_per_broker {
            return Err(ConfigError::Invalid(format!(
                "pool.min_idle({}) 不能大于 pool.max_per_broker({})",
                self.pool.min_idle, self.pool.max_per_broker
            )));
        }
        // 校验每项格式：支持 1/2/3 段(逗号分隔)，见 review D8 三元组。
        //   - "advertise_host:port"
        //   - "bind_host:port,advertise_host:port"
        //   - "orig_broker,bind_host:port,advertise_host:port"
        // 校验最后一段(广告地址)的主机名非空(缺省时回退 advertise_host)。
        for (i, m) in self.proxy.bootstrap_server_mapping.iter().enumerate() {
            let parts: Vec<&str> = m.split(',').map(|s| s.trim()).collect();
            let advertise_part = match parts.len() {
                1 | 2 | 3 => parts[parts.len() - 1],
                _ => {
                    return Err(ConfigError::Invalid(format!(
                        "proxy.bootstrap_server_mapping[{i}] = {m:?} 格式非法：支持 1/2/3 段(逗号分隔)"
                    )));
                }
            };
            let host = advertise_part
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or("");
            if host.is_empty() {
                // 主机名缺省：必须配了 advertise_host 作回退。
                if self
                    .proxy
                    .advertise_host
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(ConfigError::Invalid(format!(
                        "proxy.bootstrap_server_mapping[{i}] = {m:?} 缺少广告主机名，\
                         且未配置 proxy.advertise_host 作回退"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// 从 TOML 反序列化 Duration：支持字符串如 `"5s"`/`"10m"`/`"1h"`/`"500ms"`，
/// 也支持整数(按秒)。配合 `#[serde(default = "...", deserialize_with = ...)]`，
/// 字段缺省时走 default(已是 Duration)，有值时走本函数。
fn deserialize_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    use serde::de;
    struct DurVisitor;
    impl de::Visitor<'_> for DurVisitor {
        type Value = Duration;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a duration string like \"5s\", \"10m\", \"1h\" or an integer (seconds)")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Duration, E> {
            parse_duration_str(v).map_err(E::custom)
        }
        fn visit_string<E: de::Error>(self, v: String) -> Result<Duration, E> {
            self.visit_str(&v)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Duration, E> {
            Ok(Duration::from_secs(v.max(0) as u64))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Duration, E> {
            Ok(Duration::from_secs(v))
        }
    }
    d.deserialize_any(DurVisitor)
}

/// 解析时长字符串：支持后缀 s/m/h/ms/us，纯数字按秒。
fn parse_duration_str(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        return num
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| format!("无效的毫秒数: {s}"));
    }
    if let Some(num) = s.strip_suffix("us") {
        return num
            .parse::<u64>()
            .map(Duration::from_micros)
            .map_err(|_| format!("无效的微秒数: {s}"));
    }
    let (num_str, mul) = if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else {
        // 无后缀：按秒。
        return s
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| format!("无效的时长: {s}"));
    };
    num_str
        .parse::<u64>()
        .map(|n| Duration::from_secs(n * mul))
        .map_err(|_| format!("无效的时长数值: {s}"))
}

/// 反序列化 `Option<Duration>`：TOML 字段缺失或为 null → None，有值 → 按 deserialize_duration 解析。
fn deserialize_optional_duration<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<Duration>, D::Error> {
    use serde::de;
    struct OptDurVisitor;
    impl de::Visitor<'_> for OptDurVisitor {
        type Value = Option<Duration>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a duration string or null")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            parse_duration_str(v).map(Some).map_err(E::custom)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(Duration::from_secs(v.max(0) as u64)))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(Duration::from_secs(v)))
        }
    }
    d.deserialize_any(OptDurVisitor)
}

/// 解析 `host:port` 字符串为 SocketAddr（解析主机名）。
pub async fn parse_addr(s: &str) -> Result<SocketAddr, ConfigError> {
    tokio::net::lookup_host(s)
        .await
        .map_err(|e| ConfigError::Resolve(format!("{s}: {e}")))?
        .next()
        .ok_or_else(|| ConfigError::Resolve(format!("{s}: 无地址解析结果")))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("读取配置文件失败: {0}")]
    Read(String),
    #[error("解析 TOML 失败: {0}")]
    Parse(String),
    #[error("配置无效: {0}")]
    Invalid(String),
    #[error("地址解析失败: {0}")]
    Resolve(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let toml = r#"
[cluster]
bootstrap_servers = ["host1:9092","host2:9092"]

[proxy]
bootstrap_server_mapping = ["proxy.svc:9092", "proxy.svc:9093"]
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.cluster.bootstrap_servers.len(), 2);
        assert_eq!(cfg.proxy.bootstrap_server_mapping.len(), 2);
        assert_eq!(cfg.proxy.bootstrap_server_mapping[0], "proxy.svc:9092");
        assert_eq!(cfg.proxy.listen_bind, "0.0.0.0");
        assert_eq!(cfg.proxy.max_downstream_connections, 10000);
        assert_eq!(cfg.upstream.auth.mechanism, AuthMechanism::None);
    }

    #[test]
    fn parse_with_gssapi() {
        let toml = r#"
[cluster]
bootstrap_servers = ["host1:9092"]

[proxy]
bootstrap_server_mapping = ["proxy.svc:9092"]

[upstream.auth]
mechanism = "gssapi"
kerberos_principal = "dayu@HADOOP.COM"
kerberos_keytab = "/etc/kp/dayukb.keytab"
kerberos_kdc = "kdc:88"
kerberos_realm = "HADOOP.COM"

[downstream.auth]
mechanism = "none"
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.upstream.auth.mechanism, AuthMechanism::Gssapi);
        assert_eq!(
            cfg.upstream.auth.kerberos_principal.as_deref(),
            Some("dayu@HADOOP.COM")
        );
        assert_eq!(cfg.downstream.auth.mechanism, AuthMechanism::None);
    }

    #[test]
    fn rejects_empty_bootstrap() {
        let toml = r#"
[cluster]
bootstrap_servers = []

[proxy]
bootstrap_server_mapping = ["p:9092"]
"#;
        assert!(ProxyConfig::from_str(toml).is_err());
    }

    #[test]
    fn rejects_empty_mapping() {
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = []
"#;
        assert!(ProxyConfig::from_str(toml).is_err());
    }

    #[test]
    fn rejects_mapping_length_mismatch() {
        let toml = r#"
[cluster]
bootstrap_servers = ["h1:9092", "h2:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]
"#;
        let err = ProxyConfig::from_str(toml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("相等"), "应报长度不匹配: {msg}");
    }

    #[test]
    fn pool_and_api_defaults() {
        // 不写 [pool]/[api] 段，应使用默认值。
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.pool.mode, PoolMode::Pooled);
        assert_eq!(cfg.pool.max_per_broker, 16);
        assert_eq!(cfg.pool.min_idle, 2);
        assert_eq!(cfg.pool.idle_timeout, Duration::from_secs(300));
        assert_eq!(cfg.pool.acquire_timeout, Duration::from_secs(5));
        assert_eq!(cfg.pool.health_check, Duration::from_secs(30));
        assert_eq!(cfg.pool.max_in_flight, 100_000);
        assert_eq!(cfg.pool.max_rss_bytes, 0);
        // [api] 默认：web 端口默认启动，health/metrics/debug 路径就绪。
        assert_eq!(cfg.api.listen, "127.0.0.1:9100");
        assert_eq!(cfg.api.default_count, 5);
        assert_eq!(cfg.ha.mode, HaMode::StatelessReplicas);
    }

    #[test]
    fn pool_per_connection_mode() {
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]

[pool]
mode = "per_connection"
max_per_broker = 8
min_idle = 1
idle_timeout = "10s"
acquire_timeout = "2s"
health_check = "15s"
max_in_flight = 50000
max_rss_bytes = 536870912
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.pool.mode, PoolMode::PerConnection);
        assert_eq!(cfg.pool.max_per_broker, 8);
        assert_eq!(cfg.pool.min_idle, 1);
        assert_eq!(cfg.pool.idle_timeout, Duration::from_secs(10));
        assert_eq!(cfg.pool.max_rss_bytes, 536_870_912);
    }

    #[test]
    fn ha_mode_kebab_alias() {
        // 验证 kebab-case 别名(stateless-replicas / dns-round-robin)可解析为枚举。
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]

[ha]
mode = "dns-round-robin"
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.ha.mode, HaMode::DnsRoundRobin);

        // snake_case 也可。
        let toml2 = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]

[ha]
mode = "stateless_replicas"
"#;
        let cfg2 = ProxyConfig::from_str(toml2).unwrap();
        assert_eq!(cfg2.ha.mode, HaMode::StatelessReplicas);
    }

    #[test]
    fn api_listen_override() {
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
bootstrap_server_mapping = ["p:9092"]

[api]
listen = "0.0.0.0:8080"
default_count = 20
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.api.listen, "0.0.0.0:8080");
        assert_eq!(cfg.api.default_count, 20);
    }

    #[test]
    fn mapping_host_fallback_to_advertise() {
        // 每项只写端口(主机名缺省)，用 advertise_host 回退应通过校验。
        let toml = r#"
[cluster]
bootstrap_servers = ["h:9092"]

[proxy]
advertise_host = "proxy.svc"
bootstrap_server_mapping = [":9092"]
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(cfg.proxy.bootstrap_server_mapping[0], ":9092");
    }

    #[test]
    fn mapping_triplet_format_accepted() {
        // 三元组格式(原始broker,绑定地址,广告地址)应通过校验(见 review D8)。
        let toml = r#"
[cluster]
bootstrap_servers = ["broker1:9092"]

[proxy]
bootstrap_server_mapping = ["broker1:9092,0.0.0.0:19193,10.57.92.174:19193"]
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(
            cfg.proxy.bootstrap_server_mapping[0],
            "broker1:9092,0.0.0.0:19193,10.57.92.174:19193"
        );
    }

    #[test]
    fn mapping_two_part_format_accepted() {
        // 两段格式(绑定地址,广告地址)应通过校验。
        let toml = r#"
[cluster]
bootstrap_servers = ["broker1:9092"]

[proxy]
bootstrap_server_mapping = ["0.0.0.0:19193,10.57.92.174:19193"]
"#;
        let cfg = ProxyConfig::from_str(toml).unwrap();
        assert_eq!(
            cfg.proxy.bootstrap_server_mapping[0],
            "0.0.0.0:19193,10.57.92.174:19193"
        );
    }

    #[test]
    fn mapping_triplet_missing_advertise_host_rejected() {
        // 三元组广告地址主机名缺省且无 advertise_host 回退 → 拒绝。
        let toml = r#"
[cluster]
bootstrap_servers = ["broker1:9092"]

[proxy]
bootstrap_server_mapping = ["broker1:9092,0.0.0.0:19193,:19193"]
"#;
        assert!(ProxyConfig::from_str(toml).is_err());
    }
}
