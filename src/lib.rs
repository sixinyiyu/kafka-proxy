//! kafka-proxy: 透明 Kafka 代理。

//!
//! 让 Java/Rust/Go 客户端，仅改 `bootstrap.servers` 指向本代理，
//! 即可访问启用/启用 SASL/GSSAPI(Kerberos) 的 Kafka 集群。认证由 proxy 统一承担。
//!
//! 端口-per-broker + per_connection(1:1) 或 pooled(多路复用) 帧转发 + 元数据改写
pub mod api;
pub mod config;
pub mod metrics;
pub mod pool;
pub mod relay;

pub mod rewrite;
pub mod upstream;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{PoolConfig, PoolMode, ProxyConfig};
use crate::metrics::SharedMetrics;
use crate::pool::{BrokerPool, CidRemap, PoolLimits};
use crate::relay::{ConnectionTracker, PooledRelay, Relay};
use crate::rewrite::RewriteMap;
use crate::upstream::{UpstreamAuth, UpstreamError};

/// 单个下游监听器的目标：node_id + 真实 broker 地址/主机名。
#[derive(Clone, Debug)]
pub struct BrokerTarget {
    pub node_id: i32,
    pub real_addr: SocketAddr,
    pub real_host: String,
}

impl BrokerTarget {
    fn debug_broker(&self) -> String {
        format!(
            "node_id: {}, raddr: {}, host: {}",
            self.node_id, self.real_addr, self.real_host
        )
    }
}

/// 启动代理：bootstrap 拉元数据 → 绑定监听 → accept 转发。
///
/// `cancel` 用于优雅关闭：收到信号后各 accept loop 停止接收新连接，
/// 等待在途请求自然排空后才退出。
pub async fn run(config: ProxyConfig, cancel: CancellationToken) -> Result<(), ProxyError> {
    let upstream_auth = Arc::new(UpstreamAuth::from_config(&config.upstream.auth)?);
    let metrics: SharedMetrics = Arc::new(crate::metrics::Metrics::new(config.api.metrics_enabled));
    if config.api.metrics_enabled {
        info!("metrics 采集已启用");
    } else {
        info!("metrics 采集已关闭(默认)，如需开启请设 [api].metrics_enabled = true");
    }

    // 下游连接追踪器：peer → (node_id, started_at)，供 /doctor/connections 查询。
    let conn_tracker: ConnectionTracker = Arc::new(dashmap::DashMap::new());

    let (targets, client) = bootstrap_targets(&config, &upstream_auth).await?;
    info!(
        "bootstrap success target  brokers {}",
        targets
            .iter()
            .map(|t| t.debug_broker())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let client = Arc::new(client);

    let (rewrite_map_inner, listeners) = build_rewrite_map(&config, &targets).await?;
    // 以可读格式打印「监听端口 → 真实 broker」映射(按 bootstrap 顺序)，
    // 避免直接 Debug 整个结构体导致日志难以阅读。
    info!(
        "下游监听映射(按 bootstrap 顺序): {}",
        listeners
            .iter()
            .map(|item| item.info())
            .collect::<Vec<_>>()
            .join("\n")
    );

    let rewrite_map = Arc::new(rewrite_map_inner);

    let client_idle_timeout = config.proxy.client_idle_timeout;
    let max_rss_bytes = if config.pool.max_rss_bytes > 0 {
        Some(config.pool.max_rss_bytes)
    } else {
        None
    };

    let api_deps = crate::api::ApiDeps::new(
        client.clone(),
        metrics.clone(),
        conn_tracker.clone(),
        &config.api,
    );
    let api_cfg = config.api.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::api::serve(api_deps, &api_cfg).await {
            warn!("Web 端点异常退出: {e}");
        }
    });

    let mode = config.pool.mode;
    info!(?mode, "转发模式");
    let cid_remap = Arc::new(CidRemap::with_max_in_flight(config.pool.max_in_flight));
    let limits = pool_limits(&config.pool);

    let mut handles = tokio::task::JoinSet::new();
    for ml in &listeners {
        // bind_addr 可能是 ":port"(用 listen_bind 回退) 或 "host:port"(三元组独立绑定地址)。
        let bind = if ml.bind_addr.starts_with(':') {
            format!("{}{}", config.proxy.listen_bind, ml.bind_addr)
        } else {
            ml.bind_addr.clone()
        };
        let listener = TcpListener::bind(&bind).await?;
        info!(node_id = ml.node_id, %bind, advertise = ml.port, "ready listener broker");

        match mode {
            PoolMode::PerConnection => {
                let t = ml.target.clone();
                let auth = upstream_auth.clone();
                let map = rewrite_map.clone();
                let m = metrics.clone();
                let max_conn = config.proxy.max_downstream_connections;
                let tracker = conn_tracker.clone();
                let idle = client_idle_timeout;
                let cancel2 = cancel.clone();
                handles.spawn(async move {
                    accept_loop_per_connection(
                        listener,
                        t,
                        auth,
                        map,
                        m,
                        max_conn,
                        idle,
                        tracker,
                        max_rss_bytes,
                        cancel2,
                    )
                    .await;
                });
            }
            PoolMode::Pooled => {
                let pool = BrokerPool::new(
                    ml.node_id,
                    ml.target.real_addr,
                    ml.target.real_host.clone(),
                    (*upstream_auth).clone(),
                    limits,
                    cid_remap.clone(),
                    rewrite_map.clone(),
                    metrics.clone(),
                );
                // 预热连接 + 启动空闲回收。
                pool.warmup().await;
                pool.start_reaper();
                let m = metrics.clone();
                let max_conn = config.proxy.max_downstream_connections;
                let tracker = conn_tracker.clone();
                let idle = client_idle_timeout;
                let cancel2 = cancel.clone();
                handles.spawn(async move {
                    accept_loop_pooled(
                        listener,
                        pool,
                        m,
                        max_conn,
                        idle,
                        tracker,
                        max_rss_bytes,
                        cancel2,
                    )
                    .await;
                });
            }
        }
    }

    // 等待所有 accept loop 退出（cancel 触发后或 listener 异常退出）
    while (handles.join_next().await).is_some() {}
    info!("所有 accept loop 已退出，在途请求已排空");
    Ok(())
}

fn pool_limits(cfg: &PoolConfig) -> PoolLimits {
    PoolLimits {
        max_per_broker: cfg.max_per_broker,
        min_idle: cfg.min_idle,
        idle_timeout: cfg.idle_timeout,
        acquire_timeout: cfg.acquire_timeout,
        max_in_flight: cfg.max_in_flight,
    }
}

#[derive(Clone, Debug)]
struct MappedListener {
    node_id: i32,
    /// 对外广告端口(写入改写后的元数据)。
    port: u16,
    /// 实际绑定监听地址(可能含独立 bind_host，见 review D8 三元组)。
    bind_addr: String,
    target: BrokerTarget,
}

impl MappedListener {
    fn info(&self) -> String {
        format!(
            "{} -> {{node_id: {}, real_addr: {}, real_host: {}}}",
            self.port, self.node_id, self.target.real_addr, self.target.real_host
        )
    }
}

/// 解析 `bootstrap_server_mapping` 单项，支持三种格式
///
/// - `"advertise_host:port"` — 绑定 listen_bind:port，广告 advertise_host:port
/// - `"bind_host:port,advertise_host:port"` — 绑定 bind_host:port，广告 advertise_host:port
/// - `"orig_broker,bind_host:port,advertise_host:port"` — 三元组(第一段忽略，同上)
///
/// 返回 (bind_addr, advertise_host, advertise_port)。
fn parse_mapping_entry(
    mapping: &str,
    fallback_host: &str,
) -> Result<(String, String, u16), ProxyError> {
    let parts: Vec<&str> = mapping.split(',').map(|s| s.trim()).collect();
    // 取最后 1 或 2 段作为 bind/advertise(三元组时第 1 段是原始 broker，忽略)。
    let (bind_part, advertise_part) = match parts.len() {
        1 => {
            // 单段：仅 advertise host:port，绑定用 listen_bind(由 caller 拼)。
            (None, parts[0])
        }
        2 => {
            // 两段：bind, advertise
            (Some(parts[0]), parts[1])
        }
        3 => {
            // 三元组：orig, bind, advertise
            (Some(parts[1]), parts[2])
        }
        _ => {
            return Err(ProxyError::Config(format!(
                "bootstrap_server_mapping 项 {mapping:?} 格式非法：支持 1/2/3 段(逗号分隔)"
            )));
        }
    };

    // 解析 advertise host:port。
    let (adv_host_raw, adv_port_str) = advertise_part.rsplit_once(':').ok_or_else(|| {
        ProxyError::Config(format!(
            "bootstrap_server_mapping 项 {mapping:?} 的广告地址缺少 :port"
        ))
    })?;
    let advertise_host = if adv_host_raw.is_empty() {
        fallback_host.to_string()
    } else {
        adv_host_raw.to_string()
    };
    let advertise_port: u16 = adv_port_str.parse().map_err(|_| {
        ProxyError::Config(format!(
            "bootstrap_server_mapping 项 {mapping:?} 广告端口非法"
        ))
    })?;

    // 绑定地址：若有 bind 段则用其 host:port，否则用 advertise_port(由 caller 拼 listen_bind)。
    let bind_addr = match bind_part {
        Some(b) => {
            // bind 段形如 host:port 或 :port
            let (bind_host, bind_port_str) = b.rsplit_once(':').ok_or_else(|| {
                ProxyError::Config(format!(
                    "bootstrap_server_mapping 项 {mapping:?} 的绑定地址缺少 :port"
                ))
            })?;
            let bind_port: u16 = bind_port_str.parse().map_err(|_| {
                ProxyError::Config(format!(
                    "bootstrap_server_mapping 项 {mapping:?} 绑定端口非法"
                ))
            })?;
            if bind_host.is_empty() {
                format!(":{}", bind_port)
            } else {
                format!("{}:{}", bind_host, bind_port)
            }
        }
        None => format!(":{}", advertise_port),
    };

    Ok((bind_addr, advertise_host, advertise_port))
}

async fn build_rewrite_map(
    config: &ProxyConfig,
    targets: &[BrokerTarget],
) -> Result<(RewriteMap, Vec<MappedListener>), ProxyError> {
    let fallback_host = config.proxy.advertise_host.as_deref().unwrap_or("");
    let mut node_ports: std::collections::HashMap<i32, (String, u16)> =
        std::collections::HashMap::new();
    let mut listeners = Vec::with_capacity(config.cluster.bootstrap_servers.len());

    for (bs, mapping) in config
        .cluster
        .bootstrap_servers
        .iter()
        .zip(config.proxy.bootstrap_server_mapping.iter())
    {
        let (bind_addr, advertise_host, port) = parse_mapping_entry(mapping, fallback_host)?;

        let bs_addr = config::parse_addr(bs)
            .await
            .map_err(ProxyError::ConfigParse)?;
        let bs_host = bs.rsplit_once(':').map(|(h, _)| h).unwrap_or(bs);
        // 先用 IP+端口精确匹配，失败时回退到 hostname 匹配(兼容内网/外网 IP 差异)。
        let target = targets.iter().find(|t| {
            (t.real_addr.ip() == bs_addr.ip() && t.real_addr.port() == bs_addr.port())
                || t.real_host == bs_host
        });
        let target = target.ok_or_else(|| {
            ProxyError::Config(format!(
                "bootstrap_servers 项 {bs:?} 在集群元数据中找不到对应 broker；已知 broker: {:?}",
                targets
                    .iter()
                    .map(|t| format!("{}({})", t.real_host, t.node_id))
                    .collect::<Vec<_>>()
            ))
        })?;
        let target = target.clone();

        node_ports.insert(target.node_id, (advertise_host.clone(), port));
        listeners.push(MappedListener {
            node_id: target.node_id,
            port,
            bind_addr,
            target,
        });
    }

    Ok((RewriteMap::new(node_ports), listeners))
}

async fn bootstrap_targets(
    config: &ProxyConfig,
    upstream_auth: &UpstreamAuth,
) -> Result<(Vec<BrokerTarget>, kafka_client::Client), ProxyError> {
    let builder = kafka_client::Client::builder(config.cluster.bootstrap_servers.clone());
    let mut builder = upstream_auth.apply_to_client_builder(builder);

    if matches!(
        upstream_auth.inner_ref(),
        crate::upstream::AuthInner::Gssapi { .. }
    ) && let Some(first_bs) = config.cluster.bootstrap_servers.first()
        && let Some(host) = first_bs.rsplit_once(':').map(|(h, _)| h)
    {
        builder = builder.with_broker_hostname(host.to_string());
    }

    info!("try to connect bootstrap broker......");
    let client = tokio::time::timeout(std::time::Duration::from_secs(30), builder.build())
        .await
        .map_err(|_| {
            ProxyError::Kafka(kafka_client::KafkaError::Io(
                "bootstrap 连接超时(30s)，请检查 bootstrap_servers 是否可达".into(),
            ))
        })??;
    info!("bootstrap broker successfully established connection，正在刷新元数据......");
    match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        client.refresh_metadata(),
    )
    .await
    {
        Ok(Ok(())) => {
            info!("元数据刷新完成");
        }
        Ok(Err(e)) => {
            warn!(?e, "元数据刷新失败，尝试降级：仅使用 bootstrap broker");
        }
        Err(_elapsed) => {
            warn!("元数据刷新超时(30s)，尝试降级：仅使用 bootstrap broker");
        }
    }

    let brokers = client.metadata().get_all_brokers().await;
    if brokers.is_empty() {
        warn!("元数据为空，用 bootstrap_servers 构造 fallback broker 列表");
        let mut targets = Vec::new();
        for (i, bs) in config.cluster.bootstrap_servers.iter().enumerate() {
            let addr = config::parse_addr(bs)
                .await
                .map_err(ProxyError::ConfigParse)?;
            targets.push(BrokerTarget {
                node_id: -(i as i32 + 1),
                real_addr: addr,
                real_host: bs
                    .rsplit_once(':')
                    .map(|(h, _)| h)
                    .unwrap_or(bs)
                    .to_string(),
            });
        }
        return Ok((targets, client));
    }

    let mut targets = Vec::with_capacity(brokers.len());
    for b in brokers {
        let addr_str = format!("{}:{}", b.host, b.port);
        let addr = config::parse_addr(&addr_str).await?;
        targets.push(BrokerTarget {
            node_id: b.node_id,
            real_addr: addr,
            real_host: b.host.clone(),
        });
    }

    Ok((targets, client))
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop_per_connection(
    listener: TcpListener,

    target: BrokerTarget,
    auth: Arc<UpstreamAuth>,
    rewrite_map: Arc<RewriteMap>,
    metrics: SharedMetrics,
    max_connections: usize,
    client_idle_timeout: Option<std::time::Duration>,
    conn_tracker: ConnectionTracker,
    max_rss_bytes: Option<u64>,
    cancel: CancellationToken,
) {
    use std::time::Instant;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(node_id = target.node_id, "accept loop 收到关闭信号，停止接受新连接");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        // RSS 熔断：超过内存上限时拒绝新连接。
                        if let Some(max_rss) = max_rss_bytes {
                            let rss = crate::metrics::process_rss_bytes();
                            if rss > max_rss {
                                warn!(
                                    node_id = target.node_id,
                                    rss_bytes = rss,
                                    max_rss_bytes = max_rss,
                                    "RSS 超过上限，拒绝新连接"
                                );
                                drop(stream);
                                continue;
                            }
                        }

                        if metrics.downstream_connection_count() as usize >= max_connections {
                            warn!(
                                node_id = target.node_id,
                                "下游连接数达上限 {max_connections}，拒绝新连接"
                            );
                            drop(stream);
                            continue;
                        }
                        metrics.inc_downstream_connections();
                        conn_tracker.insert(peer, (target.node_id, Instant::now()));
                        info!(node_id = target.node_id, %peer, "下游连接接入");
                        let relay = Relay::new(
                            stream,
                            target.real_addr,
                            target.real_host.clone(),
                            (*auth).clone(),
                            rewrite_map.clone(),
                            metrics.clone(),
                        )
                        .with_client_idle_timeout(client_idle_timeout);
                        let m = metrics.clone();
                        let t = conn_tracker.clone();
                        tokio::spawn(async move {
                            relay.run().await;
                            m.dec_downstream_connections();
                            t.remove(&peer);
                        });
                    }
                    Err(e) => {
                        warn!(node_id = target.node_id, "accept 失败: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop_pooled(
    listener: TcpListener,
    pool: Arc<BrokerPool>,

    metrics: SharedMetrics,
    max_connections: usize,
    client_idle_timeout: Option<std::time::Duration>,
    conn_tracker: ConnectionTracker,
    max_rss_bytes: Option<u64>,
    cancel: CancellationToken,
) {
    use std::time::Instant;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(node_id = pool.node_id, "accept loop 收到关闭信号，停止接受新连接");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        // RSS 熔断：超过内存上限时拒绝新连接。
                        if let Some(max_rss) = max_rss_bytes {
                            let rss = crate::metrics::process_rss_bytes();
                            if rss > max_rss {
                                warn!(
                                    node_id = pool.node_id,
                                    rss_bytes = rss,
                                    max_rss_bytes = max_rss,
                                    "RSS 超过上限，拒绝新连接"
                                );
                                drop(stream);
                                continue;
                            }
                        }

                        if metrics.downstream_connection_count() as usize >= max_connections {
                            warn!(
                                node_id = pool.node_id,
                                "下游连接数达上限 {max_connections}，拒绝新连接"
                            );
                            drop(stream);
                            continue;
                        }
                        metrics.inc_downstream_connections();
                        conn_tracker.insert(peer, (pool.node_id, Instant::now()));
                        info!(node_id = pool.node_id, %peer, "下游连接接入(pooled)");
                        let p = pool.clone();
                        let m = metrics.clone();
                        let t = conn_tracker.clone();
                        tokio::spawn(async move {
                            PooledRelay::new(stream, p, m.clone())
                                .with_client_idle_timeout(client_idle_timeout)
                                .run()
                                .await;
                            m.dec_downstream_connections();
                            t.remove(&peer);
                        });
                    }
                    Err(e) => {
                        warn!(node_id = pool.node_id, "accept 失败: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("配置错误: {0}")]
    Config(String),
    #[error("上游认证配置错误: {0}")]
    UpstreamAuth(#[from] UpstreamError),
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("配置解析: {0}")]
    ConfigParse(#[from] config::ConfigError),
    #[error("kafka_client 错误: {0}")]
    Kafka(#[from] kafka_client::KafkaError),
}
