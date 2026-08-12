//! 帧转发 — 见设计文档 §6.1(per_connection) 与 §6.3(pooled)。
//!
//! ## per_connection 模式(1:1)
//! 一条下游连接独占一条上游已认证连接。两个方向各一个任务：
//!   down→up: 读下游帧 → 记录 (cid → api_key, version) → 原样写上游。
//!   up→down: 读上游帧 → 查 cid 得 (api_key, version) → 需改写则改写 → 写下游。
//! correlation_id 全程透传(1:1 不冲突)。任一方向 EOF/出错 → 关闭对端。
//!
//! ## pooled 模式(多路复用)
//! 一条下游连接不再独占上游连接，而是从 BrokerPool 借用：每收到一个下游请求帧，
//! 借一条上游连接、分配 cid_u、改写帧头写上游；上游响应由 pool 的读循环经
//! cid_remap 路由回来，PooledRelay 只负责把响应写回下游。
//!
//! ## 性能优化
//! - 连接指标通过 RAII guard 保证即使 task panic 也能正确递减

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use kafka_client::wire::{KafkaCodec, KafkaFrame};

use crate::metrics::SharedMetrics;
use crate::pool::{BrokerPool, DownstreamResp, WriteReq};
use crate::rewrite::{self, RewriteMap};
use crate::upstream::{UpstreamAuth, UpstreamError};

/// RAII guard：drop 时递减上游连接计数，确保 panic 也不泄漏。
struct UpstreamGuard(SharedMetrics);
impl Drop for UpstreamGuard {
    fn drop(&mut self) {
        self.0.dec_upstream_connections();
    }
}

/// 下游连接追踪器：peer → (node_id, started_at)，供 /doctor/connections 查询。
pub type ConnectionTracker = Arc<dashmap::DashMap<std::net::SocketAddr, (i32, std::time::Instant)>>;

/// 一条下游连接 + 它对应的真实 broker 信息（per_connection 模式）。
pub struct Relay {
    downstream: TcpStream,
    /// 真实 broker 地址(已解析)。
    broker_addr: std::net::SocketAddr,
    /// 真实 broker 主机名(Kerberos 服务 principal 用)。
    broker_host: String,
    upstream_auth: UpstreamAuth,
    rewrite_map: Arc<RewriteMap>,
    metrics: SharedMetrics,
    /// 客户端空闲超时：下游在此时间内未发帧则主动关闭。None 即无限等。
    client_idle_timeout: Option<Duration>,
}

impl Relay {
    pub fn new(
        downstream: TcpStream,
        broker_addr: std::net::SocketAddr,
        broker_host: String,
        upstream_auth: UpstreamAuth,
        rewrite_map: Arc<RewriteMap>,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            downstream,
            broker_addr,
            broker_host,
            upstream_auth,
            rewrite_map,
            metrics,
            client_idle_timeout: None,
        }
    }

    /// 设置客户端空闲超时。
    pub fn with_client_idle_timeout(mut self, t: Option<Duration>) -> Self {
        self.client_idle_timeout = t;
        self
    }

    /// 运行转发循环，直到任一方向结束。
    pub async fn run(self) {
        match self.run_inner().await {
            Ok(()) => debug!("relay 正常结束"),
            Err(e) => warn!("relay 结束: {e}"),
        }
    }

    async fn run_inner(self) -> Result<(), RelayError> {
        // 建立上游已认证 framed 流。
        let upstream = self
            .upstream_auth
            .connect_framed(self.broker_addr, &self.broker_host)
            .await?;
        self.metrics.inc_upstream_connections();
        // RAII guard：即使 spawn 的 task panic，也能正确递减连接计数。
        let _guard = UpstreamGuard(self.metrics.clone());

        // 下游也用 KafkaCodec 分帧(明文)。
        let downstream = Framed::new(self.downstream, KafkaCodec::new());

        // cid → (api_key, version) 映射，供 up→down 改写用。
        // 用 DashMap 替代 Mutex<HashMap>(见 review D2/B4)：up→down 每帧 remove
        // 不存在时是廉价 no-op，无需异步锁。
        let pending: Arc<dashmap::DashMap<i32, (i16, i16)>> = Arc::new(dashmap::DashMap::new());
        /// per_connection 模式 pending 映射上限，防止恶意客户端耗尽内存。
        const MAX_PENDING: usize = 1024;

        // split() 返回 (SplitSink 可写, SplitStream 可读)。
        let (mut down_write, mut down_read) = downstream.split();
        let (mut up_write, mut up_read) = upstream.split();

        let idle = self.client_idle_timeout;
        let pending_d2u = pending.clone();
        let metrics_d2u = self.metrics.clone();
        // Flush 定时器：批量 flush 减少 syscall 开销，而非每帧 flush。
        const PER_CONN_FLUSH_MS: u64 = 5;
        let down_to_up = tokio::spawn(async move {
            let mut flush_tick = interval(Duration::from_millis(PER_CONN_FLUSH_MS));
            let mut needs_flush = false;
            loop {
                // select between read + flush tick：写入缓冲后定时 flush。
                let frame = if let Some(d) = idle {
                    tokio::select! {
                        item = tokio::time::timeout(d, down_read.next()) => {
                            match item {
                                Ok(Some(item)) => Some(item),
                                Ok(None) => {
                                    let _ = up_write.flush().await;
                                    let _ = up_write.close().await;
                                    return;
                                }
                                Err(_elapsed) => {
                                    warn!("下游客户端空闲超时，主动关闭连接");
                                    let _ = up_write.flush().await;
                                    let _ = up_write.close().await;
                                    return;
                                }
                            }
                        }
                        _ = flush_tick.tick() => {
                            if needs_flush {
                                if up_write.flush().await.is_err() {
                                    return;
                                }
                                needs_flush = false;
                            }
                            continue;
                        }
                    }
                } else {
                    tokio::select! {
                        item = down_read.next() => {
                            match item {
                                Some(item) => Some(item),
                                None => {
                                    let _ = up_write.flush().await;
                                    let _ = up_write.close().await;
                                    return;
                                }
                            }
                        }
                        _ = flush_tick.tick() => {
                            if needs_flush {
                                if up_write.flush().await.is_err() {
                                    return;
                                }
                                needs_flush = false;
                            }
                            continue;
                        }
                    }
                };

                match frame {
                    Some(Ok(KafkaFrame { data })) => {
                        metrics_d2u.inc_frames_downstream();
                        metrics_d2u.add_bytes_downstream(data.len() as u64);
                        if let Some((api_key, api_version, cid)) =
                            rewrite::parse_request_header(&data)
                            && rewrite::needs_rewrite(api_key) {
                                if pending_d2u.len() >= MAX_PENDING {
                                    warn!("pending 映射达上限 {MAX_PENDING}，拒绝记录新 cid");
                                } else {
                                    pending_d2u.insert(cid, (api_key, api_version));
                                }
                            }
                        // 缓冲写入：仅 send，由定时器负责 flush。
                        if up_write.send(KafkaFrame::new(data)).await.is_err() {
                            return;
                        }
                        needs_flush = true;
                    }
                    Some(Err(e)) => {
                        warn!("下游读错误: {e}");
                        let _ = up_write.flush().await;
                        let _ = up_write.close().await;
                        return;
                    }
                    None => unreachable!(),
                }
            }
        });

        let pending_u2d = pending.clone();
        let rewrite_map = self.rewrite_map.clone();
        let metrics_u2d = self.metrics.clone();
        // up→down 也用定时批量 flush，减少 syscall。
        let up_to_down = tokio::spawn(async move {
            let mut flush_tick = interval(Duration::from_millis(PER_CONN_FLUSH_MS));
            let mut needs_flush = false;
            loop {
                tokio::select! {
                    frame = up_read.next() => {
                        match frame {
                            Some(Ok(KafkaFrame { data })) => {
                                metrics_u2d.inc_frames_upstream();
                                metrics_u2d.add_bytes_upstream(data.len() as u64);
                                let cid = rewrite::parse_response_cid(&data);
                                let entry = match cid {
                                    Some(c) => pending_u2d.remove(&c).map(|(_, v)| v),
                                    None => None,
                                };
                                let out = match entry {
                                    Some((api_key, version)) => {

                                        rewrite::rewrite_response(api_key, version, &data, &rewrite_map)
                                            .map(KafkaFrame::new)
                                            .unwrap_or_else(|| KafkaFrame::new(data))
                                    }
                                    None => KafkaFrame::new(data),
                                };
                                if down_write.send(out).await.is_err() {
                                    return;
                                }
                                needs_flush = true;
                            }
                            _ => {
                                let _ = down_write.flush().await;
                                let _ = down_write.close().await;
                                return;
                            }
                        }
                    }
                    _ = flush_tick.tick() => {
                        if needs_flush {
                            if down_write.flush().await.is_err() {
                                return;
                            }
                            needs_flush = false;
                        }
                    }
                }
            }
        });

        // 确保两个 spawn 的 task 都完成后再返回，这样 UpstreamGuard 的 drop 时机正确。
        let _ = tokio::try_join!(down_to_up, up_to_down);
        Ok(())
    }
}

/// pooled 模式 relay：一条下游连接经 BrokerPool 多路复用上游连接。
///
/// 流程：
/// - 读下游请求帧 → 解析 (api_key, version, cid_d) → 从 pool 借上游连接 →
///   分配 cid_u、改写帧头 cid_d→cid_u、写上游(经 pool 的 cid_remap 注册回写通道)。
/// - pool 的读循环会把响应经 cid_remap 路由到本 relay 的响应通道，
///   本 relay 读响应通道 → 写下游。
pub struct PooledRelay {
    downstream: TcpStream,
    pool: Arc<BrokerPool>,
    metrics: SharedMetrics,
    client_idle_timeout: Option<Duration>,
}

impl PooledRelay {
    pub fn new(downstream: TcpStream, pool: Arc<BrokerPool>, metrics: SharedMetrics) -> Self {
        Self {
            downstream,
            pool,
            metrics,
            client_idle_timeout: None,
        }
    }

    /// 设置客户端空闲超时。
    pub fn with_client_idle_timeout(mut self, t: Option<Duration>) -> Self {
        self.client_idle_timeout = t;
        self
    }

    /// 运行多路复用转发循环。
    pub async fn run(self) {
        if let Err(e) = self.run_inner().await {
            warn!("pooled relay 结束: {e}");
        } else {
            debug!("pooled relay 正常结束");
        }
    }

    async fn run_inner(self) -> Result<(), RelayError> {
        let downstream = Framed::new(self.downstream, KafkaCodec::new());
        let (mut down_write, mut down_read) = downstream.split();

        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<DownstreamResp>(256);

        // 任务1：读下游请求帧 → 借上游连接 → 改写 cid → 写上游。
        //
        // 租约模式 不再每帧 acquire+release，而是持有一条上游连接
        // 在租约窗口内复用，窗口结束或下游 EOF 才 release。大幅减少锁竞争。
        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let resp_tx1 = resp_tx.clone();
        let idle = self.client_idle_timeout;
        let down_to_up = tokio::spawn(async move {
            /// 租约时长：复用上游连接的窗口，到期则归还并重新借用。
            const LEASE_DURATION: Duration = Duration::from_millis(200);
            let mut lease: Option<crate::pool::Acquired> = None;
            let mut lease_deadline = std::time::Instant::now();

            loop {
                let frame = if let Some(d) = idle {
                    match tokio::time::timeout(d, down_read.next()).await {
                        Ok(Some(item)) => item,
                        Ok(None) => {
                            // 下游 EOF：归还当前租约。
                            if let Some(a) = lease.take() {
                                pool.release(a.conn_id, a.write_tx).await;
                            }
                            return;
                        }
                        Err(_elapsed) => {
                            warn!("下游客户端空闲超时(pooled)，主动关闭连接");
                            if let Some(a) = lease.take() {
                                pool.release(a.conn_id, a.write_tx).await;
                            }
                            return;
                        }
                    }
                } else {
                    match down_read.next().await {
                        Some(item) => item,
                        None => {
                            if let Some(a) = lease.take() {
                                pool.release(a.conn_id, a.write_tx).await;
                            }
                            return;
                        }
                    }
                };

                match frame {
                    Ok(KafkaFrame { data }) => {
                        metrics.inc_frames_downstream();
                        metrics.add_bytes_downstream(data.len() as u64);

                        let (api_key, api_version, cid_d) =
                            match rewrite::parse_request_header(&data) {
                                Some(h) => h,
                                None => continue,
                            };

                        // 租约过期或无租约 → 归还旧连接并重新借用。
                        if lease.is_none() || std::time::Instant::now() >= lease_deadline {
                            // 归还过期连接(若有)。
                            if let Some(old) = lease.take() {
                                pool.release(old.conn_id, old.write_tx).await;
                            }
                            match pool.acquire().await {
                                Ok(a) => {
                                    lease = Some(a);
                                    lease_deadline = std::time::Instant::now() + LEASE_DURATION;
                                }
                                Err(e) => {
                                    warn!("借用上游连接失败: {e}");
                                    return;
                                }
                            }
                        }
                        let acquired = lease.as_ref().unwrap();

                        let cid_u = match pool.register_request(
                            resp_tx1.clone(),
                            cid_d,
                            api_key,
                            api_version,
                            acquired.conn_id,
                        ) {
                            Some(u) => u,
                            None => {
                                warn!("背压：在途请求达上限，关闭下游连接让客户端重试");
                                if let Some(a) = lease.take() {
                                    pool.release(a.conn_id, a.write_tx).await;
                                }
                                return;
                            }
                        };
                        metrics.inc_requests_in_flight();

                        let mut buf = BytesMut::with_capacity(data.len());
                        buf.put_slice(&data[..4]);
                        buf.put_i32(cid_u);
                        buf.put_slice(&data[8..]);

                        if acquired
                            .write_tx
                            .send(WriteReq {
                                frame: KafkaFrame::new(buf.freeze()),
                                cid_u,
                            })
                            .await
                            .is_err()
                        {
                            // 上游连接已死：清理在途映射，丢弃租约(连接已不可用)。
                            pool.cancel_request(cid_u);
                            lease.take();
                            return;
                        }
                        // 租约模式下不立即 release，继续复用直到窗口到期。
                    }
                    Err(e) => {
                        warn!("下游读错误(pooled): {e}");
                        if let Some(a) = lease.take() {
                            pool.release(a.conn_id, a.write_tx).await;
                        }
                        return;
                    }
                }
            }
        });

        // 任务2：读响应通道 → 写下游（批量 flush，减少 syscall）。
        let flush_interval_ms: u64 = 5;
        let up_to_down = tokio::spawn(async move {
            let mut flush_tick = interval(Duration::from_millis(flush_interval_ms));
            let mut needs_flush = false;
            loop {
                tokio::select! {
                    resp = resp_rx.recv() => {
                        match resp {
                            Some(r) => {
                                if down_write.send(r.frame).await.is_err() {
                                    return;
                                }
                                needs_flush = true;
                            }
                            None => {
                                let _ = down_write.flush().await;
                                return;
                            }
                        }
                    }
                    _ = flush_tick.tick() => {
                        if needs_flush {
                            if down_write.flush().await.is_err() {
                                return;
                            }
                            needs_flush = false;
                        }
                    }
                }
            }
        });

        tokio::select! {
            _ = down_to_up => {},
            _ = up_to_down => {},
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("上游连接失败: {0}")]
    Upstream(#[from] UpstreamError),
}
