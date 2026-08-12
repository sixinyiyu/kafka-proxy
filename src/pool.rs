//! 上游连接池 + correlation_id 重映射（pooled 模式）— 见设计文档 §3.6。
//!
//! 端口-per-broker + 1:1 流转发最简单，但每条下游连接建一条上游已认证连接浪费
//! 握手(GSSAPI/TLS)。连接池让多条下游连接复用少量上游已认证连接，按帧调度 +
//! correlation_id 重映射，把昂贵的握手摊薄。
//!
//! ## 模型
//! - 每 broker(node_id) 一个 [`BrokerPool`]，池内若干条已认证上游连接。
//! - 下游请求帧经 [`CidRemap`] 分配上游 cid_u，改写帧头 cid_d→cid_u 写上游；
//!   上游响应按 cid_u 查表还原 (downstream_tx, cid_d)，改写回写下游。
//! - 映射随响应返回即删除(请求-响应配对释放)。
//!
//! ## 并发安全
//! - 池内空闲连接用 `Mutex<VecDeque<LeasedConn>>` 保护；借用走 async notify 等待。
//! - cid 表用 `DashMap<cid_u, RouteEntry>`(分片无锁，见 .clinerules：并发场景优先 dashmap)。

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use kafka_client::wire::KafkaFrame;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::{interval, timeout};
use tracing::{debug, warn};

use crate::metrics::SharedMetrics;
use crate::rewrite;
use crate::rewrite::RewriteMap;
use crate::upstream::{UpstreamAuth, UpstreamError, UpstreamFramed};

/// Flush 间隔：写端累积帧后定时 flush，而非每帧 flush，减少 syscall。
const FLUSH_INTERVAL_MS: u64 = 5;

/// 一条上游已认证连接的封装：带 split 后的读写半(用 reactor 模式)。
///
/// 这里采用「每条上游连接一个写任务 + 一个读任务」：写任务接收 (帧, cid_u)，
/// 读任务把响应按 cid_u 派发回 cid 表。这样一条上游连接可多路复用多个下游请求。
pub struct PooledConn {
    /// 写入帧的通道：发送 (改写后的帧, cid_u)。
    pub write_tx: mpsc::Sender<WriteReq>,
    /// 上游连接是否存活。
    pub alive: bool,
    /// 最后使用时间(LRU)。
    pub last_used: Instant,
    /// 连接唯一 id(故障时按连接清理在途请求)。
    pub conn_id: ConnId,
}

/// 写请求：要写入上游的帧 + 其上游 cid(用于读循环失败时清理在途)。
pub struct WriteReq {
    pub frame: KafkaFrame,
    pub cid_u: i32,
}

/// 上游连接标识：每条上游连接分配一个唯一 id，用于故障时按连接清理在途请求。
pub type ConnId = u64;

/// cid 重映射表条目：上游 cid_u → 下游回写通道 + 原下游 cid + 改写信息。
struct RouteEntry {
    /// 下游回写通道(把改写后的响应帧发回对应下游连接)。
    downstream_tx: mpsc::Sender<DownstreamResp>,
    /// 原下游 correlation_id(响应需改回此值)。
    cid_d: i32,
    /// 请求的 api_key/version(决定响应是否需改写)。
    api_key: i16,
    api_version: i16,
    /// 请求发出时刻(用于端到端延迟)。
    started: Instant,
    /// 所属上游连接 id(故障时按连接精确清理，避免误删其他连接在途请求)。
    conn_id: ConnId,
}

/// 发回下游的响应(已改写 cid + 元数据)。
pub struct DownstreamResp {
    pub frame: KafkaFrame,
}

/// cid 重映射表：cid_u → RouteEntry。所有上游连接共享一份(因为 cid_u 全局唯一)。
///
/// 使用 `DashMap` 替代 `Mutex<HashMap>`：insert/take 为单 key 操作，分片后基本无锁，
/// 消除全局 Mutex 序列化点(见 review D1/B1)。fail_conn 用 DashMap::retain 按 conn_id 过滤。
#[derive(Default)]
pub struct CidRemap {
    map: DashMap<i32, RouteEntry>,
    next_cid: std::sync::atomic::AtomicI32,
    next_conn_id: std::sync::atomic::AtomicU64,
    max_in_flight: usize,
}

impl CidRemap {
    pub fn new() -> Self {
        Self::with_max_in_flight(100_000)
    }

    pub fn with_max_in_flight(max_in_flight: usize) -> Self {
        Self {
            map: DashMap::new(),
            next_cid: std::sync::atomic::AtomicI32::new(1),
            next_conn_id: std::sync::atomic::AtomicU64::new(1),
            max_in_flight,
        }
    }

    /// 分配一个全局唯一上游 cid_u。使用 CAS 循环，溢出时回绕到 1 避免冲突。
    fn alloc_cid_u(&self) -> i32 {
        use std::sync::atomic::Ordering;
        loop {
            let prev = self.next_cid.load(Ordering::SeqCst);
            // 如果当前值 <=0 或已到 i32::MAX，回绕到 1。
            let next = if prev <= 0 || prev == i32::MAX {
                1
            } else {
                prev + 1
            };
            if self
                .next_cid
                .compare_exchange(prev, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return next;
            }
            // CAS 失败(并发竞争)，重试。
        }
    }

    /// 分配一个全局唯一上游连接 id。
    pub fn alloc_conn_id(&self) -> ConnId {
        self.next_conn_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// 记录一条在途请求映射。返回分配的 cid_u。
    ///
    /// 若在途数已达 `max_in_flight` 上限，返回 `None`(背压)，调用方应拒绝该请求。
    fn insert(
        &self,
        downstream_tx: mpsc::Sender<DownstreamResp>,
        cid_d: i32,
        api_key: i16,
        api_version: i16,
        conn_id: ConnId,
        metrics: &SharedMetrics,
    ) -> Option<i32> {
        // DashMap::len() 是各分片原子求和，无需加锁。
        if self.map.len() >= self.max_in_flight {
            // 背压：cid 表达上限，拒绝新请求。
            return None;
        }
        let cid_u = self.alloc_cid_u();
        let entry = RouteEntry {
            downstream_tx,
            cid_d,
            api_key,
            api_version,
            started: Instant::now(),
            conn_id,
        };
        self.map.insert(cid_u, entry);
        metrics.inc_cid_map_entries();
        Some(cid_u)
    }

    /// 上游响应到达：查 cid_u 还原，返回 (下游通道, cid_d, api_key, version, 端到端延迟)。
    fn take(&self, cid_u: i32, metrics: &SharedMetrics) -> Option<RouteEntry> {
        let entry = self.map.remove(&cid_u).map(|(_, v)| v);
        if entry.is_some() {
            metrics.dec_cid_map_entries();
        }
        entry
    }

    /// 当前在途(已注册未响应)映射数。
    pub fn in_flight(&self) -> usize {
        self.map.len()
    }

    /// 清理某条上游连接的所有在途请求(连接故障时调用)。
    /// 只清理属于该 conn_id 的条目，不影响其他连接。
    fn fail_conn(&self, conn_id: ConnId, metrics: &SharedMetrics) -> usize {
        let before = self.map.len();
        self.map.retain(|_, entry| entry.conn_id != conn_id);
        let removed = before - self.map.len();
        metrics.sub_cid_map_entries(removed as i64);
        removed
    }
}

/// 单 broker 连接池。
pub struct BrokerPool {
    pub node_id: i32,
    pub real_addr: SocketAddr,
    pub real_host: String,
    auth: UpstreamAuth,
    config: PoolLimits,
    idle: Mutex<VecDeque<PooledConn>>,
    /// 当前已建连接数(含在用 + 空闲)。
    count: Mutex<usize>,
    /// 池满时的等待者通知。
    notify: Notify,
    pub cid_remap: Arc<CidRemap>,
    pub rewrite_map: Arc<RewriteMap>,
    pub metrics: SharedMetrics,
}

/// 池策略参数(从配置映射)。
#[derive(Clone, Copy, Debug)]
pub struct PoolLimits {
    pub max_per_broker: usize,
    pub min_idle: usize,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
    pub max_in_flight: usize,
}

impl BrokerPool {
    pub fn new(
        node_id: i32,
        real_addr: SocketAddr,
        real_host: String,
        auth: UpstreamAuth,
        config: PoolLimits,
        cid_remap: Arc<CidRemap>,
        rewrite_map: Arc<RewriteMap>,
        metrics: SharedMetrics,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            real_addr,
            real_host,
            auth,
            config,
            idle: Mutex::new(VecDeque::new()),
            count: Mutex::new(0),
            notify: Notify::new(),
            cid_remap,
            rewrite_map,
            metrics,
        })
    }

    /// 借用一条上游连接(用于发送一个请求)。返回写通道 + 是否为新建(miss)。
    ///
    /// 调用方拿到 write_tx 后，分配 cid_u、改写帧头、写上游；响应由读循环经
    /// cid_remap 自动派发回下游。连接用完无需显式归还(读循环常驻，写通道发完即止)。
    pub async fn acquire(self: &Arc<Self>) -> Result<Acquired, PoolError> {
        let started = Instant::now();
        loop {
            // 1. 尝试取空闲。
            {
                let mut idle = self.idle.lock().await;
                if let Some(conn) = idle.pop_front() {
                    if conn.alive {
                        self.metrics.inc_pool_hits();
                        let dur = started.elapsed().as_secs_f64();
                        self.metrics.acquire_hist.observe(dur);
                        return Ok(Acquired {
                            write_tx: conn.write_tx.clone(),
                            conn_id: conn.conn_id,
                        });
                    }
                }
            }

            // 2. 尝试新建(未达上限)。
            let can_create = {
                let mut c = self.count.lock().await;
                if *c < self.config.max_per_broker {
                    *c += 1;
                    true
                } else {
                    false
                }
            };
            if can_create {
                self.metrics.inc_pool_misses();
                let dur = started.elapsed().as_secs_f64();
                self.metrics.acquire_hist.observe(dur);
                // 新建连接(握手)；失败则回退计数并重试/等待。
                match self.create_conn().await {
                    Ok((write_tx, conn_id)) => {
                        self.metrics.inc_upstream_connections();
                        return Ok(Acquired { write_tx, conn_id });
                    }
                    Err(e) => {
                        // 回退计数，记录认证失败。
                        let mut c = self.count.lock().await;
                        *c -= 1;
                        self.metrics.inc_auth_failures();
                        return Err(e);
                    }
                }
            }

            // 3. 池满：排队等待。
            self.metrics.inc_pool_waiters();
            let wait_result = timeout(self.config.acquire_timeout, self.notify.notified()).await;
            self.metrics.dec_pool_waiters();
            if wait_result.is_err() {
                return Err(PoolError::AcquireTimeout);
            }
            // 被唤醒后重试。
            let dur = started.elapsed().as_secs_f64();
            self.metrics.acquire_hist.observe(dur);
        }
    }

    /// 建立一条新的已认证上游连接，启动其读循环，把写半注册为可用。
    /// 返回 (写通道, 连接 id)。
    async fn create_conn(&self) -> Result<(mpsc::Sender<WriteReq>, ConnId), PoolError> {
        let t0 = Instant::now();
        let framed = self
            .auth
            .connect_framed(self.real_addr, &self.real_host)
            .await?;
        let dur = t0.elapsed().as_secs_f64();
        self.metrics.handshake_hist.observe(dur);

        let (write_tx, write_rx) = mpsc::channel::<WriteReq>(256);
        let (read_tx, read_rx) = mpsc::channel::<Bytes>(64);

        // 为此连接分配唯一 conn_id(用于故障时按连接精确清理在途请求)。
        let conn_id = self.cid_remap.alloc_conn_id();

        // 启动写任务：从 write_rx 取帧写上游。
        let cid_remap_w = self.cid_remap.clone();
        let metrics_w = self.metrics.clone();
        tokio::spawn(write_loop(
            framed,
            write_rx,
            read_tx,
            cid_remap_w,
            metrics_w,
            conn_id,
        ));

        // 启动读任务：读上游响应 → 查 cid_remap → 改写 → 发回下游。
        let cid_remap_r = self.cid_remap.clone();
        let rewrite_map = self.rewrite_map.clone();
        let metrics_r = self.metrics.clone();
        tokio::spawn(read_loop(read_rx, cid_remap_r, rewrite_map, metrics_r));

        Ok((write_tx, conn_id))
    }

    /// 当前空闲连接数。
    pub async fn idle_count(&self) -> usize {
        self.idle.lock().await.len()
    }

    /// 预热：按 `min_idle` 配置预建空闲连接，消除冷启动握手延迟。
    /// 预建失败仅 warn 不中止（pool 会在 acquire 时按需创建）。
    pub async fn warmup(self: &Arc<Self>) {
        let target = self.config.min_idle;
        if target == 0 {
            return;
        }
        debug!(min_idle = target, node_id = self.node_id, "预热上游连接池");
        for _ in 0..target {
            match self.create_conn().await {
                Ok((write_tx, conn_id)) => {
                    self.metrics.inc_upstream_connections();
                    let conn = PooledConn {
                        write_tx,
                        alive: true,
                        last_used: Instant::now(),
                        conn_id,
                    };
                    self.idle.lock().await.push_back(conn);
                }
                Err(e) => {
                    warn!(
                        ?e,
                        node_id = self.node_id,
                        "预热连接失败，将在 acquire 时重试"
                    );
                    break;
                }
            }
        }
    }

    /// 启动后台任务：定时回收空闲超时连接 + 清理不健康连接。
    /// 需在 pool 创建后 spawn。
    pub fn start_reaper(self: &Arc<Self>) {
        let pool = self.clone();
        let idle_timeout = self.config.idle_timeout;
        tokio::spawn(async move {
            // 每 10s 扫描一次：回收超过 idle_timeout 的空闲连接 + 不健康连接。
            let mut tick = interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let mut idle = pool.idle.lock().await;
                let before = idle.len();
                let now = Instant::now();
                // retain 闭包不能 async，先标记待移除的连接。
                idle.retain(|conn| {
                    let expired = now.duration_since(conn.last_used) >= idle_timeout;
                    if expired || !conn.alive {
                        if expired {
                            debug!(conn_id = conn.conn_id, "回收空闲超时上游连接");
                        } else {
                            debug!(conn_id = conn.conn_id, "回收不健康上游连接");
                        }
                        return false;
                    }
                    true
                });
                let removed = before - idle.len();
                // 批量更新计数器（不再需要 await，因为 retain 是同步的）。
                for _ in 0..removed {
                    pool.metrics.inc_pool_evictions();
                    pool.metrics.dec_upstream_connections();
                }
                if removed > 0 {
                    let mut c = pool.count.lock().await;
                    *c = c.saturating_sub(removed);
                    // 唤醒等待者尝试新建连接（count 已减）。
                    drop(c);
                    pool.notify.notify_one();
                }
            }
        });
    }

    /// 归还一条借用完毕的上游连接到空闲池，供后续复用(省握手)。
    ///
    /// 调用方在 `acquire` 并发送完一个请求帧后调用此方法。连接的写通道
    /// (write_tx) 可被多次 clone，所以归还只需把一份 clone 放回 idle。
    /// 若空闲池已满或连接已死，直接丢弃(读循环会自然关闭)。
    pub async fn release(self: &Arc<Self>, conn_id: ConnId, write_tx: mpsc::Sender<WriteReq>) {
        let conn = PooledConn {
            write_tx,
            alive: true,
            last_used: Instant::now(),
            conn_id,
        };
        let mut idle = self.idle.lock().await;
        // 空闲池容量不超过 max_per_broker，避免无限堆积。
        if idle.len() < self.config.max_per_broker {
            idle.push_back(conn);
        }
        // 通知一个等待者(若有)可复用此连接。
        drop(idle);
        self.notify.notify_one();
    }

    /// 为一个下游请求注册 cid 重映射，返回分配的上游 cid_u。
    ///
    /// pool 的读循环收到上游响应后，会按 cid_u 查表，把改写后的响应经
    /// `downstream_tx` 发回调用方(PooledRelay)。
    ///
    /// `conn_id` 标识该请求走哪条上游连接，连接故障时按 conn_id 精确清理。
    /// 若在途数达 `max_in_flight` 上限，返回 `None`(背压)。
    pub fn register_request(
        &self,
        downstream_tx: mpsc::Sender<DownstreamResp>,
        cid_d: i32,
        api_key: i16,
        api_version: i16,
        conn_id: ConnId,
    ) -> Option<i32> {
        self.cid_remap.insert(
            downstream_tx,
            cid_d,
            api_key,
            api_version,
            conn_id,
            &self.metrics,
        )
    }
    /// 取消一条已注册的 cid 重映射（上游连接故障时由 PooledRelay 调用）。
    pub fn cancel_request(&self, cid_u: i32) {
        if self.cid_remap.take(cid_u, &self.metrics).is_some() {
            self.metrics.dec_requests_in_flight();
        }
    }
}

/// 借用结果：一个可写入上游的通道 + 该连接的唯一 id。
pub struct Acquired {
    pub write_tx: mpsc::Sender<WriteReq>,
    /// 上游连接 id(注册请求时需带上，故障时按连接清理在途)。
    pub conn_id: ConnId,
}

/// 写循环：从 write_rx 取 (帧, cid_u) 写上游 framed；上游帧到来时转发给 read_tx。
///
/// 由于 Framed 是「读写一体」，这里需要在一个任务里同时驱动读与写。
/// 我们用 select：写通道有帧就 send，framed 有帧就转发给 read_tx。
async fn write_loop(
    framed: UpstreamFramed,
    mut write_rx: mpsc::Receiver<WriteReq>,
    read_tx: mpsc::Sender<Bytes>,
    cid_remap: Arc<CidRemap>,
    metrics: SharedMetrics,
    conn_id: ConnId,
) {
    let (mut sink, mut stream) = framed.split();
    let mut flush_tick = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    let mut needs_flush = false;
    loop {
        tokio::select! {
            // 写：收到下游请求帧(已改写 cid) → 缓冲写入上游。
            req = write_rx.recv() => {
                match req {
                    Some(WriteReq { frame, .. }) => {
                        if sink.send(frame).await.is_err() {
                            break;
                        }
                        needs_flush = true;
                    }
                    None => {
                        // 调用方丢弃写通道：flush 后关闭写半，退出循环。
                        let _ = sink.flush().await;
                        let _ = sink.close().await;
                        break;
                    }
                }
            }
            // 定时 flush：减少每帧 flush 的 syscall 开销。
            _ = flush_tick.tick() => {
                if needs_flush {
                    if sink.flush().await.is_err() {
                        break;
                    }
                    needs_flush = false;
                }
            }
            // 读：上游响应帧 → 转发给读循环(由 read_loop 处理 cid 路由)。
            frame = stream.next() => {
                match frame {
                    Some(Ok(KafkaFrame { data })) => {
                        metrics.inc_frames_upstream();
                        metrics.add_bytes_upstream(data.len() as u64);
                        if read_tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("上游读错误: {e}");
                        break;
                    }
                    None => {
                        debug!("上游连接 EOF");
                        break;
                    }
                }
            }
        }
    }
    // 连接结束：清理该连接的在途请求(只清本连接，不影响其他连接)。
    let removed = cid_remap.fail_conn(conn_id, &metrics);

    if removed > 0 {
        // 每个被清理的在途请求都需减少 in_flight 计数
        for _ in 0..removed {
            metrics.dec_requests_in_flight();
        }
    }
    metrics.dec_upstream_connections();
    metrics.inc_pool_evictions();
}

/// 读循环：从 write_loop 转发的上游响应 → 查 cid_remap → 改写 → 发回下游。
async fn read_loop(
    mut read_rx: mpsc::Receiver<Bytes>,
    cid_remap: Arc<CidRemap>,
    rewrite_map: Arc<RewriteMap>,
    metrics: SharedMetrics,
) {
    while let Some(data) = read_rx.recv().await {
        // 取 cid_u(响应帧前 4 字节)。
        let cid_u = match rewrite::parse_response_cid(&data) {
            Some(c) => c,
            None => continue,
        };
        let entry = match cid_remap.take(cid_u, &metrics) {
            Some(e) => e,

            None => {
                debug!(cid_u, "无匹配在途请求，丢弃上游响应");
                continue;
            }
        };
        metrics.dec_requests_in_flight();
        let elapsed = entry.started.elapsed().as_secs_f64();
        metrics.request_hist.observe(elapsed);

        // 改写 cid_u → cid_d，并在需要时改写元数据地址。
        let out = route_response(&data, &entry, &rewrite_map);
        // 用 try_send 避免慢下游阻塞 read_loop：若下游消费慢，丢弃响应让客户端重试。
        if entry
            .downstream_tx
            .try_send(DownstreamResp {
                frame: KafkaFrame::new(out),
            })
            .is_err()
        {
            debug!("下游连接背压或已关闭，丢弃响应 cid_u={}", cid_u);
        }
    }
}

/// 把上游响应改写为发给下游的帧：cid_u→cid_d + (若需)元数据地址改写。
///
/// 优化(见 review D4)：rewrite_response 返回的 Bytes 已含 cid_u(若改写)。
/// 无论是否改写，都只需一次分配：新 buf 写入 cid_d + body[4..]。
/// 避免了原先「先 clone 整帧再拷贝」的冗余操作。
fn route_response(data: &Bytes, entry: &RouteEntry, rewrite_map: &RewriteMap) -> Bytes {
    // rewrite_response 期望「含 cid 的完整响应帧」，返回改写后的新帧(含 cid_u)。
    // 若无需改写则返回 None，直接用原 data。
    let base = match rewrite::rewrite_response(entry.api_key, entry.api_version, data, rewrite_map)
    {
        Some(rewritten) => rewritten, // 已含 cid_u 的新帧
        None => data.clone(),         // 无需改写，用原始帧(含 cid_u)
    };

    // 统一替换前 4 字节 cid_u → cid_d。
    let mut buf = BytesMut::with_capacity(base.len());
    buf.put_i32(entry.cid_d);
    buf.put_slice(&base[4..]);
    buf.freeze()
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("上游连接失败: {0}")]
    Upstream(#[from] UpstreamError),
    #[error("借用上游连接超时")]
    AcquireTimeout,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_client::protocol::{Message, Response};

    #[test]
    fn cid_remap_insert_take() {
        let remap = Arc::new(CidRemap::new());
        let metrics = Arc::new(crate::metrics::Metrics::new(true));
        let conn_id = remap.alloc_conn_id();
        let (tx, _rx) = mpsc::channel::<DownstreamResp>(8);
        let cid_u = remap
            .insert(tx, 100, 3, 9, conn_id, &metrics)
            .expect("应插入成功");
        assert_eq!(cid_u, 2); // CAS loop returns next value
        assert_eq!(remap.in_flight(), 1);

        let entry = remap.take(cid_u, &metrics).expect("应取到");
        assert_eq!(entry.cid_d, 100);
        assert_eq!(entry.api_key, 3);
        assert_eq!(entry.conn_id, conn_id);
        assert_eq!(remap.in_flight(), 0);
    }

    #[test]
    fn cid_remap_backpressure() {
        let remap = Arc::new(CidRemap::with_max_in_flight(2));
        let metrics = Arc::new(crate::metrics::Metrics::new(true));
        let conn_id = remap.alloc_conn_id();
        let (tx, _rx) = mpsc::channel::<DownstreamResp>(8);
        assert!(
            remap
                .insert(tx.clone(), 1, 3, 9, conn_id, &metrics)
                .is_some()
        );
        assert!(
            remap
                .insert(tx.clone(), 2, 3, 9, conn_id, &metrics)
                .is_some()
        );
        assert!(remap.insert(tx, 3, 3, 9, conn_id, &metrics).is_none());
        assert_eq!(remap.in_flight(), 2);
    }

    #[test]
    fn cid_remap_fail_conn_isolates() {
        let remap = Arc::new(CidRemap::new());
        let metrics = Arc::new(crate::metrics::Metrics::new(true));
        let (tx, _rx) = mpsc::channel::<DownstreamResp>(8);
        let conn1 = 1u64;
        let conn2 = 2u64;
        let _c1 = remap.insert(tx.clone(), 100, 3, 9, conn1, &metrics);
        let _c2 = remap.insert(tx.clone(), 200, 3, 9, conn2, &metrics);
        assert_eq!(remap.in_flight(), 2);

        let removed = remap.fail_conn(conn1, &metrics);
        assert_eq!(removed, 1);
        assert_eq!(remap.in_flight(), 1);
    }

    #[test]
    fn route_response_rewrites_cid_and_metadata() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i32(555); // cid_u
        rewrite::encode_unsigned_varint(&mut buf, 0);
        let resp = kafka_client::protocol::MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![kafka_client::protocol::MetadataResponseBroker {
                node_id: 1,
                host: "real".to_string(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("c".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: -2147483648,
            error_code: 0,
        };
        resp.flexible_encode(&mut buf, 9).unwrap();
        let data = buf.freeze();

        let mut ports = std::collections::HashMap::new();
        ports.insert(1, ("127.0.0.1".to_string(), 19092));
        let rewrite_map = Arc::new(RewriteMap::new(ports));

        let entry = RouteEntry {
            downstream_tx: mpsc::channel::<DownstreamResp>(1).0,
            cid_d: 42,
            api_key: 3,
            api_version: 9,
            started: Instant::now(),
            conn_id: 1,
        };
        let out = route_response(&data, &entry, &rewrite_map);

        assert_eq!((bytes::Buf::get_i32(&mut &out[..4])), 42);
        let (_h, resp2): (_, kafka_client::protocol::MetadataResponse) =
            kafka_client::protocol::MetadataResponse::decode_frame(out.clone(), 9).unwrap();
        assert_eq!(resp2.brokers[0].host, "127.0.0.1");
        assert_eq!(resp2.brokers[0].port, 19092);
    }
}
