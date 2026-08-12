//! 性能与内存指标采集 + Prometheus 导出。
//!
//! 指标分四组：吞吐、延迟、连接/池、内存。通过 `/metrics` 端点暴露
//! Prometheus 文本格式。
//!
//! 设计上使用 `std::sync::atomic` 计数器(Gauge/Counter)，无锁、低开销，
//! 适合 512MB 低内存场景。直方图用一组固定桶累计计数，导出时计算分位。
//!
//! 通过 `[api].metrics_enabled` 控制是否采集，默认关闭以减少性能开销。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// 全局指标集：所有 acceptor / relay / pool 共享一个实例。
#[derive(Debug)]
pub struct Metrics {
    enabled: AtomicBool,

    frames_downstream: AtomicU64,
    frames_upstream: AtomicU64,
    bytes_downstream: AtomicU64,
    bytes_upstream: AtomicU64,
    requests_in_flight: AtomicI64,
    downstream_connections: AtomicI64,
    upstream_connections: AtomicI64,
    pool_hits: AtomicU64,
    pool_misses: AtomicU64,
    pool_evictions: AtomicU64,
    pool_waiters: AtomicI64,
    auth_failures: AtomicU64,
    pub acquire_hist: Histogram,
    pub request_hist: Histogram,
    pub handshake_hist: Histogram,
    cid_map_entries: AtomicI64,
}

/// 固定桶直方图：累计落入各延迟桶的计数。
#[derive(Debug)]
pub struct Histogram {
    buckets: &'static [f64],
    counts: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub const DEFAULT_BUCKETS: &'static [f64] =
        &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

    pub fn new() -> Self {
        Self::with_buckets(Self::DEFAULT_BUCKETS)
    }

    pub fn with_buckets(buckets: &'static [f64]) -> Self {
        let counts = (0..buckets.len() + 1).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            counts,
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, secs: f64) {
        let micros = (secs * 1_000_000.0) as u64;
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // 用二分搜索定位桶索引 (O(log n) 替代 O(n) 线性搜索)
        let idx = self
            .buckets
            .binary_search_by(|&b| b.partial_cmp(&secs).unwrap_or(std::cmp::Ordering::Less))
            .unwrap_or_else(|i| i);
        for c in &self.counts[idx..] {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn render(&self, name: &str, out: &mut String) {
        for (i, &b) in self.buckets.iter().enumerate() {
            let le = format_bucket(b);
            let v = self.counts[i].load(Ordering::Relaxed);
            out.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {v}\n"));
        }
        let inf = self.counts[self.buckets.len()].load(Ordering::Relaxed);
        out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {inf}\n"));
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let count = self.count.load(Ordering::Relaxed);
        out.push_str(&format!("{name}_sum {sum}\n"));
        out.push_str(&format!("{name}_count {count}\n"));
    }
}

/// 格式化 Prometheus bucket 上界标签，保证小数位一致(见 review D7)。
/// 例：1.0 输出 "1.0" 而非 "1"，与 Prometheus client 库惯例一致。
fn format_bucket(b: f64) -> String {
    if b == b.trunc() {
        format!("{b:.1}")
    } else {
        format!("{b}")
    }
}

impl Metrics {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            frames_downstream: AtomicU64::new(0),
            frames_upstream: AtomicU64::new(0),
            bytes_downstream: AtomicU64::new(0),
            bytes_upstream: AtomicU64::new(0),
            requests_in_flight: AtomicI64::new(0),
            downstream_connections: AtomicI64::new(0),
            upstream_connections: AtomicI64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_misses: AtomicU64::new(0),
            pool_evictions: AtomicU64::new(0),
            pool_waiters: AtomicI64::new(0),
            auth_failures: AtomicU64::new(0),
            acquire_hist: Histogram::new(),
            request_hist: Histogram::new(),
            handshake_hist: Histogram::new(),
            cid_map_entries: AtomicI64::new(0),
        }
    }

    #[inline]
    fn on(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    // ---- 吞吐计数 ----
    pub fn inc_frames_downstream(&self) {
        if self.on() {
            self.frames_downstream.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn add_bytes_downstream(&self, n: u64) {
        if self.on() {
            self.bytes_downstream.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn inc_frames_upstream(&self) {
        if self.on() {
            self.frames_upstream.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn add_bytes_upstream(&self, n: u64) {
        if self.on() {
            self.bytes_upstream.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn inc_requests_in_flight(&self) {
        if self.on() {
            self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn dec_requests_in_flight(&self) {
        if self.on() {
            self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }

    // ---- 连接(始终计数：连接管理/安全限制需要，非纯性能数据) ----
    pub fn inc_downstream_connections(&self) {
        self.downstream_connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_downstream_connections(&self) {
        self.downstream_connections.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn downstream_connection_count(&self) -> i64 {
        self.downstream_connections.load(Ordering::Relaxed)
    }
    pub fn inc_upstream_connections(&self) {
        self.upstream_connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_upstream_connections(&self) {
        self.upstream_connections.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn inc_auth_failures(&self) {
        if self.on() {
            self.auth_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ---- 池 ----
    pub fn inc_pool_hits(&self) {
        if self.on() {
            self.pool_hits.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn inc_pool_misses(&self) {
        if self.on() {
            self.pool_misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn inc_pool_evictions(&self) {
        if self.on() {
            self.pool_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn inc_pool_waiters(&self) {
        if self.on() {
            self.pool_waiters.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn dec_pool_waiters(&self) {
        if self.on() {
            self.pool_waiters.fetch_sub(1, Ordering::Relaxed);
        }
    }

    // ---- cid 映射 ----
    pub fn inc_cid_map_entries(&self) {
        if self.on() {
            self.cid_map_entries.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn dec_cid_map_entries(&self) {
        if self.on() {
            self.cid_map_entries.fetch_sub(1, Ordering::Relaxed);
        }
    }
    pub fn sub_cid_map_entries(&self, n: i64) {
        if self.on() {
            self.cid_map_entries.fetch_sub(n, Ordering::Relaxed);
        }
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        out.push_str("# HELP kafka_proxy_frames_total Frames forwarded by direction.\n");
        out.push_str("# TYPE kafka_proxy_frames_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_frames_total{{dir=\"downstream\"}} {}\n",
            self.frames_downstream.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "kafka_proxy_frames_total{{dir=\"upstream\"}} {}\n",
            self.frames_upstream.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_bytes_total Bytes forwarded by direction.\n");
        out.push_str("# TYPE kafka_proxy_bytes_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_bytes_total{{dir=\"downstream\"}} {}\n",
            self.bytes_downstream.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "kafka_proxy_bytes_total{{dir=\"upstream\"}} {}\n",
            self.bytes_upstream.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_requests_in_flight In-flight requests.\n");
        out.push_str("# TYPE kafka_proxy_requests_in_flight gauge\n");
        out.push_str(&format!(
            "kafka_proxy_requests_in_flight {}\n",
            self.requests_in_flight.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_downstream_connections Active downstream connections.\n");
        out.push_str("# TYPE kafka_proxy_downstream_connections gauge\n");
        out.push_str(&format!(
            "kafka_proxy_downstream_connections {}\n",
            self.downstream_connections.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP kafka_proxy_upstream_connections Active upstream pooled connections.\n",
        );
        out.push_str("# TYPE kafka_proxy_upstream_connections gauge\n");
        out.push_str(&format!(
            "kafka_proxy_upstream_connections {}\n",
            self.upstream_connections.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_pool_hits_total Pool acquire hits.\n");
        out.push_str("# TYPE kafka_proxy_pool_hits_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_pool_hits_total {}\n",
            self.pool_hits.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_pool_misses_total Pool acquire misses.\n");
        out.push_str("# TYPE kafka_proxy_pool_misses_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_pool_misses_total {}\n",
            self.pool_misses.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_pool_evictions_total Pool connection evictions.\n");
        out.push_str("# TYPE kafka_proxy_pool_evictions_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_pool_evictions_total {}\n",
            self.pool_evictions.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_pool_waiters Requests waiting for a pooled connection.\n");
        out.push_str("# TYPE kafka_proxy_pool_waiters gauge\n");
        out.push_str(&format!(
            "kafka_proxy_pool_waiters {}\n",
            self.pool_waiters.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_pool_hit_rate Derived pool hit rate.\n");
        out.push_str("# TYPE kafka_proxy_pool_hit_rate gauge\n");
        let h = self.pool_hits.load(Ordering::Relaxed) as f64;
        let m = self.pool_misses.load(Ordering::Relaxed) as f64;
        let rate = if h + m > 0.0 { h / (h + m) } else { 0.0 };
        out.push_str(&format!("kafka_proxy_pool_hit_rate {rate}\n"));

        out.push_str("# HELP kafka_proxy_auth_failures_total Upstream authentication failures.\n");
        out.push_str("# TYPE kafka_proxy_auth_failures_total counter\n");
        out.push_str(&format!(
            "kafka_proxy_auth_failures_total {}\n",
            self.auth_failures.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP kafka_proxy_acquire_duration_seconds Time waiting to acquire an upstream connection.\n");
        out.push_str("# TYPE kafka_proxy_acquire_duration_seconds histogram\n");
        self.acquire_hist
            .render("kafka_proxy_acquire_duration_seconds", &mut out);

        out.push_str("# HELP kafka_proxy_request_duration_seconds End-to-end request latency.\n");
        out.push_str("# TYPE kafka_proxy_request_duration_seconds histogram\n");
        self.request_hist
            .render("kafka_proxy_request_duration_seconds", &mut out);

        out.push_str("# HELP kafka_proxy_handshake_duration_seconds Upstream GSSAPI/TLS handshake duration.\n");
        out.push_str("# TYPE kafka_proxy_handshake_duration_seconds histogram\n");
        self.handshake_hist
            .render("kafka_proxy_handshake_duration_seconds", &mut out);

        out.push_str("# HELP kafka_proxy_process_resident_memory_bytes Process RSS bytes.\n");
        out.push_str("# TYPE kafka_proxy_process_resident_memory_bytes gauge\n");
        out.push_str(&format!(
            "kafka_proxy_process_resident_memory_bytes {}\n",
            process_rss_bytes()
        ));

        out.push_str("# HELP kafka_proxy_cid_map_entries correlation_id remap table entries.\n");
        out.push_str("# TYPE kafka_proxy_cid_map_entries gauge\n");
        out.push_str(&format!(
            "kafka_proxy_cid_map_entries {}\n",
            self.cid_map_entries.load(Ordering::Relaxed)
        ));

        out
    }
}

pub fn process_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = std::fs::read_to_string("/proc/self/statm") {
            let fields: Vec<&str> = text.split_whitespace().collect();
            if fields.len() >= 2 {
                if let Ok(pages) = fields[1].parse::<u64>() {
                    return pages * page_size_kb() * 1024;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(target_os = "linux")]
fn page_size_kb() -> u64 {
    use std::sync::OnceLock;
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::process::Command::new("getconf")
            .arg("PAGESIZE")
            .output()
            .ok()
            .and_then(|o| {
                std::str::from_utf8(&o.stdout)
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .map(|b| b / 1024)
            .unwrap_or(4)
    })
}

pub type SharedMetrics = Arc<Metrics>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_observe_and_cumulative() {
        let h = Histogram::new();
        h.observe(0.00005);
        h.observe(0.002);
        h.observe(10.0);
        assert_eq!(h.count.load(Ordering::Relaxed), 3);
        assert_eq!(h.counts[0].load(Ordering::Relaxed), 1);
        let idx_005 = h.buckets.iter().position(|&b| b == 0.005).unwrap();
        assert_eq!(h.counts[idx_005].load(Ordering::Relaxed), 2);
        assert_eq!(h.counts[h.buckets.len()].load(Ordering::Relaxed), 3);
    }

    #[test]
    fn metrics_render_contains_key_lines() {
        let m = Metrics::new(true);
        m.inc_frames_downstream();
        m.inc_downstream_connections();
        let text = m.render_prometheus();
        assert!(text.contains("kafka_proxy_frames_total{dir=\"downstream\"}"));
        assert!(text.contains("kafka_proxy_downstream_connections"));
        assert!(text.contains("kafka_proxy_process_resident_memory_bytes"));
    }

    #[test]
    fn metrics_disabled_skips_collection() {
        let m = Metrics::new(false);
        m.inc_frames_downstream();
        m.inc_pool_hits();
        m.inc_cid_map_entries();
        m.inc_downstream_connections();
        let text = m.render_prometheus();
        assert!(text.contains("kafka_proxy_frames_total{dir=\"downstream\"} 0"));
        assert!(text.contains("kafka_proxy_cid_map_entries 0"));
        assert!(text.contains("kafka_proxy_pool_hits_total 0"));
        assert!(text.contains("kafka_proxy_downstream_connections 1"));
    }
}
