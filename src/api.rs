//! 统一 Web API：health / metrics / debug 路由，复用同一 HTTP 端口。
//!
//! 倾向默认启动 web 端口，health/metrics/debug 统一作为
//! router 添加，移除单独启动 tcp 端口的代码。
//!
//! 端点(均返回 JSON / Prometheus 文本，暂不实现前端页面)：
//! - `GET  /health`                  存活检查(k8s liveness/readiness)
//! - `GET  /metrics`                 Prometheus 文本格式指标(数据来自 metrics.rs)
//! - `GET  /doctor/topics`           列出集群所有 topic(名称/分区数/是否内部)
//! - `POST /doctor/topics`           创建 topic(分区数/副本因子可缺省, 用 broker 默认)
//! - `GET  /doctor/consumers/{topic}` 查询某 topic 的消费者组(成员/分区/lag)
//! - `GET  /doctor/messages/{topic}`  查看某 topic 最新数据(默认前 5 条)
//! - `POST /doctor/messages/{topic}`  给某 topic 发送数据
//! - `GET  /doctor/connections`       当前连接的客户端数量及持续时间

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use kafka_client::{
    AutoOffsetReset, ConsumerConfig, ConsumerRecord, ProducerConfig, ProducerRecord,
    admin::{AdminClient, NewTopic},
};
use serde::Deserialize;

use serde_json::Value;
use tracing::warn;

use crate::config::ApiConfig;
use crate::metrics::SharedMetrics;
use crate::relay::ConnectionTracker;

/// Web API 依赖：已认证的 kafka_client::Client(共享 cluster 内部连接池) + 指标集 +
/// 连接追踪器。
#[derive(Clone)]
pub struct ApiDeps {
    client: Arc<kafka_client::Client>,
    metrics: SharedMetrics,
    conn_tracker: ConnectionTracker,
    default_count: usize,
}

impl ApiDeps {
    pub fn new(
        client: Arc<kafka_client::Client>,
        metrics: SharedMetrics,
        conn_tracker: ConnectionTracker,
        cfg: &ApiConfig,
    ) -> Self {
        Self {
            client,
            metrics,
            conn_tracker,
            default_count: cfg.default_count,
        }
    }
}

/// 启动统一 Web 服务：health + metrics + debug，复用同一端口。返回即长期运行。
pub async fn serve(deps: ApiDeps, cfg: &ApiConfig) -> std::io::Result<()> {
    use axum::extract::DefaultBodyLimit;
    use tokio::net::TcpListener;

    tracing::info!(listen = %cfg.listen, "Web 端点就绪");

    let app = build_router(deps)
        // 请求体大小限制 10MB，防止超大 payload 耗尽内存
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024));
    let listener = TcpListener::bind(&cfg.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// 构造统一路由
pub fn build_router(deps: ApiDeps) -> Router {
    let metrics = deps.metrics.clone();

    Router::new()
        .route("/health", get(health_handler))
        .route(
            "/metrics",
            get(move || {
                let m = metrics.clone();
                async move {
                    let body = m.render_prometheus();
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        body,
                    )
                }
            }),
        )
        .route(
            "/doctor/topics",
            get(list_topics_handler).post(create_topic_handler),
        )
        .route("/doctor/consumers/{{topic}}", get(list_consumers_handler))
        .route(
            "/doctor/messages/{{topic}}",
            get(latest_messages_handler).post(send_message_handler),
        )
        .route("/doctor/connections", get(connections_handler))
        .with_state(deps)
}

// ===========================================================================
// health handler
// ===========================================================================

async fn health_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"status": "ok"})),
    )
}

// ===========================================================================
// /doctor/connections — 当前连接代理的客户端列表
// ===========================================================================

async fn connections_handler(State(deps): State<ApiDeps>) -> impl IntoResponse {
    let now = std::time::Instant::now();
    let mut conns: Vec<Value> = Vec::new();
    for entry in deps.conn_tracker.iter() {
        let peer = entry.key();
        let (node_id, started) = entry.value();
        let dur = now.duration_since(*started).as_secs_f64();
        conns.push(serde_json::json!({
            "peer": peer.to_string(),
            "node_id": *node_id,
            "duration_secs": (dur * 1000.0).round() / 1000.0,
        }));
    }
    conns.sort_by_key(|c| c["peer"].as_str().unwrap_or("").to_string());
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "connections": conns,
            "count": conns.len(),
        })),
    )
}

// ===========================================================================
// doctor handlers：参数提取 + 调用业务逻辑 + JSON 响应
// ===========================================================================

async fn list_topics_handler(State(deps): State<ApiDeps>) -> impl IntoResponse {
    match list_topics(&deps.client.admin()).await {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json_error(&e)),
        )
            .into_response(),
    }
}

async fn create_topic_handler(
    State(deps): State<ApiDeps>,
    axum::Json(req): axum::Json<CreateTopicRequest>,
) -> impl IntoResponse {
    match create_topic(&deps.client.admin(), req).await {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err((code, msg)) => (code, axum::Json(json_error(&msg))).into_response(),
    }
}

async fn list_consumers_handler(
    State(deps): State<ApiDeps>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    match list_consumers(&deps.client.admin(), &topic).await {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json_error(&e)),
        )
            .into_response(),
    }
}

async fn latest_messages_handler(
    State(deps): State<ApiDeps>,
    Path(topic): Path<String>,
    Query(q): Query<CountQuery>,
) -> impl IntoResponse {
    let count = q.count.unwrap_or(deps.default_count).clamp(1, 1000);
    match latest_messages(&deps.client, &topic, count).await {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json_error(&e)),
        )
            .into_response(),
    }
}

async fn send_message_handler(
    State(deps): State<ApiDeps>,
    Path(topic): Path<String>,
    axum::Json(req): axum::Json<ProduceRequest>,
) -> impl IntoResponse {
    match send_message(&deps.client, &topic, req).await {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json_error(&e)),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct CountQuery {
    count: Option<usize>,
}

// ===========================================================================
// 接口0：topic 管理（列出 / 创建）
// ===========================================================================

#[derive(Debug, Deserialize)]
struct CreateTopicRequest {
    name: String,
    #[serde(default)]
    num_partitions: Option<i32>,
    #[serde(default)]
    replication_factor: Option<i16>,
    #[serde(default)]
    configs: Vec<(String, String)>,
}

async fn list_topics(admin: &AdminClient) -> Result<Value, String> {
    let topics = admin.list_topics().await.map_err(err_str)?;
    let entries: Vec<Value> = topics
        .into_iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "internal": t.internal,
                "partitions": t.partitions,
            })
        })
        .collect();
    Ok(serde_json::json!({
        "count": entries.len(),
        "topics": entries,
    }))
}

async fn create_topic(
    admin: &AdminClient,
    req: CreateTopicRequest,
) -> Result<Value, (StatusCode, String)> {
    let num_partitions = req.num_partitions.unwrap_or(-1).max(-1);
    let replication_factor = req.replication_factor.unwrap_or(-1).max(-1);

    let mut topic = NewTopic::new(req.name, num_partitions, replication_factor);
    for (k, v) in req.configs {
        topic = topic.with_config(k, v);
    }

    let result = admin
        .create_topic(&topic)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, err_str(e)))?;

    if result.already_exists() {
        return Err((
            StatusCode::CONFLICT,
            format!("topic '{}' already exists", result.name),
        ));
    }
    if !result.is_success() {
        let msg = result
            .error_message
            .unwrap_or_else(|| format!("error code: {:?}", result.error_code));
        return Err((StatusCode::INTERNAL_SERVER_ERROR, msg));
    }

    Ok(serde_json::json!({
        "name": result.name,
        "created": true,
    }))
}

// ===========================================================================
// 接口1：查询某 topic 的消费者组
// ===========================================================================

async fn list_consumers(admin: &AdminClient, topic: &str) -> Result<Value, String> {
    let groups = admin.list_groups().await.map_err(err_str)?;
    let consumer_groups: Vec<String> = groups
        .into_iter()
        .filter(|g| g.protocol_type == "consumer" || g.protocol_type.is_empty())
        .map(|g| g.group_id)
        .collect();

    if consumer_groups.is_empty() {
        return Ok(serde_json::json!({
            "topic": topic,
            "consumers": [],
        }));
    }

    let group_ids: Vec<&str> = consumer_groups.iter().map(|s| s.as_str()).collect();
    let descriptions = admin.describe_groups(&group_ids).await.map_err(err_str)?;
    let desc_by_id: std::collections::HashMap<&str, _> = descriptions
        .iter()
        .map(|d| (d.group_id.as_str(), d))
        .collect();

    // 并行获取所有 consumer group 的 offsets，加整体超时保护。
    let fetch_futs: Vec<_> = consumer_groups
        .iter()
        .map(|gid| {
            let gid = gid.clone();
            async move {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    admin.fetch_group_offsets(&gid),
                )
                .await
                {
                    Ok(Ok(o)) => Some((gid.clone(), o)),
                    Ok(Err(e)) => {
                        warn!(group = %gid, "fetch_group_offsets 失败: {e}");
                        None
                    }
                    Err(_) => {
                        warn!(group = %gid, "fetch_group_offsets 超时(5s)");
                        None
                    }
                }
            }
        })
        .collect();
    let offsets_results = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        futures::future::join_all(fetch_futs),
    )
    .await
    .unwrap_or_default();

    let mut consumers = Vec::new();
    for result in offsets_results {
        let (gid, offsets) = match result {
            Some((gid, offsets)) => (gid, offsets),
            None => continue,
        };
        let topic_offsets: Vec<_> = offsets.into_iter().filter(|o| o.topic == topic).collect();
        if topic_offsets.is_empty() {
            continue;
        }

        let desc = desc_by_id.get(gid.as_str());
        let members = desc
            .map(|d| {
                d.members
                    .iter()
                    .map(|m| {
                        serde_json::json!({
                            "member_id": m.member_id,
                            "client_id": m.client_id,
                            "client_host": m.client_host,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let partitions: Vec<Value> = topic_offsets
            .iter()
            .map(|o| {
                serde_json::json!({
                    "partition": o.partition,
                    "committed_offset": o.committed_offset,
                    "log_end_offset": o.log_end_offset,
                    "lag": o.lag,
                })
            })
            .collect();
        let total_lag: i64 = topic_offsets
            .iter()
            .filter(|o| o.lag >= 0)
            .map(|o| o.lag)
            .sum();

        consumers.push(serde_json::json!({
            "group": gid,
            "state": desc.map(|d| d.state.as_str()).unwrap_or("Unknown"),
            "members": members,
            "partitions": partitions,
            "total_lag": total_lag,
        }));
    }

    Ok(serde_json::json!({
        "topic": topic,
        "consumers": consumers,
    }))
}

// ===========================================================================
// 接口2：查看某 topic 最新数据(默认前 5 条)
// ===========================================================================

/// 使用公开API（`Client::send_to_any_broker` + `ListOffsetsRequest`）查询
/// 某 topic 所有分区的 log-end offset，替代之前 vendored
/// `AdminClient::list_end_offsets()` 的 patch。
async fn fetch_log_end_offsets(
    client: &kafka_client::Client,
    admin: &AdminClient,
    topic: &str,
) -> Result<Vec<(i32, i64)>, String> {
    let desc = admin.describe_topics(&[topic]).await.map_err(err_str)?;
    // 收集全部分区，合并为单个 ListOffsetsRequest 一次请求拿回(见 review D5)，
    // 避免逐分区串行请求。
    let mut all_partitions: Vec<i32> = Vec::new();
    for td in &desc {
        for p in &td.partitions {
            all_partitions.push(p.partition);
        }
    }
    if all_partitions.is_empty() {
        return Ok(Vec::new());
    }

    let request = kafka_client::protocol::ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![kafka_client::protocol::ListOffsetsTopic {
            name: topic.to_string(),
            partitions: all_partitions
                .iter()
                .map(|&p| kafka_client::protocol::ListOffsetsPartition {
                    partition_index: p,
                    current_leader_epoch: -1,
                    timestamp: -1, // latest
                })
                .collect(),
        }],
        timeout_ms: 5000,
    };

    let mut ends = Vec::new();
    match client
        .send_to_any_broker::<
            kafka_client::protocol::ListOffsetsRequest,
            kafka_client::protocol::ListOffsetsResponse,
        >(&request)
        .await
    {
        Ok(resp) => {
            for t in &resp.topics {
                if t.name == topic {
                    for rp in &t.partitions {
                        if rp.error_code == 0 {
                            ends.push((rp.partition_index, rp.offset));
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!(topic = %topic, "ListOffsets 失败: {e}");
        }
    }
    ends.sort_by_key(|(p, _)| *p);
    Ok(ends)
}

/// 全局并发限制：同时最多 3 个 latest_messages 请求，防止恶意并发耗尽上游连接。
static MSG_SEM: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(3));

async fn latest_messages(
    client: &kafka_client::Client,
    topic: &str,
    count: usize,
) -> Result<Value, String> {
    let _permit = MSG_SEM
        .acquire()
        .await
        .map_err(|_| "too many concurrent latest_messages requests".to_string())?;
    let admin = client.admin();
    admin.refresh_metadata().await.map_err(err_str)?;

    let ends = fetch_log_end_offsets(client, &admin, topic)
        .await
        .map_err(err_str)?;
    if ends.is_empty() {
        return Ok(serde_json::json!({
            "topic": topic,
            "count": 0,
            "messages": [],
        }));
    }

    let mut starts: Vec<(i32, i64, i64)> = Vec::with_capacity(ends.len());
    for (part, end) in &ends {
        if *end <= 0 {
            continue;
        }
        let start = (*end - count as i64).max(0);
        starts.push((*part, start, *end));
    }
    if starts.is_empty() {
        return Ok(serde_json::json!({
            "topic": topic,
            "count": 0,
            "messages": [],
        }));
    }

    let config = ConsumerConfig::new()
        .with_auto_commit(false)
        .with_auto_offset_reset(AutoOffsetReset::Earliest)
        .with_max_poll_records(count * ends.len() + 16)
        .with_max_wait(std::time::Duration::from_millis(1500));
    let mut consumer = client.consumer(config);

    let partitions: Vec<i32> = starts.iter().map(|(p, _, _)| *p).collect();
    consumer
        .assign(topic.to_string(), partitions)
        .await
        .map_err(err_str)?;
    for (part, start, _end) in &starts {
        consumer
            .seek(topic.to_string(), *part, *start)
            .await
            .map_err(err_str)?;
    }

    let want_total = count * starts.len();
    let mut collected: Vec<ConsumerRecord> = Vec::with_capacity(want_total);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    while collected.len() < want_total && std::time::Instant::now() < deadline {
        let batch = consumer
            .poll_timeout(std::time::Duration::from_secs(2))
            .await
            .map_err(err_str)?;
        if batch.is_empty() {
            if all_partitions_done(&collected, &starts) {
                break;
            }
            continue;
        }
        for r in batch {
            if let Some((_, start, end)) = starts.iter().find(|(p, _, _)| p == &r.partition)
                && r.offset >= *start
                && r.offset < *end
            {
                collected.push(r);
            }
        }
        if all_partitions_done(&collected, &starts) {
            break;
        }
    }
    let _ = consumer.close().await;

    let mut by_part: std::collections::HashMap<i32, Vec<ConsumerRecord>> =
        std::collections::HashMap::new();
    for r in collected {
        by_part.entry(r.partition).or_default().push(r);
    }
    let mut messages: Vec<Value> = Vec::new();
    for (_part, mut recs) in by_part {
        recs.sort_by_key(|r| r.offset);
        let take = recs.len().min(count);
        let recent = recs.split_off(recs.len() - take);
        for r in recent {
            messages.push(record_to_json(r));
        }
    }
    messages.sort_by(|a, b| {
        let pa = a["partition"].as_i64().unwrap_or(0);
        let pb = b["partition"].as_i64().unwrap_or(0);
        pa.cmp(&pb).then_with(|| {
            a["offset"]
                .as_i64()
                .unwrap_or(0)
                .cmp(&b["offset"].as_i64().unwrap_or(0))
        })
    });

    Ok(serde_json::json!({
        "topic": topic,
        "count": messages.len(),
        "messages": messages,
    }))
}

fn all_partitions_done(collected: &[ConsumerRecord], starts: &[(i32, i64, i64)]) -> bool {
    for (part, start, end) in starts {
        let have = collected.iter().filter(|r| r.partition == *part).count();
        let avail = (*end - start) as usize;
        if have < avail {
            return false;
        }
    }
    true
}

fn record_to_json(r: ConsumerRecord) -> Value {
    let key = r.key.map(bytes_to_text);
    let value = bytes_to_text(r.value);
    let headers: Vec<Value> = r
        .headers
        .into_iter()
        .map(|h| {
            serde_json::json!({
                "key": h.key,
                "value": bytes_to_text(h.value),
            })
        })
        .collect();
    serde_json::json!({
        "partition": r.partition,
        "offset": r.offset,
        "timestamp": r.timestamp,
        "key": key,
        "value": value,
        "headers": headers,
    })
}

fn bytes_to_text(b: Bytes) -> Value {
    match std::str::from_utf8(&b) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            Value::String(format!("0x{hex}"))
        }
    }
}

// ===========================================================================
// 接口3：给某 topic 发送数据
// ===========================================================================

#[derive(Debug, Deserialize)]
struct ProduceRequest {
    value: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    partition: Option<i32>,
}

async fn send_message(
    client: &kafka_client::Client,
    topic: &str,
    req: ProduceRequest,
) -> Result<Value, String> {
    let producer = client
        .producer(
            ProducerConfig::new()
                .with_acks(1)
                .with_linger(0)
                .with_retries(3),
        )
        .await;

    let mut record = ProducerRecord::new(topic.to_string(), Bytes::from(req.value));
    if let Some(k) = req.key {
        record = record.with_key(Bytes::from(k));
    }
    if let Some(p) = req.partition {
        record = record.with_partition(p);
    }

    let meta = producer.send(record).await.map_err(err_str)?;
    let _ = producer.close().await;

    Ok(serde_json::json!({
        "topic": meta.topic,
        "partition": meta.partition,
        "offset": meta.offset,
        "timestamp": meta.timestamp,
    }))
}

// ===========================================================================
// 工具函数
// ===========================================================================

fn json_error(msg: &str) -> Value {
    serde_json::json!({ "error": msg })
}

fn err_str<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_to_text_utf8_and_binary() {
        let v = bytes_to_text(Bytes::from_static(b"hello"));
        assert_eq!(v, Value::String("hello".to_string()));

        let v = bytes_to_text(Bytes::from_static(&[0xff, 0xfe]));
        let s = v.as_str().unwrap();
        assert!(s.starts_with("0x"));
    }

    #[test]
    fn produce_request_parse() {
        let j = r#"{"value":"hi","key":"k","partition":2}"#;
        let r: ProduceRequest = serde_json::from_str(j).unwrap();
        assert_eq!(r.value, "hi");
        assert_eq!(r.key.as_deref(), Some("k"));
        assert_eq!(r.partition, Some(2));
    }

    #[test]
    fn produce_request_minimal() {
        let j = r#"{"value":"hi"}"#;
        let r: ProduceRequest = serde_json::from_str(j).unwrap();
        assert_eq!(r.value, "hi");
        assert!(r.key.is_none());
        assert!(r.partition.is_none());
    }

    #[test]
    fn create_topic_request_parse_full() {
        let j = r#"{"name":"orders","num_partitions":6,"replication_factor":3,"configs":[["retention.ms","86400000"]]}"#;
        let r: CreateTopicRequest = serde_json::from_str(j).unwrap();
        assert_eq!(r.name, "orders");
        assert_eq!(r.num_partitions, Some(6));
        assert_eq!(r.replication_factor, Some(3));
        assert_eq!(r.configs, vec![("retention.ms".into(), "86400000".into())]);
    }

    #[test]
    fn create_topic_request_minimal() {
        let j = r#"{"name":"test"}"#;
        let r: CreateTopicRequest = serde_json::from_str(j).unwrap();
        assert_eq!(r.name, "test");
        assert!(r.num_partitions.is_none());
        assert!(r.replication_factor.is_none());
        assert!(r.configs.is_empty());
    }

    #[test]
    fn json_error_shape() {
        let v = json_error("boom");
        assert_eq!(v["error"].as_str().unwrap(), "boom");
    }

    #[test]
    fn api_config_defaults() {
        let cfg = ApiConfig::default();
        assert_eq!(cfg.listen, "127.0.0.1:9100");
        assert_eq!(cfg.default_count, 5);
    }
}
