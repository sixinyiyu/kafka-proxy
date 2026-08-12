//! 响应拦截与改写（Metadata / DescribeCluster）— 见设计文档 §6.2。
//!
//! Kafka 客户端是智能客户端：会从元数据响应里读 broker 的 host:port 然后直连。
//! 因此必须把每个 broker 的 host:port 改写为 proxy 自己暴露的对应监听地址，
//! 否则客户端会绕过 proxy 直连真实 broker(且被 Kerberos 挡住)。
//!
//! 改写思路：对上游返回的「原始响应帧」按版本解码 → 改写 brokers 的
//! host/port → 重新编码。其余响应帧原样透传。
//!
//! 响应帧(去掉长度前缀)布局：
//!   correlation_id int32 [0..4]
//!   ... body (随 api_key/version 不同)
//!
//! 我们只需识别 api_key=3(Metadata) 与 api_key=60(DescribeCluster) 的「响应」。
//! 但响应帧里没有 api_key/version(只有 correlation_id)，无法从响应帧本身判断类型。
//! 因此改写器需要知道「这条响应对应的请求是什么 api_key」——由 relay 在转发请求时
//! 记录 api_key↔correlation_id 映射，响应回来时查表。这是 P1.5 relay 的职责。
//!
//! 本模块提供：给定 (api_key, version, 原始响应字节, 改写参数) → 改写后字节。

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use kafka_client::protocol::{
    DescribeClusterResponse, FindCoordinatorResponse, Message, MetadataResponse, Response,
};

/// 需要改写的响应 api_key。
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_DESCRIBE_CLUSTER: i16 = 60;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;

/// 改写参数：node_id → (advertise_host, 下游监听端口)。
///
/// 每个 broker 可有独立的 advertise_host(对应 `bootstrap_server_mapping` 每项的
/// 主机名)，写入元数据告诉客户端"连这个地址"。
///
/// 用 `DashMap` 而非 `HashMap`：RewriteMap 经 Arc 在多条 relay 任务间共享，改写时
/// 并发读；P4 元数据动态化时还需并发插入新 broker 端口(见 .clinerules Development
/// Tools：并发场景优先 dashmap)。
#[derive(Debug)]
pub struct RewriteMap {
    /// node_id -> (advertise_host, 下游端口)(并发安全)。
    pub node_ports: DashMap<i32, (String, u16)>,
}

impl RewriteMap {
    /// 从普通 HashMap 构造(配置加载 + node_id 反查后一次性转换)。
    pub fn new(node_ports: HashMap<i32, (String, u16)>) -> Self {
        Self {
            node_ports: DashMap::from_iter(node_ports),
        }
    }
}

/// 尝试改写一条「响应帧」(去掉长度前缀后的完整数据，含 correlation_id)。
///
/// - 若 api_key 需要改写且能成功解码，返回改写后的新字节。
/// - 否则(无需改写/无法解码)返回 None，调用方应原样透传。
///
/// `api_key`/`version` 由调用方(relay)从「对应请求帧」解析并提供。
pub fn rewrite_response(
    api_key: i16,
    version: i16,
    raw: &Bytes,
    map: &RewriteMap,
) -> Option<Bytes> {
    match api_key {
        API_KEY_METADATA => rewrite_metadata_response(version, raw, map),
        API_KEY_DESCRIBE_CLUSTER => rewrite_describe_cluster_response(version, raw, map),
        API_KEY_FIND_COORDINATOR => rewrite_find_coordinator_response(version, raw, map),
        _ => None,
    }
}

/// 改写 MetadataResponse。
///
/// 响应帧(去掉长度前缀)布局：
///   correlation_id            int32 [0..4]
///   throttle_time_ms          int32 [4..8]   (v3+)
///   brokers                   array
///     node_id                 int32
///     host                    string
///     port                    int32
///     rack                    nullable string (v1+)
///   cluster_id                nullable string (v2+)
///   controller_id             int32 (v1+) / 在 v0 由 partition leader 隐含
///   ...
///
/// 这里用 protocol crate 的 `MetadataResponse::decode_frame` 解码(它会跳过
/// correlation_id)，改写 brokers 后手动重新编码(响应头 + body)。
fn rewrite_metadata_response(version: i16, raw: &Bytes, map: &RewriteMap) -> Option<Bytes> {
    // decode_frame 期望「去掉长度前缀的完整响应数据」(含 correlation_id)。
    let (header, mut resp): (_, MetadataResponse) =
        MetadataResponse::decode_frame(raw.clone(), version).ok()?;

    let mut changed = false;
    for b in &mut resp.brokers {
        if let Some(entry) = map.node_ports.get(&b.node_id) {
            let (host, port) = entry.value();
            b.host = host.clone();
            b.port = *port as i32;
            changed = true;
        }
    }

    if !changed {
        return None;
    }

    // 手动重新编码：响应头(correlation_id [+ flexible 的 tagged_fields]) + body。
    // Response trait 没有 encode_frame，需用 Message::encode / flexible_encode。
    //
    // 注意(见 review M3)：此处响应头 tagged_fields 硬编码为 0(空)。对当前所有
    // Kafka 版本(≤ KRaft/3.x)的 Metadata/DescribeCluster/FindCoordinator 响应头
    // 均正确——它们的 ResponseHeader 在 flexible 版本下只有 correlation_id +
    // 空 tagged_fields。若未来 Kafka 版本在响应头引入非空 tagged_fields，需从
    // decode_frame 返回的 header 透传而非硬编码。
    let use_flexible = MetadataResponse::is_flexible_version(version);
    let cid = header.correlation_id();
    let mut buf = BytesMut::new();
    // 响应头
    buf.put_i32(cid);
    if use_flexible {
        // tagged_fields 数量 = 0 (varint 0)
        encode_unsigned_varint(&mut buf, 0);
    }

    // 响应体
    let encode_result = if use_flexible {
        resp.flexible_encode(&mut buf, version)
    } else {
        resp.encode(&mut buf, version)
    };
    if let Err(e) = encode_result {
        tracing::warn!(?e, "MetadataResponse 重新编码失败，原样透传");
        return None;
    }

    // buf 现为「不含长度前缀的完整响应帧」，relay 的 codec 会再加长度前缀。
    Some(buf.freeze())
}

/// 改写 DescribeClusterResponse(apikey 60)。
///
/// 与 Metadata 类似：把 brokers[].host/port 改写为 proxy 的 advertise 地址。
/// DescribeClusterResponse 始终是 flexible 版本(flexible_versions="0+")，valid 0-2。
/// brokers 用 DescribeClusterBroker，其 id 字段为 `broker_id`(而非 node_id)。
fn rewrite_describe_cluster_response(version: i16, raw: &Bytes, map: &RewriteMap) -> Option<Bytes> {
    let (header, mut resp): (_, DescribeClusterResponse) =
        DescribeClusterResponse::decode_frame(raw.clone(), version).ok()?;

    let mut changed = false;
    for b in &mut resp.brokers {
        if let Some(entry) = map.node_ports.get(&b.broker_id) {
            let (host, port) = entry.value();
            b.host = host.clone();
            b.port = *port as i32;
            changed = true;
        }
    }

    if !changed {
        return None;
    }

    // DescribeClusterResponse 始终 flexible：响应头 = correlation_id + tagged_fields。
    let use_flexible = DescribeClusterResponse::is_flexible_version(version);
    let cid = header.correlation_id();
    let mut buf = BytesMut::new();
    buf.put_i32(cid);
    if use_flexible {
        encode_unsigned_varint(&mut buf, 0);
    }
    let encode_result = if use_flexible {
        resp.flexible_encode(&mut buf, version)
    } else {
        resp.encode(&mut buf, version)
    };
    if let Err(e) = encode_result {
        tracing::warn!(?e, "DescribeClusterResponse 重新编码失败，原样透传");
        return None;
    }

    Some(buf.freeze())
}

/// 改写 FindCoordinatorResponse(apikey 10)。
///
/// 消费者/生产者通过 FindCoordinator 发现 group/transaction coordinator 的
/// host:port，如果 proxy 不改写，客户端会绕过 proxy 直连真实 coordinator broker
/// (被 Kerberos/网络隔离挡住)，导致日志中反复出现
/// "Connection to node X (/127.0.0.1:port) could not be established"。
///
/// FindCoordinatorResponse 有两个 epoch：
///   v0–v3: 顶层字段 node_id / host / port(单个 coordinator)。
///   v4+  : coordinators 数组(每个含 node_id / host / port)。
/// flexible_versions="3+"。
fn rewrite_find_coordinator_response(version: i16, raw: &Bytes, map: &RewriteMap) -> Option<Bytes> {
    let (header, mut resp): (_, FindCoordinatorResponse) =
        FindCoordinatorResponse::decode_frame(raw.clone(), version).ok()?;

    let mut changed = false;
    if version <= 3 {
        // v0–v3：改写顶层 node_id / host / port。
        if let Some(entry) = map.node_ports.get(&resp.node_id) {
            let (host, port) = entry.value();
            tracing::debug!(
                node_id = resp.node_id,
                from = %resp.host,
                from_port = resp.port,
                to = %host,
                to_port = *port,
                cid = header.correlation_id(),
                version,
                "FindCoordinator(v0-v3) 改写 coordinator 地址"
            );
            resp.host = host.clone();
            resp.port = *port as i32;
            changed = true;
        } else {
            tracing::debug!(
                node_id = resp.node_id,
                host = %resp.host,
                "FindCoordinator(v0-v3) node_id 不在映射中，原样透传"
            );
        }
    } else {
        // v4+：改写 coordinators 数组每项的 node_id / host / port。
        for c in &mut resp.coordinators {
            if let Some(entry) = map.node_ports.get(&c.node_id) {
                let (host, port) = entry.value();
                tracing::debug!(
                    node_id = c.node_id,
                    key = %c.key,
                    from = %c.host,
                    from_port = c.port,
                    to = %host,
                    to_port = *port,
                    cid = header.correlation_id(),
                    version,
                    "FindCoordinator(v4+) 改写 coordinator 地址"
                );
                c.host = host.clone();
                c.port = *port as i32;
                changed = true;
            }
        }
    }

    if !changed {
        return None;
    }

    let use_flexible = FindCoordinatorResponse::is_flexible_version(version);
    let cid = header.correlation_id();
    let mut buf = BytesMut::new();
    buf.put_i32(cid);
    if use_flexible {
        encode_unsigned_varint(&mut buf, 0);
    }
    let encode_result = if use_flexible {
        resp.flexible_encode(&mut buf, version)
    } else {
        resp.encode(&mut buf, version)
    };
    if let Err(e) = encode_result {
        tracing::warn!(?e, "FindCoordinatorResponse 重新编码失败，原样透传");
        return None;
    }

    Some(buf.freeze())
}

/// 编码无符号 varint(协议 flexible 格式用)。
pub fn encode_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// 从「请求帧」(去掉长度前缀)解析 api_key 与 version。
/// 布局: api_key int16 [0..2], api_version int16 [2..4], correlation_id int32 [4..8].
pub fn parse_request_header(raw: &[u8]) -> Option<(i16, i16, i32)> {
    if raw.len() < 8 {
        return None;
    }
    let mut cur = &raw[..];
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();
    Some((api_key, api_version, correlation_id))
}

/// 从「响应帧」(去掉长度前缀)解析 correlation_id(响应帧前 4 字节)。
pub fn parse_response_cid(raw: &[u8]) -> Option<i32> {
    if raw.len() < 4 {
        return None;
    }
    Some((&raw[..4]).get_i32())
}

/// 构造一个「需要改写的 api_key 集合」判定。
pub fn needs_rewrite(api_key: i16) -> bool {
    matches!(
        api_key,
        API_KEY_METADATA | API_KEY_DESCRIBE_CLUSTER | API_KEY_FIND_COORDINATOR
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use kafka_client::protocol::api::describe_cluster_response::DescribeClusterBroker;
    use kafka_client::protocol::api::find_coordinator_response::Coordinator;

    /// 手动编码一个 MetadataRequest v1 原始帧(去掉长度前缀)用于测试 header 解析。
    fn metadata_request_v1(cid: i32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // api_key
        buf.put_i16(1); // api_version
        buf.put_i32(cid);
        buf.put_i16(-1); // client_id null
        buf.put_i32(-1); // topics null
        buf.freeze()
    }

    #[test]
    fn parse_req_header() {
        let f = metadata_request_v1(0x11223344);
        let (ak, v, cid) = parse_request_header(&f).unwrap();
        assert_eq!(ak, 3);
        assert_eq!(v, 1);
        assert_eq!(cid, 0x11223344);
    }

    #[test]
    fn parse_resp_cid() {
        let mut buf = BytesMut::new();
        buf.put_i32(0x55667788);
        assert_eq!(parse_response_cid(&buf), Some(0x55667788));
    }

    #[test]
    fn needs_rewrite_metadata() {
        assert!(needs_rewrite(API_KEY_METADATA));
        assert!(needs_rewrite(API_KEY_DESCRIBE_CLUSTER));
        assert!(!needs_rewrite(0)); // Produce
        assert!(!needs_rewrite(1)); // Fetch
    }

    #[test]
    fn short_frame_header_none() {
        assert!(parse_request_header(&[1, 2, 3]).is_none());
        assert!(parse_response_cid(&[1, 2, 3]).is_none());
    }

    // ---- 元数据改写 round-trip 测试 ----

    /// 构造改写映射：node 1->("127.0.0.1",19092), 2->("127.0.0.1",19093), 3->("127.0.0.1",19094)。
    fn test_map() -> RewriteMap {
        let mut ports = HashMap::new();
        ports.insert(1, ("127.0.0.1".to_string(), 19092));
        ports.insert(2, ("127.0.0.1".to_string(), 19093));
        ports.insert(3, ("127.0.0.1".to_string(), 19094));
        RewriteMap::new(ports)
    }

    /// 编码一条 MetadataResponse v9(flexible) 响应帧(含 correlation_id)。
    fn encode_metadata_response_v9(cid: i32) -> Bytes {
        use kafka_client::protocol::{
            MetadataResponse, MetadataResponseBroker, MetadataResponseTopic,
        };
        let _ = MetadataResponseTopic::default();
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 1,
                    host: "real-broker-1".to_string(),
                    port: 9092,
                    rack: None,
                },
                MetadataResponseBroker {
                    node_id: 2,
                    host: "real-broker-2".to_string(),
                    port: 9092,
                    rack: None,
                },
            ],
            cluster_id: Some("test-cluster".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: -2147483648,
            error_code: 0,
        };
        // v9 是 flexible 版本(metadata flexible_version=9)。
        let version = 9;
        let mut buf = BytesMut::new();
        buf.put_i32(cid); // correlation_id
        encode_unsigned_varint(&mut buf, 0); // tagged_fields
        resp.flexible_encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    #[test]
    fn rewrite_metadata_roundtrip() {
        let raw = encode_metadata_response_v9(0x1234);
        let map = test_map();
        let rewritten = rewrite_metadata_response(9, &raw, &map).expect("应改写成功");

        // 解码改写后的帧，验证 host/port 已被改写。
        let (header, resp): (_, MetadataResponse) =
            MetadataResponse::decode_frame(rewritten.clone(), 9).unwrap();
        assert_eq!(header.correlation_id(), 0x1234, "correlation_id 应透传");
        assert_eq!(resp.brokers.len(), 2);
        for b in &resp.brokers {
            assert_eq!(b.host, "127.0.0.1", "host 应改写为 advertise");
            assert!(
                b.port == 19092 || b.port == 19093,
                "port 应改写为 proxy 端口, got {}",
                b.port
            );
        }
    }

    #[test]
    fn rewrite_metadata_no_change_returns_none() {
        // 映射里没有 node_id=1 的端口(空映射)，不应改写。
        let raw = encode_metadata_response_v9(1);
        let map = RewriteMap::new(HashMap::new());
        assert!(rewrite_metadata_response(9, &raw, &map).is_none());
    }

    /// 编码一条 DescribeClusterResponse v0(flexible) 响应帧(含 correlation_id)。
    fn encode_describe_cluster_response_v0(cid: i32) -> Bytes {
        let resp = DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            endpoint_type: 1,
            cluster_id: "test-cluster".to_string(),
            controller_id: 1,
            brokers: vec![
                DescribeClusterBroker {
                    broker_id: 1,
                    host: "real-broker-1".to_string(),
                    port: 9092,
                    rack: None,
                    is_fenced: false,
                },
                DescribeClusterBroker {
                    broker_id: 2,
                    host: "real-broker-2".to_string(),
                    port: 9092,
                    rack: None,
                    is_fenced: false,
                },
            ],
            cluster_authorized_operations: -2147483648,
        };
        // v0 即 flexible(describe_cluster flexible_versions="0+")。
        let version = 0;
        let mut buf = BytesMut::new();
        buf.put_i32(cid); // correlation_id
        encode_unsigned_varint(&mut buf, 0); // tagged_fields
        resp.flexible_encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    #[test]
    fn rewrite_describe_cluster_roundtrip() {
        let raw = encode_describe_cluster_response_v0(0x5678);
        let map = test_map();
        let rewritten = rewrite_describe_cluster_response(0, &raw, &map).expect("应改写成功");

        let (header, resp): (_, DescribeClusterResponse) =
            DescribeClusterResponse::decode_frame(rewritten.clone(), 0).unwrap();
        assert_eq!(header.correlation_id(), 0x5678, "correlation_id 应透传");
        assert_eq!(resp.brokers.len(), 2);
        for b in &resp.brokers {
            assert_eq!(b.host, "127.0.0.1", "host 应改写为 advertise");
            assert!(
                b.port == 19092 || b.port == 19093,
                "port 应改写为 proxy 端口, got {}",
                b.port
            );
        }
    }

    #[test]
    fn rewrite_describe_cluster_via_dispatch() {
        // 通过顶层 rewrite_response 派发，验证 api_key=60 路由正确。
        let raw = encode_describe_cluster_response_v0(42);
        let map = test_map();
        let rewritten = rewrite_response(API_KEY_DESCRIBE_CLUSTER, 0, &raw, &map);
        assert!(rewritten.is_some(), "DescribeCluster 应被派发改写");
    }

    #[test]
    fn rewrite_describe_cluster_no_change_returns_none() {
        let raw = encode_describe_cluster_response_v0(1);
        let map = RewriteMap::new(HashMap::new());
        assert!(rewrite_describe_cluster_response(0, &raw, &map).is_none());
    }

    // ---- FindCoordinator 改写 round-trip 测试 ----

    /// 编码一条 FindCoordinatorResponse v3(flexible) 响应帧。
    /// v3 是第一个 flexible 版本(flexible_versions="3+")，顶层字段含 node_id/host/port。
    fn encode_find_coordinator_response_v3(cid: i32) -> Bytes {
        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            node_id: 3,
            host: "real-coordinator-3".to_string(),
            port: 9092,
            coordinators: vec![],
        };
        let version: i16 = 3;
        let mut buf = BytesMut::new();
        buf.put_i32(cid);
        // v3 是 flexible 版本：需要 tagged_fields 头。
        encode_unsigned_varint(&mut buf, 0);
        resp.flexible_encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    #[test]
    fn rewrite_find_coordinator_v3_roundtrip() {
        let raw = encode_find_coordinator_response_v3(0xABCD);
        let map = test_map();
        let rewritten = rewrite_find_coordinator_response(3, &raw, &map).expect("v3 应改写成功");

        let (header, resp): (_, FindCoordinatorResponse) =
            FindCoordinatorResponse::decode_frame(rewritten.clone(), 3).unwrap();
        assert_eq!(header.correlation_id(), 0xABCD, "correlation_id 应透传");
        // node_id=3 映射为 127.0.0.1:19094。
        assert_eq!(resp.host, "127.0.0.1", "coordinator host 应改写");
        assert_eq!(resp.port, 19094, "coordinator port 应改写");
        assert_eq!(resp.node_id, 3, "node_id 应保留");
    }

    #[test]
    fn rewrite_find_coordinator_v3_no_change_returns_none() {
        let raw = encode_find_coordinator_response_v3(1);
        let map = RewriteMap::new(HashMap::new());
        assert!(rewrite_find_coordinator_response(3, &raw, &map).is_none());
    }

    /// 编码一条 FindCoordinatorResponse v4(flexible) 响应帧，含 coordinators 数组。
    fn encode_find_coordinator_response_v4(cid: i32) -> Bytes {
        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            node_id: -1,
            host: String::new(),
            port: -1,
            coordinators: vec![
                Coordinator {
                    key: "dayu_audit".to_string(),
                    node_id: 2,
                    host: "real-coordinator-2".to_string(),
                    port: 9092,
                    error_code: 0,
                    error_message: None,
                },
                Coordinator {
                    key: "txn_coord".to_string(),
                    node_id: 3,
                    host: "real-coordinator-3".to_string(),
                    port: 9092,
                    error_code: 0,
                    error_message: None,
                },
            ],
        };
        let version: i16 = 4;
        let mut buf = BytesMut::new();
        buf.put_i32(cid);
        encode_unsigned_varint(&mut buf, 0); // tagged_fields
        resp.flexible_encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    #[test]
    fn rewrite_find_coordinator_v4_roundtrip() {
        let raw = encode_find_coordinator_response_v4(0xBEEF);
        let map = test_map();
        let rewritten = rewrite_find_coordinator_response(4, &raw, &map).expect("v4 应改写成功");

        let (header, resp): (_, FindCoordinatorResponse) =
            FindCoordinatorResponse::decode_frame(rewritten.clone(), 4).unwrap();
        assert_eq!(header.correlation_id(), 0xBEEF, "correlation_id 应透传");
        assert_eq!(resp.coordinators.len(), 2);
        // node_id=2 → 127.0.0.1:19093。
        assert_eq!(resp.coordinators[0].host, "127.0.0.1");
        assert_eq!(resp.coordinators[0].port, 19093);
        assert_eq!(resp.coordinators[0].key, "dayu_audit");
        // node_id=3 → 127.0.0.1:19094。
        assert_eq!(resp.coordinators[1].host, "127.0.0.1");
        assert_eq!(resp.coordinators[1].port, 19094);
    }

    #[test]
    fn rewrite_find_coordinator_via_dispatch() {
        let raw = encode_find_coordinator_response_v3(99);
        let map = test_map();
        let rewritten = rewrite_response(API_KEY_FIND_COORDINATOR, 3, &raw, &map);
        assert!(rewritten.is_some(), "FindCoordinator 应被派发改写");
    }

    #[test]
    fn needs_rewrite_find_coordinator() {
        assert!(needs_rewrite(API_KEY_FIND_COORDINATOR));
    }
}
