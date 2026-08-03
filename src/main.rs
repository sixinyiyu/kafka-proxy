use rdkafka::ClientConfig;
use rdkafka::ClientContext;
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::Rebalance;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::error::KafkaResult;
use rdkafka::types::RDKafkaErrorCode;
use serde::Deserialize;
use serde::Serialize;
use simple_log::LogConfigBuilder;
use simple_log::{error, info, warn};
use std::fs;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ctrlc::set_handler(move || {
        info!("程序主动退出");
        std::process::exit(0);
    })
    .unwrap();
    let config = LogConfigBuilder::builder()
        .path("./demo.log")
        .size(1 * 100)
        .roll_count(10)
        .time_format("%Y-%m-%d %H:%M:%S.%f") //E.g:%H:%M:%S.%f
        .level("debug")?
        .output_file()
        .output_console()
        .build();
    simple_log::new(config)?;

    let kafka_config = KafkaConfig {
        group_id: "test_group".to_string(),
        domain: "10.57.177.4".to_string(),
        brokers: "10.57.177.4:9092".to_string(),
        topic: "test_topic".to_string(),
        timeout: 5000,
        ssl_addr: "".to_string(),
        property: Property {
            items: vec![
                "security.protocol=SASL_PLAINTEXT".to_string(),
                "sasl.kerberos.principal=dayu@HADOOP.COM".to_string(),
                "sasl.kerberos.service.name=kafka".to_string(),
                "sasl.mechanism=GSSAPI".to_string(),
            ],
        },
    };

    kafka_config.to_krb5_cnf();

    // 首次初始化kafka配置(设置topic,订阅), 有报错直接返回
    let timeout = Duration::from_secs(kafka_config.timeout);
    // 启动kafka管理员，并尝试新建topic
    let admin_context = AdminCustomContext;
    let admin = KafkaAdminClient::new(&kafka_config, admin_context);
    admin.set_topic(&kafka_config.topic, Some(timeout)).await?;
    info!("set kafka topic {} success", kafka_config.topic);

    // 启动kafka消费者，并尝试进行订阅
    let consumer_context = ConsumerCustomContext;
    let consumer = KafkaConsumer::new(&kafka_config, consumer_context);
    consumer.subscribe(&[kafka_config.topic.as_str()]).await?;
    // 若订阅成功，则开始消费
    // 订阅成功后, 断开kafka, 程序这里也不会往下执行重试, rdkafka库会一直报错并重试
    consumer.start_consuming().await;

    Ok(())
}

pub struct KafkaConfig {
    pub group_id: String,
    pub domain: String,
    pub brokers: String,
    pub topic: String,
    pub timeout: u64,
    pub ssl_addr: String,
    pub property: Property,
}

impl KafkaConfig {
    pub fn to_krb5_cnf(&self) {
        if self.domain.is_empty() {
            return;
        }
        let content = r#"
[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log

[libdefaults]
 default_realm = HADOOP.COM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 500d
 renew_lifetime = 500d
 forwardable = true
 rdns = false
 udp_preference_limit = 1
 kdc_timeout = 1000
 max_retries = 3

[realms]
 HADOOP.COM = {
 kdc = {$domain}:88
 kdc = hostname:88
 admin_server = {$domain}:749
 default_domain = HADOOP.COM
}
"#
        .replace("{$domain}", &self.domain);
        match fs::write("/etc/krb5.conf", &content) {
            Err(e) => error!("写入krbs配置异常 {:?}", e),
            Ok(_) => info!("写入krbs配置成功 {}", content),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq)]
pub struct Property {
    pub items: Vec<String>,
}

pub struct AdminCustomContext;
impl ClientContext for AdminCustomContext {}

/// Kafka消费者端Context，用于配置消费者行为
pub struct ConsumerCustomContext;
impl ClientContext for ConsumerCustomContext {}
impl ConsumerContext for ConsumerCustomContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

/// 限制topic拥有最多3个副本
const DEFAULT_TOPIC_REPLICA_NUM: usize = 3;

/// kafka管理员客户端，用于构建新topic
pub struct KafkaAdminClient<C>
where
    C: ClientContext,
{
    client: AdminClient<C>,
}

impl<C> KafkaAdminClient<C>
where
    C: ClientContext,
{
    /// 根据Kafka服务节点和管理员context，构建管理员client
    pub fn new(kafka_config: &KafkaConfig, admin_context: C) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_config.brokers.clone());
        if !kafka_config.property.items.is_empty() {
            for item in &kafka_config.property.items {
                let kv: Vec<&str> = item.split('=').map(|s| s.trim()).collect();
                if kv[0].is_empty() || kv[1].is_empty() {
                    continue;
                }
                config.set(kv[0], kv[1]);
            }
        }
        if let Some(protocol) = config.get("security.protocol") {
            if protocol.eq("SASL_SS") && !kafka_config.ssl_addr.is_empty() {
                config.set("bootstrap.servers", kafka_config.ssl_addr.clone());
            }
        }
        info!(
            "try create kafka admin {}",
            serde_json::to_string(config.config_map()).unwrap()
        );
        let admin_client: AdminClient<C> = config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(admin_context)
            .expect("Consumer creation failed");
        Self {
            client: admin_client,
        }
    }

    /// 获取Kafka节点数
    pub fn get_num_of_brokers(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> Result<(usize, bool), KafkaError> {
        let metadata = self.client.inner().fetch_metadata(None, timeout)?;

        let topics = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<String>>();

        Ok((
            metadata.brokers().len(),
            topics.contains(&topic.to_string()),
        ))
    }

    /// 设置topic
    pub async fn set_topic(
        &self,
        topic: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), KafkaError> {
        // 根据kafka节点数量，设置topic副本数
        let metadata = self.get_num_of_brokers(topic, timeout)?;
        if metadata.1 {
            warn!("当前topic {} 已存在", topic);
            return Ok(());
        }
        info!("number of kafka brokers: {}", metadata.0);
        let new_topic = create_topic(topic, metadata.0);

        // client尝试设置topic
        let topic_creation_results = self
            .client
            .create_topics::<Vec<&NewTopic>>(
                vec![&new_topic],
                &AdminOptions::new().request_timeout(timeout),
            )
            .await?;
        // 处理topic创建结果
        for topic_result in topic_creation_results {
            match topic_result {
                Ok(success) => info!("Kafka topic creation successful: {}", success),
                Err((topic, err_code)) => {
                    // topic已经存在
                    if let RDKafkaErrorCode::TopicAlreadyExists = err_code {
                        warn!(
                            "Failed to create kafka topic {}: topic already exists",
                            topic
                        );
                    }
                    // 其他错误情形
                    else {
                        return Err(KafkaError::AdminOpCreation(format!(
                            "创建topic {} 失败, 错误码: {:?}",
                            topic, err_code
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// 根据topic名称和Kafka节点数量，构建NewTopic
fn create_topic(topic: &str, num_of_brokers: usize) -> NewTopic<'_> {
    // 限制最多3个副本
    let replica_num = if num_of_brokers <= DEFAULT_TOPIC_REPLICA_NUM {
        num_of_brokers
    } else {
        DEFAULT_TOPIC_REPLICA_NUM
    };
    let replica = TopicReplication::Fixed(replica_num as i32);
    NewTopic::new(topic, 1, replica)
}

/// kafka消费者客户端，用于消费指定topic数据
pub struct KafkaConsumer<C>
where
    C: ConsumerContext,
{
    client: StreamConsumer<C>,
}

impl<C> KafkaConsumer<C>
where
    C: ConsumerContext + 'static,
{
    /// 根据Kafka服务节点、group id和消费者context，构建消费者client
    pub fn new(kafka_config: &KafkaConfig, consumer_context: C) -> Self {
        let mut config = ClientConfig::new();
        config
            .set("group.id", kafka_config.group_id.clone())
            .set("bootstrap.servers", kafka_config.brokers.clone())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true");
        if !kafka_config.property.items.is_empty() {
            for item in &kafka_config.property.items {
                let kv: Vec<&str> = item.split('=').map(|s| s.trim()).collect();
                if kv[0].is_empty() || kv[1].is_empty() {
                    continue;
                }
                config.set(kv[0], kv[1]);
            }
        }
        if let Some(protocol) = config.get("security.protocol") {
            if protocol.eq("SASL_SS") && !kafka_config.ssl_addr.is_empty() {
                config.set("bootstrap.servers", kafka_config.ssl_addr.clone());
            }
        }
        info!(
            "try create kafka consume config is : {}",
            serde_json::to_string(config.config_map()).unwrap()
        );
        let consumer = config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(consumer_context)
            .expect("Consumer creation failed");
        Self { client: consumer }
    }

    /// 订阅topics
    pub async fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.client.subscribe(topics)?;
        Ok(())
    }

    /// 开始消费
    pub async fn start_consuming(&self) {
        loop {
            match self.client.recv().await {
                // 消息接收失败，告警
                Err(e) => warn!("Kafka error: {}", e),
                // 消息接收成功
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        // 载荷为空，不作操作
                        None => "",
                        // 载荷不为空，进行存储
                        Some(Ok(payload)) => payload,
                        // 载荷反序列化出错，告警
                        Some(Err(e)) => {
                            warn!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    info!("payload: '{}', topic: {}", payload, m.topic());
                    // 提交
                    if let Err(e) = self.client.commit_message(&m, CommitMode::Async) {
                        error!("kafka提交失败, 错误信息【{}】", e);
                    };
                }
            };
        }
    }
}
