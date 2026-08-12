//! 上游连接：用 kafka_client 0.8 的 `Builder::build_framed()` 建立
//! GSSAPI/SASL/明文 认证后的 raw framed 流，供 1:1 帧转发使用。
//!
//! 关键：每个 broker 用各自的服务 principal `kafka/<broker-host>`，
//! 该 per-broker principal 修复已在上游 v0.7.0 合并(commit d536afb0)，
//! 故无需再 vendor kafka_client 源码。`build_framed()` 返回
//! `(KafkaFramed, NegotiatedVersions)`，本代理做 1:1 透传(不本地应答
//! ApiVersions)，故仅取 `KafkaFramed::into_inner()` 拿到底层 `Framed`
//! 用于 `.split()` 全双工转发。

use std::net::SocketAddr;
use std::sync::Arc;

use kafka_client::connection::Builder;
use kafka_client::transport::SecurityProtocol;
use kafka_client::wire::{KafkaCodec, KafkaFrame};
use kafka_client::{KerberosCredentials, SaslCredentials, SaslMechanismType};
use tokio_util::codec::Framed;

use crate::config::{AuthConfig, AuthMechanism};

/// 上游认证参数（从配置构造，可在多条连接间共享/克隆）。
#[derive(Clone)]
pub struct UpstreamAuth {
    inner: AuthInner,
}

/// 认证类型枚举（内部）：用枚举匹配，避免字符串(见 .clinerules Code Style)。
#[derive(Clone)]
pub enum AuthInner {
    /// 无认证(明文集群)。
    Plaintext,
    /// SASL PLAIN/SCRAM(非 Kerberos)。
    Sasl {
        mechanism: SaslMechanismType,
        credentials: SaslCredentials,
    },
    /// SASL/GSSAPI(Kerberos)。
    Gssapi {
        creds: KerberosCredentials,
        kdc_host: String,
        kdc_port: u16,
    },
}

impl UpstreamAuth {
    /// 从配置段的 `mechanism`(枚举) 构造。
    ///
    /// 用 `AuthMechanism` 枚举匹配，避免字符串拼写错误(见 .clinerules Code Style)。
    pub fn from_config(auth: &AuthConfig) -> Result<Self, UpstreamError> {
        match auth.mechanism {
            AuthMechanism::None => Ok(Self {
                inner: AuthInner::Plaintext,
            }),
            AuthMechanism::Gssapi => {
                let principal = auth
                    .kerberos_principal
                    .clone()
                    .ok_or(UpstreamError::Missing("kerberos_principal"))?;
                let keytab = auth
                    .kerberos_keytab
                    .clone()
                    .ok_or(UpstreamError::Missing("kerberos_keytab"))?;
                let mut creds = KerberosCredentials::new(principal).with_keytab(keytab);
                if let Some(realm) = &auth.kerberos_realm {
                    creds = creds.with_realm(realm);
                }
                // kerberos_kdc 形如 "host:88"
                let (kdc_host, kdc_port) = parse_kdc(auth)?;
                Ok(Self {
                    inner: AuthInner::Gssapi {
                        creds,
                        kdc_host,
                        kdc_port,
                    },
                })
            }
            AuthMechanism::Plain => {
                let (u, p) = user_pass(auth)?;
                Ok(Self {
                    inner: AuthInner::Sasl {
                        mechanism: SaslMechanismType::Plain,
                        credentials: SaslCredentials::new(SaslMechanismType::Plain, u, p),
                    },
                })
            }
            AuthMechanism::ScramSha256 => {
                let (u, p) = user_pass(auth)?;
                Ok(Self {
                    inner: AuthInner::Sasl {
                        mechanism: SaslMechanismType::ScramSha256,
                        credentials: SaslCredentials::new(SaslMechanismType::ScramSha256, u, p),
                    },
                })
            }
            AuthMechanism::ScramSha512 => {
                let (u, p) = user_pass(auth)?;
                Ok(Self {
                    inner: AuthInner::Sasl {
                        mechanism: SaslMechanismType::ScramSha512,
                        credentials: SaslCredentials::new(SaslMechanismType::ScramSha512, u, p),
                    },
                })
            }
            AuthMechanism::Mtls => Err(UpstreamError::UnsupportedMechanism("mtls".to_string())),
        }
    }

    /// 建立 GSSAPI/SASL/明文 认证后的 raw framed 流。
    ///
    /// `broker_host` 用于 Kerberos 服务 principal `kafka/<broker_host>`，
    /// 每个 broker 必须传各自的主机名(这正是 per-broker principal 修复点)。
    pub async fn connect_framed(
        &self,
        addr: SocketAddr,
        broker_host: &str,
    ) -> Result<UpstreamFramed, UpstreamError> {
        let mut builder = Builder::new(
            addr,
            self.security_protocol(),
            kafka_client::NAME.to_string(),
            kafka_client::VERSION.to_string(),
        )
        .with_client_id("kafka-proxy".to_string());

        match &self.inner {
            AuthInner::Plaintext => {
                // 无 SASL：Builder 内部会探测，明文集群直接通过。
            }
            AuthInner::Sasl {
                mechanism,
                credentials,
            } => {
                builder = builder.with_sasl(*mechanism, credentials.clone());
            }
            AuthInner::Gssapi {
                creds,
                kdc_host,
                kdc_port,
            } => {
                let creds = creds.clone().with_broker_hostname(broker_host.to_string());
                builder = builder
                    .with_kerberos(creds)
                    .with_kdc(kdc_host.clone(), *kdc_port)
                    .with_broker_hostname(broker_host);
            }
        }

        // build_framed() 返回 (KafkaFramed, NegotiatedVersions)。本代理做 1:1
        // 帧透传(不本地应答 ApiVersions)，无需 NegotiatedVersions；通过
        // KafkaFramed::into_inner() 拿到底层 Framed 用于 .split() 全双工转发。
        let (framed, _negotiated) = builder
            .build_framed()
            .await
            .map_err(UpstreamError::Connect)?;
        Ok(framed.into_inner())
    }

    /// 返回内部认证类型引用（供 lib.rs 判断是否为 GSSAPI 以决定 bootstrap 策略）。
    pub fn inner_ref(&self) -> &AuthInner {
        &self.inner
    }

    fn security_protocol(&self) -> SecurityProtocol {
        match &self.inner {
            AuthInner::Plaintext => SecurityProtocol::Plaintext,
            AuthInner::Sasl { .. } | AuthInner::Gssapi { .. } => SecurityProtocol::SaslPlaintext,
        }
    }

    /// 把认证配置应用到 kafka_client::ClientBuilder（工厂模式，见 .clinerules 架构）。
    ///
    /// bootstrap 阶段用 `Client` 拉元数据时复用此方法，避免在 lib.rs 重复一遍
    /// 字符串匹配。构造逻辑集中在 UpstreamAuth，枚举匹配只此一处。
    pub fn apply_to_client_builder(
        &self,
        mut builder: kafka_client::ClientBuilder,
    ) -> kafka_client::ClientBuilder {
        match &self.inner {
            AuthInner::Plaintext => builder.with_plaintext(),
            AuthInner::Sasl {
                mechanism,
                credentials,
            } => {
                // ClientBuilder::with_sasl 接收 (SaslMechanismType, user, pass)，
                // 这里用 getter 从已构造的 credentials 取出。
                builder = builder.with_sasl(
                    *mechanism,
                    credentials.username().to_string(),
                    credentials.password().to_string(),
                );
                builder
            }
            AuthInner::Gssapi {
                creds,
                kdc_host,
                kdc_port,
            } => {
                builder = builder
                    .with_kerberos(creds.clone())
                    .with_kdc(kdc_host.clone(), *kdc_port)
                    .with_client_id("kafka-proxy-bootstrap");
                builder
            }
        }
    }
}

fn user_pass(auth: &AuthConfig) -> Result<(String, String), UpstreamError> {
    let u = auth
        .username
        .clone()
        .ok_or(UpstreamError::Missing("username"))?;
    let p = auth
        .password
        .clone()
        .ok_or(UpstreamError::Missing("password"))?;
    Ok((u, p))
}

fn parse_kdc(auth: &AuthConfig) -> Result<(String, u16), UpstreamError> {
    let kdc = auth
        .kerberos_kdc
        .as_deref()
        .ok_or(UpstreamError::Missing("kerberos_kdc"))?;
    let (h, p) = kdc
        .rsplit_once(':')
        .ok_or_else(|| UpstreamError::InvalidKdc(kdc.to_string()))?;
    let port: u16 = p
        .parse()
        .map_err(|_| UpstreamError::InvalidKdc(kdc.to_string()))?;
    Ok((h.to_string(), port))
}

/// 共享的上游认证句柄（Arc 包一层便于在 acceptor 间克隆）。
pub type SharedUpstreamAuth = Arc<UpstreamAuth>;

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("缺少配置项: {0}")]
    Missing(&'static str),
    #[error("不支持的认证机制: {0}")]
    UnsupportedMechanism(String),
    #[error("kerberos_kdc 格式错误(应为 host:port): {0}")]
    InvalidKdc(String),
    #[error("建立上游连接失败: {0}")]
    Connect(#[from] kafka_client::KafkaError),
}

// 重新导出帧类型，relay 模块直接用。
pub use kafka_client::transport::NetworkStream;
pub type UpstreamFramed = Framed<Box<dyn NetworkStream>, KafkaCodec>;
pub type UpstreamFrame = KafkaFrame;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, AuthMechanism};

    #[test]
    fn plaintext_auth() {
        let auth = AuthConfig {
            mechanism: AuthMechanism::None,
            ..Default::default()
        };
        let ua = UpstreamAuth::from_config(&auth).unwrap();
        assert!(matches!(ua.inner, AuthInner::Plaintext));
    }

    #[test]
    fn gssapi_requires_principal_keytab_kdc() {
        let mut auth = AuthConfig {
            mechanism: AuthMechanism::Gssapi,
            ..Default::default()
        };
        // 缺 principal
        assert!(UpstreamAuth::from_config(&auth).is_err());
        auth.kerberos_principal = Some("dayu@HADOOP.COM".into());
        // 缺 keytab
        assert!(UpstreamAuth::from_config(&auth).is_err());
        auth.kerberos_keytab = Some("/etc/kp/kb.keytab".into());
        // 缺 kdc
        assert!(UpstreamAuth::from_config(&auth).is_err());
        auth.kerberos_kdc = Some("kdc:88".into());
        let ua = UpstreamAuth::from_config(&auth).unwrap();
        assert!(matches!(ua.inner, AuthInner::Gssapi { .. }));
    }

    #[test]
    fn plain_auth() {
        let auth = AuthConfig {
            mechanism: AuthMechanism::Plain,
            username: Some("user".into()),
            password: Some("pass".into()),
            ..Default::default()
        };
        let ua = UpstreamAuth::from_config(&auth).unwrap();
        assert!(matches!(ua.inner, AuthInner::Sasl { .. }));
    }

    #[test]
    fn scram_auth() {
        let auth = AuthConfig {
            mechanism: AuthMechanism::ScramSha512,
            username: Some("user".into()),
            password: Some("pass".into()),
            ..Default::default()
        };
        let ua = UpstreamAuth::from_config(&auth).unwrap();
        assert!(matches!(
            ua.inner,
            AuthInner::Sasl {
                mechanism: SaslMechanismType::ScramSha512,
                ..
            }
        ));
    }

    #[test]
    fn parse_kdc_format() {
        let auth = AuthConfig {
            mechanism: AuthMechanism::Gssapi,
            kerberos_principal: Some("p".into()),
            kerberos_keytab: Some("k".into()),
            kerberos_kdc: Some("kdc.example.com:88".into()),
            ..Default::default()
        };
        let (h, p) = parse_kdc(&auth).unwrap();
        assert_eq!(h, "kdc.example.com");
        assert_eq!(p, 88);
        let bad = AuthConfig {
            kerberos_kdc: Some("noport".into()),
            ..auth
        };
        assert!(parse_kdc(&bad).is_err());
    }

    #[test]
    fn mtls_unsupported() {
        let auth = AuthConfig {
            mechanism: AuthMechanism::Mtls,
            ..Default::default()
        };
        assert!(UpstreamAuth::from_config(&auth).is_err());
    }
}
