//! kafka-proxy 入口：加载配置 → 启动代理。

use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use tracing_subscriber::EnvFilter;
#[allow(unused_imports)]
use tracing_subscriber::layer::SubscriberExt;

use kafka_proxy::{config::ProxyConfig, run};

/// kafka-proxy：透明 kafka 代理。
#[derive(Debug, Parser)]
#[command(name = "kafka-proxy", version, about = "透明 Kafka 代理")]
struct Cli {
    /// 配置文件路径。
    #[arg(short = 'c', long = "config", default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // 先用最小 stdout logger 初始化，确保配置加载阶段的错误有日志输出。
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!(path = ?cli.config, "加载配置");
    let config = ProxyConfig::load(&cli.config)?;

    // 如果配置了 log_dir，切换到「文件 + stdout」双输出。
    //
    // log_dir 支持绝对路径和相对路径：
    //   - 绝对路径(如 /var/log/kafka-proxy) → 直接使用。
    //   - 相对路径(如 logs 或 ./logs) → 相对于配置文件所在目录解析，
    //     而非进程 CWD(systemd 下 CWD 可能是 /，容易写错位置)。
    if let Some(ref log_dir) = config.log.log_dir {
        // 相对路径则拼接配置文件所在目录，使日志目录与配置文件同级。
        let resolved_dir = if std::path::Path::new(log_dir).is_absolute() {
            log_dir.clone()
        } else {
            let config_dir = cli
                .config
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            config_dir.join(log_dir).to_string_lossy().to_string()
        };

        // 确保日志目录存在。
        if let Err(e) = std::fs::create_dir_all(&resolved_dir) {
            warn!("创建日志目录 {resolved_dir} 失败: {e}，日志仅输出到 stdout");
        } else {
            let appender = tracing_appender::rolling::daily(&resolved_dir, "kafka-proxy.log");
            let (file_writer, guard) = tracing_appender::non_blocking(appender);
            // guard 需保活，否则非阻塞写入会丢日志。
            std::mem::forget(guard);
            let filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log.level));
            let subscriber = tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(file_writer)
                        .with_ansi(false),
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stdout)
                        .with_ansi(true),
                );
            let _ = tracing::subscriber::set_global_default(subscriber);
            info!(log_dir = %resolved_dir, level = %config.log.level, "日志已配置(文件+stdout)");
        }
    }

    info!(
        bootstrap = ?config.cluster.bootstrap_servers,
        mapping = ?config.proxy.bootstrap_server_mapping,
        listen_bind = %config.proxy.listen_bind,
        upstream_mechanism = %config.upstream.auth.mechanism,
        "配置加载完成，启动代理"
    );

    let cancel = CancellationToken::new();
    let cancel_run = cancel.clone();

    // 优雅关闭：收到 SIGTERM/SIGINT 后取消 CancellationToken，
    // 各 accept loop 停止接收新连接，等待在途请求自然排空后退出。
    // k8s 发 SIGTERM，终端 Ctrl-C 发 SIGINT
    tokio::spawn(async move {
        let shutdown = async {
            // 监听 SIGINT(Ctrl-C)
            tokio::signal::ctrl_c().await.ok();
        };
        #[cfg(unix)]
        let shutdown = async {
            use tokio::signal::unix::{SignalKind, signal};
            // SIGTERM 或 SIGINT，先到先触发
            tokio::select! {
                _ = shutdown => {}
                _ = async {
                    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
                    term.recv().await;
                } => {}
            }
        };
        shutdown.await;
        info!("收到关闭信号(SIGTERM/SIGINT)，开始优雅关闭...");
        cancel.cancel();
    });

    run(config, cancel_run).await?;
    info!("优雅关闭完成");
    Ok(())
}
