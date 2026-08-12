//! kafka-proxy 入口：加载配置 → 启动代理。

use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    info!(path = ?cli.config, "加载配置");
    let config = ProxyConfig::load(&cli.config)?;
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
