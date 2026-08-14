//! kafka-proxy 入口：加载配置 → 启动代理。

use std::path::PathBuf;

use std::fmt;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{Event, Level, Subscriber, info, warn};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::{
    FmtContext,
    format::{FormatEvent, FormatFields, Writer},
    time::{FormatTime, SystemTime},
};
#[allow(unused_imports)]
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

use kafka_proxy::{config::ProxyConfig, run};

/// 自定义日志格式：保留默认 UTC 时间戳与 level，并在消息前打印 `<target:file:line>` 源码位置，
/// 便于定位日志输出点。时间戳仍由 `SystemTime`（UTC ISO8601）生成，日期格式不变。
#[derive(Clone, Copy, Default)]
struct LocationFormat;

impl<S, N> FormatEvent<S, N> for LocationFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let ansi = writer.has_ansi_escapes();
        let meta = event.metadata();
        let level = meta.level();

        // 时间戳：复用 SystemTime（UTC ISO8601），保持原有日期格式不变。
        SystemTime.format_time(&mut writer)?;
        writer.write_char(' ')?;

        // level（stdout 带 ANSI 颜色，文件输出为纯文本），配色与 tracing-subscriber 默认一致。
        let (lvl_str, code) = match *level {
            Level::TRACE => ("TRACE", "35"),
            Level::DEBUG => ("DEBUG", "34"),
            Level::INFO => (" INFO", "32"),
            Level::WARN => (" WARN", "33"),
            Level::ERROR => ("ERROR", "31"),
        };
        if ansi {
            write!(writer, "\x1b[{code}m{lvl_str}\x1b[0m")?;
        } else {
            write!(writer, "{lvl_str}")?;
        }

        // 源码位置 <target:file:line>，置于消息之前，便于直接定位日志输出点。
        // 用 `meta.file()`（file!() 宏结果）区分 main.rs / lib.rs / metrics.rs 等文件：
        // 本项目 target 恒为 crate 根 `kafka_proxy`，单凭 target 无法定位到具体文件。
        let target = meta.target();
        let file = meta.file().unwrap_or(target);
        let loc = match meta.line() {
            Some(line) => format!("{target}:{file}:{line}"),
            None => format!("{target}:{file}"),
        };
        if ansi {
            write!(writer, " \x1b[2m<{loc}>\x1b[0m ")?;
        } else {
            write!(writer, " <{loc}> ")?;
        }
        // （上方的 level 与此处 <loc> 之间已含一个空格，整体为单空格分隔）

        // 事件字段（含 message），委托给内置字段格式化器，保持原有输出语义。
        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

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
        .event_format(LocationFormat)
        .init();

    info!(path = ?cli.config, "加载配置");
    let config = ProxyConfig::load(&cli.config)?;

    // 如果配置了 log_dir，切换到「文件 + stdout」双输出。
    //
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
                        .with_ansi(false)
                        .event_format(LocationFormat),
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stdout)
                        .with_ansi(true)
                        .event_format(LocationFormat),
                );
            let _ = tracing::subscriber::set_global_default(subscriber);
            // 明确打印实际日志文件完整路径，便于排查日志去向。
            let log_file = std::path::Path::new(&resolved_dir).join("kafka-proxy.log");
            info!(log_file = %log_file.display(), level = %config.log.level, "日志将输出到: 文件 + stdout");
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
