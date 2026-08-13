#!/usr/bin/env bash
#
# kafka-proxy 安装脚本：解压安装二进制 + 配置，注册为 systemd 系统服务。
# 二进制注册为可通过 systemctl 管理的系统服务。
#
# 用法:
#   sudo ./install.sh                          # 从同目录的 tar.gz 安装
#   sudo ./install.sh kafka-proxy-v0.1.0-x86_64.tar.gz  # 指定安装包
#   sudo ./install.sh /tmp/kafka-proxy-*.tar.gz         # 指定路径
#
# 安装后:
#   systemctl start kafka-proxy
#   systemctl status kafka-proxy
#   systemctl enable kafka-proxy    # 开机自启
#   journalctl -u kafka-proxy -f    # 查看日志
#
set -euo pipefail

# ---- 配置常量 ----
INSTALL_DIR="/opt/kafka-proxy"
CONFIG_DIR="/etc/kafka-proxy"
LOG_DIR="/var/log/kafka-proxy"
SERVICE_FILE="/etc/systemd/system/kafka-proxy.service"
BIN_NAME="kafka-proxy"

# ---- 颜色输出 ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---- 检查 root 权限 ----
if [[ $EUID -ne 0 ]]; then
    error "此脚本需要 root 权限运行，请使用 sudo"
    exit 1
fi

# ---- 定位安装包 ----
PKG="${1:-}"
if [[ -z "$PKG" ]]; then
    # 自动查找当前目录下的 tar.gz
    PKG=$(ls kafka-proxy-*.tar.gz 2>/dev/null | head -1)
    if [[ -z "$PKG" ]]; then
        error "未找到安装包。用法: sudo $0 <kafka-proxy-xxx.tar.gz>"
        exit 1
    fi
fi

if [[ ! -f "$PKG" ]]; then
    error "安装包不存在: $PKG"
    exit 1
fi

info "使用安装包: $PKG"

# ---- 解压到临时目录 ----
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

info "解压中..."
tar xzf "$PKG" -C "$TMP_DIR"

# 找到解压后的目录(通常为 kafka-proxy-<version>-<arch>)
EXTRACTED=$(find "$TMP_DIR" -name "$BIN_NAME" -type f -print -quit)
if [[ -z "$EXTRACTED" ]]; then
    error "解压后未找到 $BIN_NAME 可执行文件"
    exit 1
fi
SRC_DIR=$(dirname "$EXTRACTED")
info "解压目录: $SRC_DIR"

# ---- 创建安装目录 ----
mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$LOG_DIR"

# ---- 安装二进制 ----
info "安装二进制到 $INSTALL_DIR/"
install -m 0755 "$SRC_DIR/$BIN_NAME" "$INSTALL_DIR/$BIN_NAME"

# ---- 安装配置文件(不覆盖已有配置) ----
if [[ -f "$CONFIG_DIR/config.toml" ]]; then
    warn "配置文件已存在 $CONFIG_DIR/config.toml，保留不覆盖"
else
    if [[ -f "$SRC_DIR/kafka-proxy.toml.example" ]]; then
        install -m 0644 "$SRC_DIR/kafka-proxy.toml.example" "$CONFIG_DIR/config.toml"
        info "安装默认配置到 $CONFIG_DIR/config.toml"
    else
        warn "安装包内无配置示例，请手动创建 $CONFIG_DIR/config.toml"
    fi
fi

# ---- 设置日志目录权限 ----
chmod 0755 "$LOG_DIR"
info "日志目录: $LOG_DIR"

# ---- 创建 systemd service 文件 ----
info "注册 systemd 服务: $SERVICE_FILE"
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Transparent Kafka Proxy
Documentation=https://github.com/sixinyiyu/kafka-proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/${BIN_NAME} -c ${CONFIG_DIR}/config.toml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
# 日志由 systemd 捕获(journalctl -u kafka-proxy 查看)
# 若 config.toml 配了 [log].log_dir，则同时写文件到该目录
StandardOutput=journal
StandardError=journal
# 运行用户(可改)
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

chmod 0644 "$SERVICE_FILE"

# ---- 重载 systemd ----
systemctl daemon-reload
info "systemd 已重载"

# ---- 提示后续操作 ----
echo ""
echo -e "${GREEN}========== 安装完成 ==========${NC}"
echo ""
echo "二进制路径:   $INSTALL_DIR/$BIN_NAME"
echo "配置文件:     $CONFIG_DIR/config.toml"
echo "日志目录:     $LOG_DIR"
echo "服务文件:     $SERVICE_FILE"
echo ""
echo -e "${YELLOW}请先编辑配置文件:${NC}"
echo "  sudo vi $CONFIG_DIR/config.toml"
echo ""
echo "然后启动服务:"
echo "  sudo systemctl start kafka-proxy"
echo "  sudo systemctl status kafka-proxy"
echo "  sudo systemctl enable kafka-proxy   # 开机自启"
echo ""
echo "查看日志:"
echo "  journalctl -u kafka-proxy -f"
echo ""
echo "停止/重启:"
echo "  sudo systemctl stop kafka-proxy"
echo "  sudo systemctl restart kafka-proxy"
echo ""
echo "卸载:"
echo "  sudo systemctl stop kafka-proxy && sudo systemctl disable kafka-proxy"
echo "  sudo rm $SERVICE_FILE && sudo rm -rf $INSTALL_DIR $CONFIG_DIR"
