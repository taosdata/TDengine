#!/bin/bash
################################################################################
# QEMU ARM64 支持安装脚本
# 在 amd64 主机上启用 arm64 Docker 容器支持
#
# 用途: 为 arm64 跨平台编译准备环境
################################################################################

set -e

SCRIPT_NAME="$(basename "$0")"
INSTALL_METHOD="${1:-docker}"  # 默认使用 docker 方式

print_header() {
  echo ""
  echo "=================================="
  echo "  $1"
  echo "=================================="
  echo ""
}

print_success() {
  echo "✅ $1"
}

print_info() {
  echo "ℹ️  $1"
}

print_error() {
  echo "❌ $1"
}

################################################################################
# 方式 1: Docker 方式 (推荐)
################################################################################
install_via_docker() {
  print_header "方式 1: Docker 方式安装 QEMU ARM64 支持"
  
  print_info "特点: 快速、简单、无需 root 权限"
  print_info "安装 tonistiigi/binfmt docker 镜像..."
  
  docker run --rm --privileged tonistiigi/binfmt --install arm64
  
  print_success "Docker 方式安装完成"
  verify_qemu
}

################################################################################
# 方式 2: 系统级安装 (持久化)
################################################################################
install_via_system() {
  print_header "方式 2: 系统级安装 QEMU (持久化)"
  
  print_info "需要 sudo 权限。将在系统重启后依然有效"
  
  # 检测操作系统
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
  else
    print_error "无法检测操作系统"
    exit 1
  fi
  
  case "$OS" in
    ubuntu|debian)
      print_info "检测到 Debian/Ubuntu 系统"
      print_info "执行: sudo apt-get install -y qemu-user-static binfmt-support"
      sudo apt-get update
      sudo apt-get install -y qemu-user-static binfmt-support
      sudo systemctl restart systemd-binfmt
      print_success "系统级安装完成"
      ;;
    centos|rhel|fedora)
      print_info "检测到 CentOS/RHEL/Fedora 系统"
      print_info "执行: sudo yum install -y qemu-user-static"
      sudo yum install -y qemu-user-static
      print_success "系统级安装完成"
      ;;
    *)
      print_error "不支持的操作系统: $OS"
      print_info "请手动安装 qemu-user-static 和 binfmt-support"
      exit 1
      ;;
  esac
  
  verify_qemu
}

################################################################################
# 验证 QEMU 安装
################################################################################
verify_qemu() {
  print_header "验证 QEMU ARM64 支持"
  
  echo "检查 1: binfmt_misc 状态"
  if [ -f /proc/sys/fs/binfmt_misc/status ]; then
    status=$(cat /proc/sys/fs/binfmt_misc/status)
    echo "  状态: $status"
    if [ "$status" = "enabled" ]; then
      print_success "binfmt_misc 已启用"
    else
      print_error "binfmt_misc 未启用"
    fi
  else
    print_info "binfmt_misc 不在此路径 (Docker 方式正常表现)"
  fi
  
  echo ""
  echo "检查 2: Docker buildx 平台支持"
  if docker buildx inspect default 2>/dev/null | grep -q "linux/arm64"; then
    print_success "Docker buildx 支持 arm64"
  else
    print_error "Docker buildx 不支持 arm64"
  fi
  
  echo ""
  echo "检查 3: 运行 arm64 容器"
  result=$(docker run --rm --platform linux/arm64 alpine uname -m 2>&1)
  if [ "$result" = "aarch64" ]; then
    print_success "arm64 容器测试通过"
  else
    print_error "arm64 容器测试失败: $result"
    exit 1
  fi
  
  echo ""
  print_header "✅ QEMU 安装验证完成"
}

################################################################################
# 显示用法
################################################################################
show_usage() {
  cat << EOF
用法: $SCRIPT_NAME [方式]

方式:
  docker     - 通过 Docker (tonistiigi/binfmt) 安装 [推荐]
  system     - 系统级安装 (qemu-user-static) [需要 sudo]
  verify     - 仅验证当前 QEMU 状态

示例:
  # 推荐方式: Docker 安装 (不需要 root)
  $ $SCRIPT_NAME docker

  # 持久化: 系统级安装 (需要 root)
  $ $SCRIPT_NAME system

  # 确认状态
  $ $SCRIPT_NAME verify

EOF
}

################################################################################
# 主函数
################################################################################
main() {
  case "$INSTALL_METHOD" in
    docker)
      install_via_docker
      ;;
    system)
      install_via_system
      ;;
    verify)
      verify_qemu
      ;;
    help|-h|--help)
      show_usage
      ;;
    *)
      print_error "未知方式: $INSTALL_METHOD"
      show_usage
      exit 1
      ;;
  esac
}

# 执行主函数
main "$@"
