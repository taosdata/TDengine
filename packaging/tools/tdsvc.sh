#!/bin/bash

# 定义组件列表
components=("taosd" "taosadapter" "taoskeeper" "taos-explorer" "taosx")

# 获取组件的 PID
get_pid() {
  local component=$1
  ps -ef | grep "$component" | grep -v "grep" | awk '{print $2}'
}

# 定义组件启动命令
start_component() {
  local component=$1
  local config_dir=$2

  # 检查是否提供了 config_dir 参数
  if [ -z "$config_dir" ]; then
    echo "Error: config_dir is required to start $component."
    echo "Usage: $0 start <component> <config_dir>"
    exit 1
  fi

  echo "Starting $component with config directory: $config_dir..."
  case $component in
    taosd)
      nohup taosd -c "$config_dir" > /dev/null 2>&1 &
      ;;
    taosadapter)
      nohup taosadapter -c "$config_dir/taosadapter.toml" > /dev/null 2>&1 &
      sleep 5
      ;;
    taoskeeper)
      nohup taoskeeper -c "$config_dir/taoskeeper.toml" > /dev/null 2>&1 &
      ;;
    taos-explorer)
      nohup taos-explorer -C "$config_dir/explorer.toml" > /dev/null 2>&1 &
      ;;
    taosx)
      nohup taosx -c "$config_dir/taosx.toml" > /dev/null 2>&1 &
      ;;
    *)
      echo "Unknown component: $component"
      exit 1
      ;;
  esac
  echo "$component started successfully."
}

# 定义组件停止命令
stop_component() {
  local component=$1
  echo "Stopping $component..."
  local pid=$(get_pid $component)
  if [ -z "$pid" ]; then
    echo "$component is not running."
    return
  fi

  # 发送 SIGTERM 信号
  kill  $pid
  echo "Sent SIGTERM to $component (PID: $pid). Waiting for it to stop..."

  # 等待进程退出
  for i in {1..10}; do
    if ! kill -0 $pid 2>/dev/null; then
      echo "$component stopped successfully."
      return
    fi
    sleep 1
  done

  # 如果进程仍未退出，强制终止
  echo "$component did not stop in time. Sending SIGKILL..."
  kill -9 $pid
  echo "$component was forcefully stopped."
}

# 启动所有组件
start_all() {
  local config_dir=$1

  # 检查是否提供了 config_dir 参数
  if [ -z "$config_dir" ]; then
    echo "Error: config_dir is required to start all components."
    echo "Usage: $0 start-all <config_dir>"
    exit 1
  fi

  echo "Starting all components with config directory: $config_dir..."
  for component in "${components[@]}"; do
    start_component $component "$config_dir"
  done
  echo "All components started successfully."
}

# 停止所有组件
stop_all() {
  echo "Stopping all components..."
  for component in "${components[@]}"; do
    stop_component $component
  done
  echo "All components stopped successfully."
}

# 脚本主逻辑
case $1 in
  start-all)
    start_all $2
    ;;
  stop-all)
    stop_all
    ;;
  start)
    if [ -z "$2" ] || [ -z "$3" ]; then
      echo "Usage: $0 start <component> <config_dir>"
      exit 1
    fi
    start_component $2 $3
    ;;
  stop)
    if [ -z "$2" ]; then
      echo "Usage: $0 stop <component>"
      exit 1
    fi
    stop_component $2
    ;;
  *)
    echo "Usage: $0 {start-all <config_dir>|stop-all|start <component> <config_dir>|stop <component>}"
    exit 1
    ;;
esac