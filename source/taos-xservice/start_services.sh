#!/bin/bash

if [ $# -eq 0 ]; then
        echo "Error: No arguments provided."
        echo "Please try $0 --help"
        exit 1
fi

for arg in "$@"; do
    case $arg in
            --agent_name=*) agent_name="${1#*=}"; shift ;;
            *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
done

    
# set -x
systemctl stop taosx
systemctl stop taosx-agent
systemctl stop taos-explorer
systemctl start taosx
systemctl start taos-explorer
sleep 5

# 后续考虑先获取cluster_id，再获取token，用于在explorer中可以展示
response0=$(curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
                    http://localhost:6041/rest/sql -d "show cluster;")
# 打印 cluster_id 以进行调试
echo "response0: $response0"
cluster_id=$(echo $response0| sed -n 's/.*"data":\[\[\([^]]*\)\].*/\1/p' | awk -F, '{print $1}')
echo "cluster_id: $cluster_id"

# 发送 POST 请求并将返回结果存储在变量中
response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST http://localhost:6060/api/x/agents \
     -H "Content-Type: application/json" \
     -d "{\"cluster_id\":\"$cluster_id\",\"dsn\":\"http://localhost:6041\",\"name\":\"$agent_name\",\"user_id\":\"root\"}")

echo "response: $response"
# 使用 jq 解析 JSON 响应并提取 token 字段
linux_agent_token=$(echo "$response" | sed -e 's/HTTP_STATUS\:.*//g' | jq -r '.token')
linux_agent_id=$(echo "$response" | sed -e 's/HTTP_STATUS\:.*//g' | jq -r '.id')
# 定义 agent.toml 文件路径
agent_toml_path="/etc/taos/agent.toml"

# 检查 agent.toml 文件是否存在，如果不存在则新建
if [ ! -f "$agent_toml_path" ]; then
    touch "$agent_toml_path"

else
    rm -rf "$agent_toml_path"
    touch "$agent_toml_path"
fi
# 将token和endpoint写入agent.toml文件
temp_file=$(mktemp)
echo "token=\"$linux_agent_token\"
      endpoint=\"http://localhost:6055\"" > "$temp_file"
cat "$agent_toml_path" >> "$temp_file"
mv "$temp_file" "$agent_toml_path"

echo "linux agent token: $linux_agent_token"

# 将linux agent id写入setenv.sh.example文件
setenv_path="tests/e2e/setenv.sh.example"
sed -i '/export TAOSX_LINUX_AGENT_ID/c\export TAOSX_LINUX_AGENT_ID='$linux_agent_id'' $setenv_path


systemctl start taosx-agent
