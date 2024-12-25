#!/bin/bash


systemctl start taosx
systemctl start taos-explorer

# 后续考虑先获取cluster_id，再获取token，用于在explorer中可以展示

# 发送 POST 请求并将返回结果存储在变量中
response=$(curl -s -X POST http://localhost:6060/api/x/agents \
     -H "Content-Type: application/json" \
     -d '{"dsn":"http://localhost:6041","name":"linux-agent","user_id":"root"}')

# 使用 jq 解析 JSON 响应并提取 token 字段
linux_agent_token=$(echo $response | jq -r '.token')
linux_agent_id=$(echo $response | jq -r '.id')
export TAOSX_LINUX_AGENT_TOKEN=$linux_agent_token
export TAOSX_LINUX_AGENT_ID=$linux_agent_id
# 定义 agent.toml 文件路径
agent_toml_path="/etc/taos/agent.toml"

# 检查 agent.toml 文件是否存在，如果不存在则新建
if [ ! -f "$agent_toml_path" ]; then
    touch "$agent_toml_path"

else
    rm -rf "$agent_toml_path"
    touch "$agent_toml_path"
fi

temp_file=$(mktemp)
echo "token = \"$linux_agent_token\" 
      endpoint = \"http://localhost:6055\"" > "$temp_file"
cat "$agent_toml_path" >> "$temp_file"
mv "$temp_file" "$agent_toml_path"

echo "linux agent token: $linux_agent_token"

systemctl start taosx-agent