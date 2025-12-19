#!/bin/bash
# setup-oidc.sh

REALM=${REALM:-test}
CLIENT_ID=client1
BASEURL=http://localhost:6060
USERNAME=test
PASSWORD=taosdata

# 等待 Keycloak 启动
echo "等待 Keycloak 启动..."
until curl -f -s http://localhost:8080/health/ready; do
    sleep 10
done

echo "Keycloak 已启动，开始配置 OIDC..."

# 获取管理员访问令牌
ACCESS_TOKEN=$(curl -s -X POST \
  http://localhost:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123&grant_type=password&client_id=admin-cli" \
  | jq -r '.access_token')

if [ "$ACCESS_TOKEN" = "null" ]; then
    echo "❌ 无法获取管理员令牌"
    exit 1
fi

echo "✅ 成功获取管理员令牌"

# 创建 Realm
curl -X POST "http://localhost:8080/admin/realms" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "realm": "'$REALM'",
    "enabled": true,
    "displayName": "TDengine Test Ream",
    "loginWithEmailAllowed": true,
    "duplicateEmailsAllowed": false,
    "resetPasswordAllowed": true,
    "editUsernameAllowed": false,
    "bruteForceProtected": true
  }'

echo "✅ Realm '$REALM' 创建成功"

# 创建 OIDC Client
curl -X POST "http://localhost:8080/admin/realms/$REALM/clients" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "clientId": "'$CLIENT_ID'",
    "enabled": true,
    "protocol": "openid-connect",
    "publicClient": true,
    "standardFlowEnabled": true,
    "implicitFlowEnabled": false,
    "directAccessGrantsEnabled": true,
    "rootUrl": "'$BASEURL'",
    "redirectUris": [
      "'$BASEURL'/*",
      "'$BASEURL'/api/-/callback"
    ],
    "webOrigins": ["'$BASEURL'"],
    "attributes": {
      "backchannel.logout.session.required": "true",
      "backchannel.logout.revoke.offline.tokens": "false"
    }
  }'

echo "✅ Public Client '$CLIENT_ID' 创建成功"

# 创建 Confidential Client（用于服务端应用）
curl -X POST "http://localhost:8080/admin/realms/$REALM/clients" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "clientId": "'$CLIENT_ID'-server",
    "enabled": true,
    "protocol": "openid-connect",
    "publicClient": false,
    "secret": "my-secret-key-123",
    "standardFlowEnabled": true,
    "implicitFlowEnabled": false,
    "directAccessGrantsEnabled": true,
    "serviceAccountsEnabled": true,
    "redirectUris": [
      "http://localhost:8081/*"
    ],
    "attributes": {
      "client_credentials.use_refresh_token": "false"
    }
  }'

echo "✅ Confidential Client '$CLIENT_ID-server' 创建成功"

# 创建用户
curl -X POST "http://localhost:8080/admin/realms/$REALM/users" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "'$USERNAME'",
    "email": "test@example.com",
    "firstName": "Test",
    "lastName": "User",
    "enabled": true,
    "credentials": [
      {
        "type": "password",
        "value": "'$PASSWORD'",
        "temporary": false
      }
    ]
  }'

echo "✅ 测试用户创建成功: $USERNAME:$PASSWORD"

echo ""
echo "🎉 OIDC 配置完成!"
echo "Realm: $REALM"
echo "管理控制台: http://localhost:8080/admin"
echo "OIDC 发现端点: http://localhost:8080/realms/$REALM/.well-known/openid-configuration"
