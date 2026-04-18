# TDengine 演示证书目录

## 说明

此目录用于存放 JDBC 安全演示所需的证书与 TrustStore 文件。

- 仅用于演示/测试环境。
- 不提供自动化脚本，统一使用手工步骤，避免平台差异和系统副作用。
- 生产环境请使用受信任 CA 和独立证书体系，不要复用本示例参数。

---

## 运行前置条件

1. 已安装 `JDK 8+`、`Maven`、`openssl`，并可执行 `keytool`。
2. TDengine Enterprise 已启用 SSL，且客户端能访问 `TDENGINE_HOST:TDENGINE_PORT`（默认 `localhost:6041`）。
3. Nacos 已启动且可访问（默认 `localhost:8848`）。
4. 已准备可用的 TDengine Token（写入 Nacos 的 `tdengine-credential` 配置）。

---

## 步骤 1：生成演示证书与 TrustStore

### 1.1 生成自签名 CA

```bash
# 生成 CA 私钥
openssl genrsa -out certs/ca.key 2048

# 生成 CA 证书（有效期 10 年）
openssl req -new -x509 -key certs/ca.key -out certs/ca.crt -days 3650 \
  -subj "/C=CN/ST=Beijing/L=Beijing/O=TDengine/CN=TDengine-Demo-CA"
```

### 1.2 生成服务端证书（含 SAN）

```bash
# 创建 OpenSSL 配置
cat > certs/openssl.cnf << 'CNF'
[req]
default_bits = 2048
prompt = no
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = TDengine
CN = 127.0.0.1

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
# 如需远程访问，按需追加：
# IP.2 = <SERVER_IP>
# 如果客户端使用域名连接（例如 TDENGINE_HOST=your-tdengine-host），
# 必须把该域名加入 SAN，否则会出现 TLS 握手失败（No subject alternative DNS name matching ... found）。
# DNS.2 = your-tdengine-host
# 只需要保留“实际会被客户端使用”的域名。若当前统一使用 your-tdengine-host，
# 则无需额外保留 td1.internal.company.com。
CNF

# 生成服务端私钥
openssl genrsa -out certs/server.key 2048

# 生成 CSR
openssl req -new -key certs/server.key -out certs/server.csr \
  -config certs/openssl.cnf -extensions v3_req

# 使用 CA 签发服务端证书（有效期 365 天）
openssl x509 -req -days 365 -in certs/server.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/server.crt -extensions v3_req -extfile certs/openssl.cnf

# 清理临时文件
rm -f certs/server.csr
```

### 1.3 创建 Java TrustStore

```bash
keytool -import -alias tdengine-demo \
  -file certs/ca.crt \
  -keystore certs/truststore.jks \
  -storepass changeit \
  -noprompt
```

---

## 步骤 2：准备 Nacos 配置（dataId=tdengine-credential）

```bash
# 可按需覆盖
NACOS_ADDR="${TDENGINE_NACOS_ADDR:-localhost:8848}"
NACOS_USER="${TDENGINE_NACOS_USER:-nacos}"
NACOS_PASSWORD="${TDENGINE_NACOS_PASSWORD:-nacos}"
TOKEN="<YOUR_TDENGINE_TOKEN>"

# 1) 检查 Nacos 服务
curl -sf "http://${NACOS_ADDR}/nacos/v1/ns/instance/list?serviceName=nacos" >/dev/null

# 2) 写入配置
curl -sS -X POST "http://${NACOS_ADDR}/nacos/v1/cs/configs" \
  -d "username=${NACOS_USER}" \
  -d "password=${NACOS_PASSWORD}" \
  -d "dataId=tdengine-credential" \
  -d "group=DEFAULT_GROUP" \
  --data-urlencode "content=token=${TOKEN}"

# 3) 读取确认
curl -sS -G "http://${NACOS_ADDR}/nacos/v1/cs/configs" \
  -d "username=${NACOS_USER}" \
  -d "password=${NACOS_PASSWORD}" \
  --data-urlencode "dataId=tdengine-credential" \
  --data-urlencode "group=DEFAULT_GROUP"
```

预期读取结果包含：`token=<YOUR_TDENGINE_TOKEN>`。

---

## 步骤 3：运行 NacosSecurityDemo

```bash
cd docs/examples/JDBC/JDBCDemo

# 按需设置环境变量（代码通过 System.getenv 读取）
export TDENGINE_HOST="localhost"
export TDENGINE_PORT="6041"
export TDENGINE_DB=""
export TDENGINE_NACOS_ADDR="localhost:8848"
export TDENGINE_NACOS_USER="nacos"
export TDENGINE_NACOS_PASSWORD="nacos"

mvn -q compile exec:java \
  -Dexec.mainClass=com.taos.example.security.NacosSecurityDemo \
  -Djavax.net.ssl.trustStore=certs/truststore.jks \
  -Djavax.net.ssl.trustStorePassword=changeit
```

---

## 证书与连接验证

```bash
# 证书有效期
openssl x509 -in certs/ca.crt -noout -dates
openssl x509 -in certs/server.crt -noout -dates

# 检查证书详情
openssl x509 -in certs/server.crt -noout -text

# SSL 连通性验证（按实际地址替换）
openssl s_client -connect 127.0.0.1:6041 -showcerts

# 域名验证（推荐）：servername 和连接主机名需命中证书 SAN
openssl s_client -connect localhost:6041 -servername localhost -showcerts

# 查看 TrustStore 内容
keytool -list -keystore certs/truststore.jks -storepass changeit
```

---

## 安全注意事项

1. 不要将 `*.key`、`*.crt`、`*.jks` 等产物提交到仓库。
2. 私钥建议权限最小化（如 `chmod 600 certs/server.key certs/ca.key`）。
3. 生产环境建议：
   - 使用企业/公共 CA；
   - 每个环境独立 CA 与证书；
   - 设置合理有效期并执行定期轮换；
   - 使用 KMS/Vault 管理密钥材料。
4. 客户端连接主机名必须与服务端证书 SAN 匹配（DNS 或 IP），否则会报 TLS 握手错误。

---

## 相关文档

- [SSL 配置指南](../../../zh/07-develop/ssl-configuration-guide.md)（服务端证书生成与 taosAdapter SSL 配置）
- [连接器安全最佳实践](../../../zh/07-develop/connector-security-best-practices.md)（客户端 SSL/TLS、Token、轮换策略）
- [TDengine 安全文档](https://docs.taosdata.com/)
