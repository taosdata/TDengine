# TDgpt SSL/TLS 支持

本文档描述如何为 tdgpt 服务启用 SSL/TLS 加密。

## 概述

tdgpt 支持两种 SSL 部署方式：

| 方案 | 场景 | 实现方式 |
|------|------|---------|
| **方案 A：Gunicorn 配置** | 生产环境 | 在 `taosanode.config.py` 中配置证书路径 |
| **方案 C：Flask 开发** | 开发/测试 | 通过命令行参数指定证书，或在配置文件中设置 |

---

## 方案 A：生产环境（Gunicorn）

### 1. 准备 SSL 证书和密钥

```bash
# 使用现有证书（推荐）
# 确保 certfile（证书）和 keyfile（私钥）文件存在

# 如果需要自签名证书用于测试
openssl req -x509 -newkey rsa:4096 -nodes \
  -out /etc/taos/taosanode/certs/server.crt \
  -keyout /etc/taos/taosanode/certs/server.key \
  -days 365
```

### 2. 配置 taosanode.config.py

编辑 `/etc/taos/taosanode.config.py`（或对应的配置文件路径）：

```python
# 启用 SSL/TLS（取消注释并填入证书路径）
certfile = '/etc/taos/taosanode/certs/server.crt'
keyfile = '/etc/taos/taosanode/certs/server.key'

# 可选：指定 TLS 版本
ssl_version = 'TLSv1_2'

# 可选：指定加密套件（更安全的配置）
ciphers = 'ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL'
```

### 3. 启动服务

```bash
# 使用 gunicorn 启动（推荐生产）
gunicorn -c /etc/taos/taosanode.config.py taosanalytics.app:app

# 或使用系统服务（如已配置）
systemctl restart tdgpt
```

### 4. 验证 HTTPS

```bash
# 测试 HTTPS 连接
curl --cacert /path/to/ca.crt https://your-server:6035/status

# 或使用自签名证书（跳过验证，仅测试）
curl -k https://your-server:6035/status
```

---

## 方案 C：开发/测试环境（Flask）

### 1. 准备证书

为了快速测试，生成自签名证书：

```bash
# 生成 4096 位 RSA 自签名证书，有效期 365 天
openssl req -x509 -newkey rsa:4096 -nodes \
  -out cert.pem -keyout key.pem -days 365

# 或更简洁（跳过交互）
openssl req -x509 -newkey rsa:4096 -nodes -days 365 \
  -out cert.pem -keyout key.pem \
  -subj "/C=CN/ST=State/L=City/O=Org/CN=localhost"
```

### 2. 通过命令行参数启动

```bash
# 方式 1：直接指定证书文件
python -m taosanalytics.app --cert cert.pem --key key.pem

# 方式 2：使用配置文件
# 编辑 taosanode.config.py，添加：
# certfile = '/path/to/cert.pem'
# keyfile = '/path/to/key.pem'
python -m taosanalytics.app -c taosanode.config.py
```

### 3. 测试 HTTPS 连接

```bash
# 跳过证书验证（用于自签名证书）
curl -k https://localhost:6035/status

# 如果使用了有效的证书
curl --cacert cert.pem https://localhost:6035/status

# 使用 Python 测试脚本
python tests/test_ssl.py

# 或手动使用 Python requests
python -c "
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
print(requests.get('https://localhost:6035/status', verify=False).json())
"
```

---

## 证书优先级

对于开发/测试模式（Flask），证书优先级如下（从高到低）：

1. **命令行参数**：`--cert` 和 `--key`（优先级最高）
2. **配置文件**：`taosanode.config.py` 中的 `certfile` 和 `keyfile`
3. **默认值**：不使用 SSL，以 HTTP 运行

---

## 常见问题

### Q: 如何验证 SSL 证书的正确性？

```bash
# 查看证书信息
openssl x509 -in cert.pem -text -noout

# 验证私钥和证书匹配
openssl x509 -noout -modulus -in cert.pem | openssl md5
openssl rsa -noout -modulus -in key.pem | openssl md5
# 两个 MD5 哈希应该相同
```

### Q: 生产环境能否使用自签名证书？

**不建议**。自签名证书只适合测试。生产环境应使用由受信任 CA 签发的证书。可从以下渠道获取：
- Let's Encrypt（免费）
- 商业 CA（付费）
- 内部企业 CA

### Q: 如何在 Docker 中使用 SSL？

```dockerfile
FROM python:3.9

# ... 安装依赖 ...

# 将证书复制到容器
COPY cert.pem /etc/certs/server.crt
COPY key.pem /etc/certs/server.key

# 设置环境变量或在配置文件中指定路径
ENV TDGPT_CERTFILE=/etc/certs/server.crt
ENV TDGPT_KEYFILE=/etc/certs/server.key

# 或在配置文件中修改路径
CMD ["gunicorn", "-c", "/etc/taos/taosanode.config.py", "taosanalytics.app:app"]
```

### Q: 如何支持自定义 TLS 版本或加密套件？

在 `taosanode.config.py` 中设置（仅 gunicorn 支持）：

```python
# TLS 版本
ssl_version = 'TLSv1_3'  # 推荐

# 加密套件（优先级顺序）
ciphers = 'ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES256-GCM-SHA384'
```

Flask 开发服务器使用 Python 默认 SSL 配置，无法自定义。

---

## 迁移指南

### 从 HTTP 迁移到 HTTPS

1. **准备阶段**
   - 获取或生成 SSL 证书
   - 在测试环境验证证书和密钥的有效性

2. **测试阶段**
   - 使用 Flask 开发模式测试 HTTPS 连接
   - 验证所有 API 端点工作正常

3. **部署阶段**
   - 在 `taosanode.config.py` 中配置证书路径
   - 使用 gunicorn 启动服务
   - 监控日志，验证 HTTPS 连接成功

4. **验证阶段**
   ```bash
   # 检查日志中是否有 SSL 相关错误
   tail -f /var/log/taos/taosanode/error.log
   
   # 测试 HTTPS 连接
   curl -k https://your-server:6035/status
   ```

---

## 安全建议

1. **证书管理**
   - 确保密钥文件权限正确：`chmod 600 key.pem`
   - 定期更新证书（有效期内）
   - 备份证书和密钥

2. **TLS 版本**
   - 使用 TLS 1.2 或 1.3（禁用 SSL 3.0、TLS 1.0、1.1）
   - 在 `taosanode.config.py` 中明确指定 `ssl_version`

3. **加密套件**
   - 仅启用强加密算法
   - 定期更新，跟进安全建议

4. **证书有效期**
   - 监控证书过期时间
   - 提前 30 天更新证书
