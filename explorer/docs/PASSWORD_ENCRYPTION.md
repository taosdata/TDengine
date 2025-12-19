# TDengine 密码 AES-256-GCM 加密

## 📋 概述

为了增强安全性，taos-explorer 现在使用 AES-256-GCM 算法加密存储在数据库中的 TDengine 密码。这确保即使数据库被泄露，攻击者也无法直接读取密码。

## 🔐 加密实现

### 算法
- **加密算法**: AES-256-GCM (Galois/Counter Mode)
- **密钥长度**: 256 bits (32 bytes)
- **Nonce 长度**: 96 bits (12 bytes)
- **认证**: AEAD (Authenticated Encryption with Associated Data)

### 特性
✅ **随机 Nonce** - 每次加密使用不同的 nonce  
✅ **认证加密** - 防止密文被篡改  
✅ **Base64 编码** - 便于存储在数据库中  
✅ **透明解密** - 应用层自动处理加解密

## ⚙️ 配置

### 1. 生成加密密钥

**推荐方式 (使用 OpenSSL)**:
```bash
# 生成 32 字节随机密钥并 Base64 编码
openssl rand -base64 32
```

输出示例:
```
kX5zYQm8P3vN9Jb2Lc4Rf1Wg7Ht6Ue0Kd9Sx3Qp5Mv2=
```

**或使用 Python**:
```python
import os
import base64
key = os.urandom(32)
print(base64.b64encode(key).decode())
```

**或使用 Rust**:
```rust
use rand::RngCore;
use base64::Engine;

let mut key = [0u8; 32];
rand::thread_rng().fill_bytes(&mut key);
println!("{}", base64::engine::general_purpose::STANDARD.encode(&key));
```

### 2. 配置加密密钥

#### 方式 1: 环境变量（推荐）

```bash
export EXPLORER_SECURITY_ENCRYPTION_KEY="kX5zYQm8P3vN9Jb2Lc4Rf1Wg7Ht6Ue0Kd9Sx3Qp5Mv2="
```

#### 方式 2: 在启动脚本中

```bash
#!/bin/bash
EXPLORER_SECURITY_ENCRYPTION_KEY="kX5zYQm8P3vN9Jb2Lc4Rf1Wg7Ht6Ue0Kd9Sx3Qp5Mv2=" ./taos-explorer
```

#### 方式 3: systemd 服务（生产环境推荐）

编辑 `/etc/systemd/system/taos-explorer.service`:
```ini
[Unit]
Description=TDengine Explorer
After=network.target

[Service]
Type=simple
User=taos
WorkingDirectory=/opt/taos-explorer
Environment="EXPLORER_SECURITY_ENCRYPTION_KEY=kX5zYQm8P3vN9Jb2Lc4Rf1Wg7Ht6Ue0Kd9Sx3Qp5Mv2="
ExecStart=/opt/taos-explorer/taos-explorer
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## 🚨 重要安全提示

### ⚠️ 密钥管理

1. **永远不要**将加密密钥提交到版本控制系统（Git）
2. **永远不要**在代码中硬编码加密密钥
3. **定期轮换**加密密钥（建议每 90-180 天）
4. **安全存储**密钥（使用密钥管理服务如 HashiCorp Vault、AWS KMS 等）
5. **限制访问**只有必要的人员才能访问加密密钥

### ⚠️ 默认密钥警告

如果未设置 `EXPLORER_SECURITY_ENCRYPTION_KEY` 环境变量，系统会使用**默认密钥**：
- ⚠️ 默认密钥是从固定字符串派生的
- ⚠️ **不适合生产环境**
- ⚠️ 所有使用默认密钥的实例都使用相同的密钥
- ⚠️ 启动时会输出警告日志

**默认密钥日志示例**:
```log
[WARN] Using default encryption key - NOT SECURE FOR PRODUCTION!
[WARN] Set EXPLORER_SECURITY_ENCRYPTION_KEY environment variable with a Base64-encoded 32-byte key
```

## 📊 加密流程

### 存储密码时（加密）

```
plaintext_password
    ↓
AES-256-GCM encrypt (with random nonce)
    ↓
[12-byte nonce] + [ciphertext + auth tag]
    ↓
Base64 encode
    ↓
Store in database (oauth_users.tsdb_password)
```

### 读取密码时（解密）

```
Load from database (oauth_users.tsdb_password)
    ↓
Base64 decode
    ↓
Split: nonce (first 12 bytes) + ciphertext
    ↓
AES-256-GCM decrypt (verify auth tag)
    ↓
plaintext_password
```

## 🔧 实现细节

### 修改的文件

**session.rs**:
```rust
// 新增字段
pub struct SessionManager {
    pool: SqlitePool,
    encryption_key: [u8; 32],  // 加密密钥
}

// 新增方法
fn encrypt_password(&self, password: &str) -> Result<String>
fn decrypt_password(&self, encrypted_password: &str) -> Result<String>
pub fn get_decrypted_tsdb_password(&self, session: &OAuthSession) -> Result<Option<String>>
```

**middleware.rs**:
```rust
// 更新密码获取逻辑
let password = session_mgr
    .get_decrypted_tsdb_password(&session)
    .map_err(|e| format!("Failed to decrypt password: {}", e))?
    .ok_or_else(|| "No TDengine password in session".to_string())?;
```

### 加密位置

1. **bind_tsdb_credentials()** - 用户绑定 TDengine 凭据时
2. **create_self_provided_session()** - 创建自提供会话时

### 解密位置

1. **middleware::extract_auth()** - Bearer token 认证时
2. **create_self_provided_session()** - 验证密码是否需要更新时

## 🧪 测试

### 1. 验证加密已启用

```bash
# 启动 explorer
cargo run

# 查看日志，应该看到其中之一:
# ✅ 使用自定义密钥
[INFO] Loaded encryption key from EXPLORER_SECURITY_ENCRYPTION_KEY

# ⚠️ 使用默认密钥（不安全）
[WARN] Using default encryption key - NOT SECURE FOR PRODUCTION!
```

### 2. 验证密码已加密存储

```sql
-- 连接到 SQLite 数据库
sqlite3 /path/to/oauth.db

-- 查看存储的密码（应该是 Base64 编码的密文）
SELECT username, substr(tsdb_password, 1, 20) || '...' as encrypted_password 
FROM oauth_users 
WHERE tsdb_password IS NOT NULL;
```

**正确输出示例**:
```
username       encrypted_password
-------------  ---------------------------
admin          /QCRjlIA7VMUa/trgd0L...
user1          kK8FnX2pL9HsW4vC1zQ...
```

**错误输出示例**（未加密）:
```
username       encrypted_password
-------------  ---------------------------
admin          taosdata...
user1          mypassword...
```

### 3. 测试加密/解密功能

```rust
#[test]
fn test_password_encryption() {
    use crate::utils::aes::{aes_encrypt_base64, aes_decrypt_base64, generate_aes_key};
    
    let key = generate_aes_key();
    let password = "test_password_123";
    
    // 加密
    let encrypted = aes_encrypt_base64(password.as_bytes(), &key).unwrap();
    println!("Encrypted: {}", encrypted);
    
    // 解密
    let decrypted_bytes = aes_decrypt_base64(&encrypted, &key).unwrap();
    let decrypted = String::from_utf8(decrypted_bytes).unwrap();
    
    assert_eq!(password, decrypted);
}
```

### 4. 端到端测试

```bash
# 1. 设置加密密钥
export EXPLORER_SECURITY_ENCRYPTION_KEY=$(openssl rand -base64 32)

# 2. 启动 explorer
cargo run

# 3. OAuth 登录并绑定 TDengine 凭据

# 4. 使用 Bearer token 访问 API
curl -H "Authorization: Bearer YOUR_SESSION_TOKEN" \
     http://localhost:6060/api/-/profile

# 5. 验证可以正常访问（密码自动解密）
```

## 🔄 密钥轮换

### 为什么需要轮换？
- 减少密钥泄露风险
- 符合安全合规要求
- 限制单个密钥的暴露时间

### 轮换步骤

⚠️ **警告**: 轮换密钥会导致所有已存储的密码无法解密！

**方案 1: 双密钥支持（待实现）**
```rust
pub struct SessionManager {
    pool: SqlitePool,
    encryption_key: [u8; 32],       // 当前密钥
    old_encryption_key: Option<[u8; 32]>, // 旧密钥（用于解密）
}
```

**方案 2: 强制重新绑定**
1. 停止服务
2. 备份数据库
3. 清空所有 `tsdb_password`
4. 更新 `EXPLORER_SECURITY_ENCRYPTION_KEY`
5. 启动服务
6. 通知用户重新绑定 TDengine 凭据

```sql
-- 清空所有密码
UPDATE oauth_users SET tsdb_password = NULL;
```

## 📋 故障排查

### Q1: 密码解密失败

**错误日志**:
```log
[ERROR] Failed to decrypt password: Failed to decrypt password: Error
```

**可能原因**:
1. 加密密钥已更改
2. 数据库中的密文已损坏
3. 密码是用旧密钥加密的

**解决方法**:
```sql
-- 检查密码是否为 Base64 格式
SELECT username, length(tsdb_password), tsdb_password 
FROM oauth_users 
WHERE tsdb_password IS NOT NULL;

-- 如果无法恢复，清空密码让用户重新绑定
UPDATE oauth_users SET tsdb_password = NULL WHERE username = 'affected_user';
```

### Q2: 启动时出现警告

**警告日志**:
```log
[WARN] Using default encryption key - NOT SECURE FOR PRODUCTION!
```

**原因**: 未设置 `EXPLORER_SECURITY_ENCRYPTION_KEY` 环境变量

**解决**:
```bash
# 生成新密钥
openssl rand -base64 32

# 设置环境变量
export EXPLORER_SECURITY_ENCRYPTION_KEY="生成的密钥"
```

### Q3: 环境变量不生效

**检查步骤**:
```bash
# 1. 验证环境变量已设置
echo $EXPLORER_SECURITY_ENCRYPTION_KEY

# 2. 验证 Base64 格式
echo $EXPLORER_SECURITY_ENCRYPTION_KEY | base64 -d | wc -c
# 应该输出: 32

# 3. 检查服务启动方式
ps aux | grep taos-explorer

# 4. 如果使用 systemd，检查环境变量
systemctl show taos-explorer | grep EXPLORER_SECURITY_ENCRYPTION_KEY
```

## 🎯 最佳实践

### 开发环境
- ✅ 可以使用默认密钥
- ✅ 密钥可以共享（team keyring）
- ✅ 可以将密钥放在 `.env` 文件（不要提交到 Git）

### 生产环境
- ✅ **必须**使用自定义密钥
- ✅ 使用密钥管理服务（KMS）
- ✅ 限制密钥访问权限
- ✅ 定期轮换密钥（90-180天）
- ✅ 启用审计日志
- ✅ 加密备份数据库

### Docker 部署
```dockerfile
FROM rust:latest
WORKDIR /app
COPY . .
RUN cargo build --release

# 不要在 Dockerfile 中硬编码密钥！
# 使用运行时环境变量
ENV EXPLORER_SECURITY_ENCRYPTION_KEY=""

CMD ["./target/release/taos-explorer"]
```

**docker-compose.yml**:
```yaml
version: '3.8'
services:
  taos-explorer:
    build: .
    environment:
      - EXPLORER_SECURITY_ENCRYPTION_KEY=${EXPLORER_SECURITY_ENCRYPTION_KEY}
    env_file:
      - .env  # 不要提交 .env 到 Git
```

### Kubernetes 部署
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: taos-explorer-secrets
type: Opaque
data:
  encryption-key: a1g1elhRbThQM3ZOOUpiMkxjNFJmMVdn...  # Base64编码

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: taos-explorer
spec:
  template:
    spec:
      containers:
      - name: taos-explorer
        image: taos-explorer:latest
        env:
        - name: EXPLORER_SECURITY_ENCRYPTION_KEY
          valueFrom:
            secretKeyRef:
              name: taos-explorer-secrets
              key: encryption-key
```

## 📚 相关资源

- [AES-GCM Wikipedia](https://en.wikipedia.org/wiki/Galois/Counter_Mode)
- [NIST SP 800-38D](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf)
- [aes-gcm crate documentation](https://docs.rs/aes-gcm/)
- [OAUTH_IMPLEMENTATION.md](./OAUTH_IMPLEMENTATION.md)

## 📝 更新日志

### v1.2.0 (2024-12-08)
- ✅ 实现 AES-256-GCM 密码加密
- ✅ 支持环境变量配置密钥
- ✅ 自动加密/解密密码
- ✅ 添加默认密钥（仅开发环境）
- ✅ 完整的错误处理和日志记录
