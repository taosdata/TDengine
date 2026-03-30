# Explorer SQL 传输层混淆 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-03 | 2026-03-03 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 背景

当前 taos-explorer 的 `/api/-/rest/sql` endpoint 使用明文传输 SQL 语句。在某些安全敏感的场景下，即使使用了 HTTPS，客户端和服务端之间仍希望对 SQL 语句内容进行额外的混淆保护，防止中间代理或日志系统直接记录敏感的 SQL 语句内容。
本特性旨在提供一个 XOR 加密传输机制，通过 HTTP Header 控制是否启用，在不破坏现有行为的前提下，为有需要的用户提供额外的传输层混淆保护。

## 3. 定义

- **XOR 加密**：使用异或（XOR）运算对数据进行简单的可逆混淆，本特性中用于对 SQL 文本进行传输层混淆
- **X-Enable-Xor**：新增的 HTTP 请求头，用于控制是否启用 XOR 加密传输
- **向后兼容**：不带 `X-Enable-Xor` header 的请求保持原有明文传输行为

## 4. 行为说明

### 4.1 API 变更

#### 4.1.1 Endpoint

`POST /api/-/rest/sql`

#### 4.1.2 新增 HTTP Header

**请求头：X-Enable-Xor**
- **类型**：可选
- **值**：
  - `true` 或 `1`：启用 XOR 加密
  - 不存在或其他值：使用明文传输（向后兼容）

#### 4.1.3 请求行为

##### 4.1.3.1 场景 1：不带 X-Enable-Xor header（向后兼容）

```bash
curl -X POST http://localhost:6060/api/-/rest/sql \
  -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
  -d "SELECT * FROM test.meters LIMIT 10"
```

**行为**：SQL 语句以明文形式在请求体中传输，服务端直接解析执行。

##### 4.1.3.2 场景 2：带 X-Enable-Xor header

```bash
curl -X POST http://localhost:6060/api/-/rest/sql \
  -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
  -H "X-Enable-Xor: true" \
  -d "<XOR_ENCRYPTED_SQL_BASE64>"
```

**行为**：
1. 客户端将 SQL 语句进行 XOR 加密，然后 Base64 编码后放入请求体
2. 服务端检测到 `X-Enable-Xor` header，对请求体进行 Base64 解码和 XOR 解密
3. 解密后得到原始 SQL 语句，正常执行

#### 4.1.4 XOR 加密算法规范

**加密过程**：
```plaintext
1. 定义固定密钥（key）：使用预定义的字节序列作为密钥
2. 对 SQL 文本的每个字节与密钥进行循环 XOR 运算
3. 将加密后的字节序列进行 Base64 编码
```

**密钥定义**：
- 默认密钥：`"TDengine-Explorer-XOR-Key-2026"` 的 UTF-8 字节序列
- 密钥长度：29 字节
- 循环使用：当 SQL 长度超过密钥长度时，密钥循环使用
**伪代码**：
```rust
fn xor_encrypt(sql: &str, key: &[u8]) -> Vec<u8> {
    sql.as_bytes()
        .iter()
        .enumerate()
        .map(|(i, &byte)| byte ^ key[i % key.len()])
        .collect()
}

fn xor_decrypt(encrypted: &[u8], key: &[u8]) -> String {
    let decrypted: Vec<u8> = encrypted
        .iter()
        .enumerate()
        .map(|(i, &byte)| byte ^ key[i % key.len()])
        .collect();
    String::from_utf8(decrypted).unwrap()
}
```

#### 4.1.5 响应行为

响应格式保持不变，不受 `X-Enable-Xor` header 影响。响应内容仍为标准的 JSON 格式。

#### 4.1.6 错误处理

| 错误场景 | HTTP 状态码 | 错误码 | 错误信息 |
| --- | --- | --- | --- |
| Base64 解码失败 | 400 | 0x2701 | "Invalid Base64 encoding in request body" |
| XOR 解密后非法 UTF-8 | 400 | 0x2702 | "Decrypted content is not valid UTF-8" |
| 解密后 SQL 为空 | 400 | 0x2703 | "Decrypted SQL is empty" |
| 其他 SQL 执行错误 | 按现有逻辑 | 按现有逻辑 | 按现有逻辑 |

#### 4.1.7 配置参数

无新增配置参数。XOR 密钥硬编码在代码中，未来如有需要可扩展为可配置。

## 5. 性能

**性能影响评估**：
- **加密开销**：XOR 运算是最简单的位运算，性能开销极小（< 1% CPU）
- **Base64 编解码**：标准库实现，对于典型 SQL 语句（< 10KB）开销可忽略
- **内存开销**：需要额外的临时缓冲区存储解密后的 SQL，对于单个请求影响可忽略
- **网络传输**：Base64 编码会增加约 33% 的传输体积，对于小型 SQL 语句影响不大
**结论**：对于典型的 SQL 查询场景（SQL 长度 < 10KB），性能影响可忽略不计。对于超大 SQL 语句（> 100KB），可能会有轻微的延迟增加（< 10ms）。

## 6. 安全

### 6.1 安全设计

1. **混淆而非加密**：XOR 不是安全的加密算法，本特性仅提供传输层混淆，防止明文日志记录
2. **密钥管理**：密钥硬编码在代码中，不提供运行时配置，避免密钥泄露风险
3. **HTTPS 依赖**：本特性不能替代 HTTPS，仍需要在生产环境使用 HTTPS 保证传输安全
4. **向后兼容**：不强制启用，用户可根据安全需求选择是否使用

### 6.2 安全考量

- **防重放攻击**：使用基于时间戳的 XOR 加密算法，防超时重放攻击。
- **不防中间人攻击**：必须配合 HTTPS 使用
- **动态密钥**：使用基于时间戳的动态密钥
- **适用场景**：仅适用于需要防止日志明文记录的场景，不适用于高安全要求场景

## 7. 兼容性

**向后兼容**：完全兼容现有行为。
- 不带 `X-Enable-Xor` header 的请求行为完全不变
- 现有客户端无需修改即可继续使用
- 新客户端直接启用 XOR 加密
**无破坏性变更**。

## 8. 运维

无特殊部署要求，随 taos-explorer 正常升级即可。

## 9. 使用场景

### 9.1 场景 1：防止代理日志记录敏感 SQL

**背景**：企业内部有 HTTP 代理服务器记录所有 HTTP 请求日志，包括请求体内容。
**需求**：防止敏感的 SQL 语句（如包含业务逻辑的复杂查询）被代理日志明文记录。
**方案**：客户端启用 `X-Enable-Xor` header，SQL 语句经过 XOR 混淆后传输，代理日志只能看到 Base64 编码的密文。

### 9.2 场景 2：审计日志脱敏

**背景**：需要记录所有 API 请求用于审计，但不希望审计日志中包含明文 SQL。
**需求**：审计系统记录请求元数据（时间、用户、IP），但 SQL 内容以混淆形式存储。
**方案**：启用 XOR 加密后，审计日志中的 SQL 内容为密文，需要专门的解密工具才能查看。

### 9.3 场景 3：开发测试环境

**背景**：开发测试环境可能不使用 HTTPS，但仍希望对 SQL 进行基本混淆。
**需求**：在不部署 HTTPS 的情况下，提供基本的传输混淆。
**方案**：使用 XOR 加密提供轻量级混淆，降低明文暴露风险。

## 10. 约束和限制

### 10.1 约束

- 必须使用 UTF-8 编码的 SQL 语句
- 客户端和服务端必须使用相同的 XOR 密钥
- 建议配合 HTTPS 使用，不应作为唯一的安全措施

### 10.2 限制

- XOR 加密不提供真正的安全保护，仅用于混淆
- 不支持自定义密钥，所有客户端使用相同的硬编码密钥
- Base64 编码会增加约 33% 的传输体积
- 不支持流式传输，必须一次性加密整个 SQL 语句

## 11. 常见错误和排查

### 11.1 错误 1：Base64 解码失败

**错误信息**：`Invalid Base64 encoding in request body`
**原因**：请求体不是有效的 Base64 编码字符串
**排查**：
1. 检查客户端是否正确进行了 Base64 编码
2. 检查请求体是否被中间件修改（如自动解压缩）
3. 使用 `echo <body> | base64 -d` 验证 Base64 编码是否有效

### 11.2 错误 2：解密后非法 UTF-8

**错误信息**：`Decrypted content is not valid UTF-8`
**原因**：XOR 密钥不匹配，或加密过程有误
**排查**：
1. 确认客户端和服务端使用相同的密钥字符串
2. 确认密钥编码为 UTF-8
3. 检查加密逻辑是否正确实现（循环 XOR）

### 11.3 错误 3：解密后 SQL 为空

**错误信息**：`Decrypted SQL is empty`
**原因**：加密前的 SQL 为空，或解密逻辑错误
**排查**：
1. 检查客户端是否传入了空字符串
2. 验证加密和解密逻辑的对称性

## 12. 可观测性

### 12.1 taos-explorer UI 影响

此版本之后，前端在发送 SQL 请求时自动添加 `X-Enable-Xor: true` header 并对 SQL 进行加密。

### 12.2 日志可观测性

服务端日志应记录：
- 每个请求是否启用了 XOR 加密
- XOR 解密失败的详细错误信息
- 解密后的 SQL 长度（用于性能分析）

## 13. 安装和卸载

无特殊安装要求。本特性作为 taos-explorer 的一部分，随正常安装流程部署。
卸载时无需特殊处理，删除 taos-explorer 即可。

## 14. 文档

无用户感知变更，不修改文档。

## 15. 参考文档

- [XOR Cipher - Wikipedia](https://en.wikipedia.org/wiki/XOR_cipher)
- [Base64 Encoding - RFC 4648](https://tools.ietf.org/html/rfc4648)
- taos-explorer REST API 现有文档

## 16. 附录

### 16.1 实现要点

#### 16.1.1 服务端实现（Rust）

```rust
// 在 taos-explorer 的 REST API handler 中添加
async fn handle_sql_request(
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let sql = if headers.get("X-Enable-Xor")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
    {
        // XOR 解密逻辑
        let key = b"TDengine-Explorer-XOR-Key-2026";
        let decoded = general_purpose::STANDARD
            .decode(&body)
            .map_err(|_| ApiError::InvalidBase64)?;

        let decrypted: Vec<u8> = decoded
            .iter()
            .enumerate()
            .map(|(i, &byte)| byte ^ key[i % key.len()])
            .collect();

        String::from_utf8(decrypted)
            .map_err(|_| ApiError::InvalidUtf8)?
    } else {
        // 明文逻辑（向后兼容）
        String::from_utf8(body.to_vec())
            .map_err(|_| ApiError::InvalidUtf8)?
    };

    if sql.is_empty() {
        return Err(ApiError::EmptySql);
    }

    // 执行 SQL...
    execute_sql(&sql).await
}
```

#### 16.1.2 前端实现（TypeScript）

```typescript
// 在 taos-explorer 前端添加
function xorEncrypt(sql: string, key: string): string {
    const sqlBytes = new TextEncoder().encode(sql);
    const keyBytes = new TextEncoder().encode(key);
    const encrypted = new Uint8Array(sqlBytes.length);

    for (let i = 0; i < sqlBytes.length; i++) {
        encrypted[i] = sqlBytes[i] ^ keyBytes[i % keyBytes.length];
    }

    return btoa(String.fromCharCode(...encrypted));
}

async function executeSql(sql: string, enableXor: boolean = false) {
    const headers: Record<string, string> = {
        'Authorization': getAuthToken(),
    };

    let body = sql;
    if (enableXor) {
        headers['X-Enable-Xor'] = 'true';
        body = xorEncrypt(sql, 'TDengine-Explorer-XOR-Key-2026');
    }

    const response = await fetch('/api/-/rest/sql', {
        method: 'POST',
        headers,
        body,
    });

    return response.json();
}
```

### 16.2 测试用例

#### 16.2.1 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_encrypt_decrypt() {
        let key = b"TDengine-Explorer-XOR-Key-2026";
        let sql = "SELECT * FROM test.meters LIMIT 10";

        let encrypted = xor_encrypt(sql, key);
        let decrypted = xor_decrypt(&encrypted, key);

        assert_eq!(sql, decrypted);
    }

    #[test]
    fn test_xor_with_long_sql() {
        let key = b"TDengine-Explorer-XOR-Key-2026";
        let sql = "SELECT * FROM test.meters WHERE ts > NOW() - 1d ".repeat(100);

        let encrypted = xor_encrypt(&sql, key);
        let decrypted = xor_decrypt(&encrypted, key);

        assert_eq!(sql, decrypted);
    }

    #[test]
    fn test_xor_with_unicode() {
        let key = b"TDengine-Explorer-XOR-Key-2026";
        let sql = "SELECT * FROM 测试.电表 WHERE 名称='传感器'";

        let encrypted = xor_encrypt(sql, key);
        let decrypted = xor_decrypt(&encrypted, key);

        assert_eq!(sql, decrypted);
    }
}
```

#### 16.2.2 集成测试

```python

## 17. tests/e2e/test_xor_encryption.py

import base64
import requests
import pytest

def xor_encrypt(sql: str, key: bytes) -> str:
    encrypted = bytes([b ^ key[i % len(key)] for i, b in enumerate(sql.encode('utf-8'))])
    return base64.b64encode(encrypted).decode('ascii')

def test_xor_encryption_enabled():
    """测试启用 XOR 加密"""
    url = "http://localhost:6060/api/-/rest/sql"
    key = b"TDengine-Explorer-XOR-Key-2026"
    sql = "SELECT * FROM test.meters LIMIT 10"

    encrypted_sql = xor_encrypt(sql, key)

    response = requests.post(
        url,
        headers={
            "Authorization": "Basic cm9vdDp0YW9zZGF0YQ==",
            "X-Enable-Xor": "true"
        },
        data=encrypted_sql
    )

    assert response.status_code == 200
    assert "data" in response.json()

def test_xor_encryption_disabled():
    """测试不启用 XOR 加密（向后兼容）"""
    url = "http://localhost:6060/api/-/rest/sql"
    sql = "SELECT * FROM test.meters LIMIT 10"

    response = requests.post(
        url,
        headers={
            "Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="
        },
        data=sql
    )

    assert response.status_code == 200
    assert "data" in response.json()

def test_xor_invalid_base64():
    """测试无效的 Base64 编码"""
    url = "http://localhost:6060/api/-/rest/sql"

    response = requests.post(
        url,
        headers={
            "Authorization": "Basic cm9vdDp0YW9zZGF0YQ==",
            "X-Enable-Xor": "true"
        },
        data="not-valid-base64!!!"
    )

    assert response.status_code == 400
    assert response.json()["code"] == 0x2701
```

### 17.1 性能基准测试

```rust
#[bench]
fn bench_xor_encrypt_small_sql(b: &mut Bencher) {
    let key = b"TDengine-Explorer-XOR-Key-2026";
    let sql = "SELECT * FROM test.meters LIMIT 10";

    b.iter(|| {
        xor_encrypt(sql, key)
    });
}

#[bench]
fn bench_xor_encrypt_large_sql(b: &mut Bencher) {
    let key = b"TDengine-Explorer-XOR-Key-2026";
    let sql = "SELECT * FROM test.meters WHERE ts > NOW() - 1d ".repeat(1000);

    b.iter(|| {
        xor_encrypt(&sql, key)
    });
}
```
