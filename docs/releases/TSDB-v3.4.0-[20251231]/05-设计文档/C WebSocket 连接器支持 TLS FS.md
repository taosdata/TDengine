# C WebSocket 连接器支持 TLS FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-31 | 2025-11-31 | 0.1 | 郭振伟 | 编写文档 |
| 2025-11-18 | 2025-11-31 | 0.2 | 郭振伟 | 根据 Review 意见修改文档 |

## 2. 背景

- 背景：随着企业级应用对数据传输安全性要求的不断提升，以及合规性认证的推进，TDengine 的 C WebSocket 连接器在现有功能基础上需进一步强化安全能力。
- 目标：C WebSocket 连接器新增 TLS 支持，实现端到端加密通信。

## 3. 定义

- CRL：CRL（Certificate Revocation List）是由证书颁发机构（CA）发布的一份已吊销证书的列表。这些证书在其原定到期日之前被撤销，因此不应再信任。
- PKCS#8：全称 “Private-Key Information Syntax Standard”。它属于 RSA 实验室（RSA Laboratories）提出的 PKCS（Public-Key Cryptography Standards）系列标准之一。
  ```shell {wrap}
  # 未加密的 PKCS#1
  -----BEGIN RSA PRIVATE KEY-----
  
  # 加密的 PKCS#1
  -----BEGIN RSA PRIVATE KEY-----
  Proc-Type: 4,ENCRYPTED
  DEK-Info: AES-256-CBC,4AF0B71E7B4E1E729CA5D230B34ED844
  
  # 未加密的 PKCS#8
  -----BEGIN PRIVATE KEY-----
  
  # 加密的 PKCS#8
  -----BEGIN ENCRYPTED PRIVATE KEY-----
  ```

## 4. 行为说明

### 4.1 配置文件

支持将所有连接都升级到 wss，运维可以统一管理、配置集中、无需每个程序改代码。

| 配置 | 类型 | 有效范围 | 缺省值 | 说明 |
| --- | --- | --- | --- | --- |
| wsTlsMode | unsigned int | [0, 3] | 0 | TLS 加密模式： - 0 - DISABLED: no TLS. If the server uses TLS, it will be automatically upgraded to TLS. - 1 - REQUIRED: enable TLS without verification - 2 - VERIFY_CA: enable TLS with CA verification - 3 - VERIFY_IDENTITY: enable TLS with CA and identity verification. SAN must be included; CN is ignored. |
| wsTlsVersion | char * | TLSv1.2,TLSv1.3 | TLSv1.3 | 客户端允许加密连接使用哪些协议。该值是一个以英文逗号分隔的协议版本列表，包含一个或多个协议版本。 |
| wsTlsCa | char * |  |  | 证书颁发机构 (CA) 证书文件的路径名或内容，用于验证服务端证书。如果使用证书，必须指定与服务器使用的证书相同的证书。 如果 `wsTlsCa` 与 `wsTlsCaDir` 同时配置，则全部生效。 |
| wsTlsCaDir | char * |  |  | 包含受信任的 TLS CA 证书文件的目录路径名。注意：此配置不递归遍历子目录。 如果 `wsTlsCa` 与 `wsTlsCaDir` 同时配置，则全部生效。 目前暂不支持，按需支持。 |
| wsTlsCrl | char * |  |  | 包含证书吊销列表的文件路径名或内容。 如果 `wsTlsCrl` 与 `wsTlsCrlDir` 同时配置，则全部生效。 目前暂不支持，按需支持。 |
| wsTlsCrlDir | char * |  |  | 包含证书吊销列表文件的目录路径名。注意：此配置不递归遍历子目录。 如果 `wsTlsCrl` 与 `wsTlsCrlDir` 同时配置，则全部生效。 目前暂不支持，按需支持。 |
| wsTlsCert | char * |  |  | 客户端公钥证书文件的路径名或内容。 目前暂不支持，按需支持。 |
| wsTlsKey | char * |  |  | 客户端私钥文件的路径名或内容。 目前暂不支持，按需支持。 |
| wsTlsKeyPwd | char * |  |  | 客户端私钥 (PKCS#8) 的解密口令，用于加载加密私钥文件。如果私钥不是加密格式，此配置可忽略。 目前暂不支持，按需支持。 |

#### 4.1.1 示例代码

```plaintext {wrap}

## 5. The security mode to use for the connection to the server. Possible values are:

## 0 - DISABLED: no TLS. If the server uses TLS, it will be automatically upgraded to TLS.

## 1 - REQUIRED: enable TLS without verification

## 2 - VERIFY_CA: enable TLS with CA verification

## 3 - VERIFY_IDENTITY: enable TLS with CA and identity verification

## 6. Default is 0 (disable).

wsTlsMode 0

## 7. Which protocols the client permits for encrypted connections.

## 8. The value is a list of one or more comma-separated protocol versions. 

wsTlsVersion TLSv1.2,TLSv1.3

## 9. The path name of the Certificate Authority (CA) certificate file.

## 10. If used, must specify the same certificate used by the server.

wsTlsCa /path/to/ca.crt

## 11. The path name of the directory that contains trusted TLS CA certificate files.

wsTlsCaDir /path/to/ca_dir

## 12. The path name of the file containing certificate revocation lists.

wsTlsCrl /path/to/crl.pem

## 13. The path name of the directory that contains files containing certificate revocation lists.

wsTlsCrlDir /path/to/crl_dir

## 14. The path name of the client public key certificate file.

wsTlsCert /path/to/client.crt

## 15. The path name of the client private key file.

wsTlsKey /path/to/client.key

## 16. The decryption password for the client's private key (PKCS#8), used to load the encrypted private key file. This configuration can be ignored if the private key is not in encrypted format.

wsTlsKeyPwd yourpass
```

### 16.1 API

新增 C API `taos_connect_with` 支持连接级别 TLS 配置。
```c
typedef struct OPTIONS {
  const char *keys[256];
  const char *values[256];
  uint16_t count;
} OPTIONS;

void taos_set_option(struct OPTIONS *options, const char *key, const char *value);

TAOS *taos_connect_with(const struct OPTIONS *options);
```

#### 16.1.1 示例代码

```java {wrap}
OPTIONS opt;
taos_set_option(&opt, "ip", "127.0.0.1");
taos_set_option(&opt, "port", "6030");
taos_set_option(&opt, "user", "root");
taos_set_option(&opt, "pass", "taosdata");
TAOS* taos = taos_connect_with(&opt);
```

## 17. 性能

开发完成后需要提供性能测试报告。

## 18. 安全

本次新增 TLS 支持主要为保障数据在网络传输过程中的机密性、完整性与可用性。

## 19. 兼容性

兼容 3.3.6 分支 TLS。

## 20. 运维

无。

## 21. 使用场景

当客户端与数据库连接跨越不可信网络（如公有云、互联网或不同数据中心）时，应启用 TLS 以保障通信通道安全。

| **wsTlsMode** | **wsTlsVersion** | **wsTlsCa** | 使用场景 |
| --- | --- | --- | --- |
| 0 - DISABLED | 不需要 | 不需要 | 不启用 TLS 加密。如果服务器启用 TLS，则会自动升级到 TLS。 |
| 1 - REQUIRED | 可选 | 不需要 | 启用 TLS 加密，但不验证服务器证书。 |
| 2 - VERIFY_CA | 可选 | 必需 | 验证服务器证书是否由可信 CA 签发，但不验证主机名。 |
| 3 - VERIFY_IDENTITY | 可选 | 必需 | 验证服务器证书是否由可信 CA 签发，并且验证主机名。 |

说明：
- `wsTlsVersion`：用于指定支持的 TLS 协议版本（如 TLSv1.2、TLSv1.3），建议根据安全要求启用较新的版本。
- `wsTlsCa`：用于指定受信任的 CA 证书文件或目录，以验证服务端证书的合法性。

## 22. 约束和限制

无。

## 23. 常见错误和排查

无。

## 24. 可观测性

无。

## 25. 安装和卸载

无。

## 26. 文档

开发完成后需要更新官网文档。

## 27. 参考文档

- [MySQL 8.0 C API Developer Guide](https://dev.mysql.com/doc/c-api/8.0/en/mysql-options.html)
- [PostgreSQL: 文档: 18: 32.1. 数据库连接控制函数 - PostgreSQL 数据库](https://postgresql.ac.cn/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
