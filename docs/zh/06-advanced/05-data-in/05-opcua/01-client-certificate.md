---
title: "生成 taosX OPC UA 客户端证书"
sidebar_label: "客户端证书"
---

当 OPC UA 服务端的 **Security Mode** 设置为 `Sign` 或 `SignAndEncrypt` 时，taosX 必须以客户端证书与私钥的方式与之建立安全通道。本页给出在 Windows 与 Linux/macOS 上生成证书与私钥的标准步骤，**Ignition、GE Cimplicity 等所有需要证书加密的场景都按本页执行一次即可**。

## 1. 证书要求

taosX OPC UA 插件使用 `tls.LoadX509KeyPair()` 加载证书，因此对证书有以下硬性要求：

- **格式**：PEM；不接受 DER、PKCS#12 等其他格式。
- **客户端 Application URI**：固定为 `urn:taosx-opc:client`。证书的 Subject Alternative Name (SAN) 中**必须包含** `URI:urn:taosx-opc:client`，否则服务端会拒绝并报 `StatusBadCertificateUriInvalid`。
- 证书可以在任意机器上生成，只需把最终的 `client_cert.pem` 与 `client_key.pem` 上传到 Explorer 即可。

:::tip
从 Ignition、GE 等服务器下载到的证书（例如 `ignition-server.der`）是**服务器自身的证书**，不能直接用作客户端证书。请按本页方法**自行生成客户端证书与私钥**。
:::

## 2. Windows（cmd.exe，推荐）

将下面的脚本另存为 `generate_taosx_opcua_cert.cmd`，在 `cmd.exe` 中执行即可。脚本会优先使用系统 `openssl`，如果未安装则回退到 Git for Windows 自带的 `C:\Program Files\Git\usr\bin\openssl.exe`。

```bat
@echo off
setlocal

set "CERT_DIR=C:\taosx_certs"
set "CONF_DIR=C:\tmp"
set "CONF_FILE=%CONF_DIR%\opcua_client_ext.cnf"
set "CERT_FILE=%CERT_DIR%\client_cert.pem"
set "KEY_FILE=%CERT_DIR%\client_key.pem"

if not exist "%CONF_DIR%" mkdir "%CONF_DIR%"
if not exist "%CERT_DIR%" mkdir "%CERT_DIR%"

echo [req]> "%CONF_FILE%"
echo distinguished_name = req_dn>> "%CONF_FILE%"
echo x509_extensions = v3_ext>> "%CONF_FILE%"
echo prompt = no>> "%CONF_FILE%"
echo.>> "%CONF_FILE%"
echo [req_dn]>> "%CONF_FILE%"
echo CN = taosx-opc-client>> "%CONF_FILE%"
echo O = TDengine>> "%CONF_FILE%"
echo.>> "%CONF_FILE%"
echo [v3_ext]>> "%CONF_FILE%"
echo basicConstraints = CA:FALSE>> "%CONF_FILE%"
echo keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment>> "%CONF_FILE%"
echo extendedKeyUsage = clientAuth, serverAuth>> "%CONF_FILE%"
echo subjectAltName = URI:urn:taosx-opc:client>> "%CONF_FILE%"

where openssl >nul 2>nul
if %errorlevel%==0 (
  set "OPENSSL=openssl"
) else (
  if exist "C:\Program Files\Git\usr\bin\openssl.exe" (
    set "OPENSSL=C:\Program Files\Git\usr\bin\openssl.exe"
  ) else (
    echo ERROR: openssl not found.
    echo Please install Git for Windows or OpenSSL, then run this script again.
    pause
    exit /b 1
  )
)

"%OPENSSL%" req -x509 -newkey rsa:2048 -nodes ^
  -subj "/CN=taosx-opc-client/O=TDengine" ^
  -keyout "%KEY_FILE%" ^
  -out "%CERT_FILE%" ^
  -days 3650 ^
  -config "%CONF_FILE%"

if not %errorlevel%==0 (
  echo ERROR: failed to generate certificate.
  pause
  exit /b 1
)

echo.
echo Certificate generated:
echo   %CERT_FILE%
echo Private key generated:
echo   %KEY_FILE%
echo.
echo Verify certificate:
"%OPENSSL%" x509 -in "%CERT_FILE%" -noout -subject -ext subjectAltName

echo.
echo SHA1 fingerprint (used by some servers, e.g. GE Cimplicity, to map the certificate to a user):
"%OPENSSL%" x509 -in "%CERT_FILE%" -noout -fingerprint -sha1

pause
endlocal
```

## 3. Windows（PowerShell）

### 步骤 1：创建证书配置文件

```powershell
@"
[req]
distinguished_name = req_dn
x509_extensions = v3_ext
prompt = no

[req_dn]
CN = taosx-opc-client
O = TDengine

[v3_ext]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = URI:urn:taosx-opc:client
"@ | Out-File -Encoding ascii C:\tmp\opcua_client_ext.cnf
```

### 步骤 2：生成证书与私钥（有效期 10 年）

```powershell
mkdir C:\taosx_certs -Force
openssl req -x509 -newkey rsa:2048 -nodes `
  -keyout C:\taosx_certs\client_key.pem `
  -out C:\taosx_certs\client_cert.pem `
  -days 3650 `
  -config C:\tmp\opcua_client_ext.cnf
```

## 4. Linux / macOS

```bash
# 创建配置文件
cat > /tmp/opcua_client_ext.cnf << 'EOF'
[req]
distinguished_name = req_dn
x509_extensions = v3_ext
prompt = no

[req_dn]
CN = taosx-opc-client
O = TDengine

[v3_ext]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = URI:urn:taosx-opc:client
EOF

# 生成证书与私钥（有效期 10 年）
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout client_key.pem \
  -out client_cert.pem \
  -days 3650 \
  -config /tmp/opcua_client_ext.cnf
```

## 5. 验证证书

无论使用哪种方式生成，都建议执行以下命令确认 SAN 包含必需的 Application URI：

```bash
openssl x509 -in client_cert.pem -noout -subject -ext subjectAltName
```

预期输出：

```text
subject=CN=taosx-opc-client, O=TDengine
X509v3 Subject Alternative Name:
    URI:urn:taosx-opc:client
```

如果服务端（例如 GE Cimplicity）需要按 SHA1 指纹将证书绑定到某个用户，使用如下命令获取指纹：

```bash
openssl x509 -in client_cert.pem -noout -fingerprint -sha1
```

## 6. 在 Explorer 中使用

生成的两个文件分别对应 Explorer **Connection Configuration** 区域的两个字段：

| 文件              | Explorer 字段              |
| ----------------- | -------------------------- |
| `client_cert.pem` | Secure Channel Certificate |
| `client_key.pem`  | Certificate's Private Key  |

上传后第一次 **Check Connection** 通常会失败，因为服务端尚未信任该证书。请按对应厂商的指南完成服务端"信任 / 绑定证书"的操作（参见 [Ignition 集成指南](./03-ignition.md) 和 [GE Cimplicity 集成指南](./04-ge-cimplicity.md)），随后重新进行连通性检查。
