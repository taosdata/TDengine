# Ignition OPC UA Server Demo

> 飞书原文（含截图）：<https://taosdata.feishu.cn/wiki/BBwpwr0msiPfQ2khPZWcCXtunff>

## Case 1: 跨服务器匿名认证

完成 Ignition 安装和启动后，进入 Ignition Gateway → **Config** → **Connections** → **OPC** → **OPC UA Server Settings**。

Ignition 默认配置绑定到 `localhost`，endpoint 地址包含：`<hostname>` 和 `<localhost>`。

这种情况下，如果 TDengine TSDB 和 Ignition 部署在不同的服务器上，将无法连接，因为 Ignition 仅在本地端口 `62541` 上监听。

需要做以下修改：

1. 将 **Bind Addresses** 从 `localhost` 更改为 `0.0.0.0`
2. 在 **Endpoint Addresses** 中添加 Ignition 服务器的 IP

可以使用 cmd 命令验证 Ignition 是否已在 `0.0.0.0:62541` 上监听：

```cmd
netstat -ano | findstr 62541
```

预期看到 `0.0.0.0:62541` 处于 `LISTENING` 状态。

完成上述配置后，即可在 TDengine Explorer 中通过匿名模式（Security Mode 选 `None`，Authentication 选 `Anonymous`）建立与 Ignition OPC UA Server 的连接。

## Case 2: 跨服务器证书认证

### 步骤 1：配置 Ignition OPC UA Server Settings

在 **General Settings** 中修改以下配置：

- **Security Policies**：勾选 `Basic256Sha256`
- **Security Mode**：勾选 `SignAndEncrypt`
- **Authentication > User Source**：设为 `default`

修改配置后保存。

> **注意**：User Source 需要使用 `default`，默认的 `opcua-module` 需要额外的权限配置。

### 步骤 2：生成客户端证书

在 PowerShell 中，首先使用以下命令生成证书配置文件：

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
"@ | Out-File -Encoding ascii $env:TEMP\opcua_client_ext.cnf
```

然后使用以下命令生成 `client_cert.pem` 和 `client_key.pem`：

```powershell
mkdir C:\taosx_certs -Force
openssl req -x509 -newkey rsa:2048 -nodes `
  -keyout C:\taosx_certs\client_key.pem `
  -out C:\taosx_certs\client_cert.pem `
  -days 3650 `
  -config $env:TEMP\opcua_client_ext.cnf
```

如果系统未安装 OpenSSL，可使用 Git for Windows 自带的 OpenSSL，在 PowerShell 中通过完整路径调用：

```powershell
& "C:\Program Files\Git\usr\bin\openssl.exe" version
```

确认可用后，将上面命令中的 `openssl` 替换为完整路径即可。

### 步骤 3：验证证书

```powershell
openssl x509 -in C:\taosx_certs\client_cert.pem -noout -subject -ext subjectAltName
```

预期输出：

```
subject=CN=taosx-opc-client, O=TDengine
X509v3 Subject Alternative Name:
    URI:urn:taosx-opc:client
```

### 步骤 4：在 Explorer 中使用生成的证书测试连通性

在 Explorer 创建 OPC-UA 数据源任务：

1. **Server Endpoint** 填写 Ignition 服务器地址
2. **Security Mode** 选择 `SignAndEncrypt`
3. **Security Policy** 选择 `Basic256Sha256`
4. **Secure Channel Certificate** 上传 `client_cert.pem`
5. **Certificate's Private Key** 上传 `client_key.pem`
6. **Authentication** 选择 `Username`，填入 Ignition `default` 用户源中的用户名和密码
7. 点击 **Check Connection**

> 首次连接会失败，这是正常的——Ignition 会将未知的客户端证书放入隔离区（Quarantined）。需要到 Ignition Gateway → **Config** → **Connections** → **OPC** → **Security** → **Server** 页面，将 Quarantined Certificates 中的 `taosx-opc-client` 证书 Trust 后，再次检查连通性。
