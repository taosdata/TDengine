---
title: TDengine SSL Configuration Guide
sidebar_label: SSL Configuration Guide
---

# TDengine SSL Configuration Guide

This guide focuses on SSL/TLS infrastructure configuration on the TDengine server side, including certificate generation and taosAdapter server configuration.

:::note Application-Layer Security
For client-side SSL/TLS (TrustStore, `wss/useSSL`, REST HTTPS), token authentication, dynamic token rotation, and connection-pool management, see [Connector Security Best Practices](./connector-security-best-practices.md).
:::

## 1. Generate a Self-Signed Certificate

### 1.1 Generate a Private Key

```bash
# Generate a 2048-bit RSA private key
openssl genrsa -out server.key 2048
```

### 1.2 Generate a Certificate Signing Request (CSR)

```bash
# Generate CSR interactively
openssl req -new -key server.key -out server.csr

# Fill in values as prompted (important: Common Name must be your server IP or domain)
# The following are example placeholders. Adjust for your environment:
#
# Country Name (2 letter code) [AU]: <YOUR_COUNTRY_CODE>           # Example: CN
# State or Province Name (full name) [Some-State]: <YOUR_STATE>    # Example: Beijing
# Locality Name (eg, city) []: <YOUR_CITY>                         # Example: Beijing
# Organization Name (eg, company) [Internet Widgits Pty Ltd]: <YOUR_ORG>  # Example: YourCompany
# Organizational Unit Name (eg, section) []: <YOUR_UNIT>            # Example: IT Department
# Common Name (e.g. server FQDN or YOUR name) []: <YOUR_SERVER_IP_OR_DOMAIN>  # Important! Example: 192.168.1.100 or tdserver.example.com
# Email Address []: <YOUR_EMAIL>                                    # Example: admin@example.com
```

:::tip Key Field

- **Common Name (CN)** must be the server IP address or domain used by clients.
- If clients connect via `192.168.1.100`, set CN to `192.168.1.100`.
- If clients connect via `tdserver.example.com`, set CN to `tdserver.example.com`.
:::

### 1.3 Generate a Self-Signed Certificate (Valid for 365 Days)

```bash
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
```

### 1.4 Copy Certificate and Key to the TDengine Configuration Directory

```bash
# Assume the TDengine configuration directory is /etc/taos
sudo cp server.crt /etc/taos/
sudo cp server.key /etc/taos/
sudo chown taos:taos /etc/taos/server.crt /etc/taos/server.key
sudo chmod 600 /etc/taos/server.key
```

---

## 2. Configure the TDengine Server Manually

:::info WebSocket SSL Configuration
This document applies to WebSocket connections. SSL is configured on **taosAdapter**.
:::

### 2.1 Edit `taosadapter.toml`

```bash
sudo vi /etc/taos/taosadapter.toml
```

### 2.2 Enable SSL in the Configuration File

```toml
[ssl]
# Enable SSL. Applicable for the Enterprise Edition.
enable   = true
# Certificate file paths (adjust to your actual paths)
certFile = "/path/to/your/server.crt"    # Example: /etc/taos/server.crt
keyFile  = "/path/to/your/server.key"    # Example: /etc/taos/server.key
```

:::tip Path Notes

- If you copied files to `/etc/taos/` in step 1.4, use the example paths above.
- If you use another path, update `certFile` and `keyFile` accordingly.
- Make sure file permissions are correct (`600` for private key files).
:::

### 2.3 Restart the taosAdapter Service

```bash
sudo systemctl restart taosadapter

# Verify service status
sudo systemctl status taosadapter
```

### 2.4 Check Logs to Confirm SSL Is Enabled

```bash
journalctl -u taosadapter -n 50

# You should see log entries similar to:
# SSL is enabled
```

---

## 3. Configure Clients

Client-side SSL/TLS (TrustStore, `wss/useSSL`, REST HTTPS) is documented in [Connector Security Best Practices](./connector-security-best-practices.md#2-ssltls-configuration).

For token authentication, dynamic rotation, and connection-pool security practices, refer to that document as well.
