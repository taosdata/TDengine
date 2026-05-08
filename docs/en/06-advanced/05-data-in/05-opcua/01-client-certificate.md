---
title: "Generate the taosX OPC UA Client Certificate"
sidebar_label: "Client Certificate"
---

When the OPC UA server's **Security Mode** is set to `Sign` or `SignAndEncrypt`, taosX must establish the secure channel with a client certificate and private key. This page documents the standard procedure for generating that certificate on Windows and on Linux/macOS. **All scenarios that require certificate-encrypted access (Ignition, GE Cimplicity, …) follow the same procedure once.**

## 1. Certificate Requirements

The taosX OPC UA plugin loads certificates with `tls.LoadX509KeyPair()` and therefore enforces:

- **Format**: PEM. DER, PKCS#12 and other formats are rejected.
- **Client Application URI**: fixed to `urn:taosx-opc:client`. The certificate's Subject Alternative Name (SAN) **must contain** `URI:urn:taosx-opc:client`, otherwise the server rejects the connection with `StatusBadCertificateUriInvalid`.
- The certificate may be generated on any machine — only the resulting `client_cert.pem` and `client_key.pem` need to be uploaded into taosX Explorer.

:::tip
The certificate that you can download from a server such as Ignition (for example `ignition-server.der`) is the **server's own certificate** and cannot be used as a client certificate. You must generate a separate client certificate and private key as described below.
:::

## 2. Windows (cmd.exe — recommended)

Save the following script as `generate_taosx_opcua_cert.cmd` and run it from `cmd.exe`. It prefers a system-wide `openssl`; otherwise it falls back to the OpenSSL bundled with Git for Windows (`C:\Program Files\Git\usr\bin\openssl.exe`).

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

## 3. Windows (PowerShell)

**Step 1: Create the certificate config file**

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

**Step 2: Generate the certificate and private key (valid for 10 years)**

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
# Create the config file
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

# Generate the certificate and private key (valid for 10 years)
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout client_key.pem \
  -out client_cert.pem \
  -days 3650 \
  -config /tmp/opcua_client_ext.cnf
```

## 5. Verify the Certificate

Regardless of which method you use, always confirm that the SAN contains the required Application URI:

```bash
openssl x509 -in client_cert.pem -noout -subject -ext subjectAltName
```

Expected output:

```text
subject=CN=taosx-opc-client, O=TDengine
X509v3 Subject Alternative Name:
    URI:urn:taosx-opc:client
```

Some servers (for example GE Cimplicity) bind the client certificate to a user by its SHA1 fingerprint. Capture it with:

```bash
openssl x509 -in client_cert.pem -noout -fingerprint -sha1
```

## 6. Use the Certificate in Explorer

The two generated files map to the **Connection Configuration** fields in taosX Explorer as follows:

| File                | Explorer field                |
| ------------------- | ----------------------------- |
| `client_cert.pem`   | Secure Channel Certificate    |
| `client_key.pem`    | Certificate's Private Key     |

The first **Check Connection** attempt usually fails, because the server has not yet trusted the certificate. Follow the vendor-specific steps to trust or bind the certificate on the server side (see [Ignition Integration Guide](./03-ignition.md) and [GE Cimplicity Integration Guide](./04-ge-cimplicity.md)), then run **Check Connection** again.
