---
title: "GE Cimplicity OPC UA Server Integration Guide"
sidebar_label: "GE Cimplicity"
---

This page describes how to configure the OPC UA connection in taosX Explorer when the data source is a **GE Cimplicity OPC UA Server**.

OPC UA security has two independent layers, and you have to satisfy both for a GE Cimplicity OPC UA Server.

### Secure Channel

- Always required when Security Mode is `SignAndEncrypt`.
- Configured via **Secure Channel Certificate** + **Certificate's Private Key** in taosX Explorer.

### Authentication

- Must match whichever user-authentication mode the GE OPC UA Server has actually enabled. Three common options:
  - **Anonymous**: the configuration verified by this guide and the GE default.
  - **Username**: when the server has enabled username/password authentication.
  - **Certificates**: only when the server has additionally configured a user-certificate whitelist for OPC UA user authentication; in normal deployments the certificate is used only for the secure channel — do **not** reuse the secure-channel certificate here by mistake.
- If you don't know which one the server has enabled, **try Anonymous first**.

## 1. Generate the taosX client certificate

Use the script from [Generate the taosX OPC UA Client Certificate](./01-client-certificate.md) (the Windows `cmd.exe` flavor is recommended for this scenario) to produce `client_cert.pem` and `client_key.pem`. Capture the certificate's **SHA1 fingerprint** — you will need it later to register the certificate on the GE OPC UA Server:

```bash
openssl x509 -in client_cert.pem -noout -fingerprint -sha1
```

## 2. Configure the OPC UA Connection in taosX Explorer

In taosX Explorer, go to **Data In → Create New Data In Task**, choose **OPC UA**, and apply the following settings.

### 2.1 Connection Configuration

| Explorer field | Value |
| --- | --- |
| Server Endpoint | `<host>:<port>/GeCssOpcUaServer` (use the actual deployment value) |
| Security Mode | `SignAndEncrypt` |
| Security Policy | `Basic128Rsa15` |
| Secure Channel Certificate | Upload `client_cert.pem` |
| Certificate's Private Key | Upload `client_key.pem` |

### 2.2 Authentication

Pick the tab that matches what the server has enabled:

| User auth enabled on server          | Explorer setting                                                                                                                |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| Anonymous (default / verified here)  | Authentication → `Anonymous`                                                                                                    |
| Username / password                  | Authentication → `Username`, fill in the credentials created on the server                                                      |
| OPC UA user certificate              | Authentication → `Certificates`, upload the certificate/key on the server's user-certificate whitelist (need not be the same as the secure-channel one) |

:::note
The **Authentication → Certificates** tab is **not** "upload the secure-channel certificate again". Unless the GE OPC UA Server has explicitly enabled OPC UA user-certificate authentication and added a certificate to its user whitelist, keep **Anonymous**.
:::

## 3. Trust and Associate the taosX Client Certificate on the GE OPC UA Server

The first **Check Connection** attempt in taosX Explorer will most likely fail. This is expected — the GE OPC UA Server has not yet trusted or mapped the new taosX client certificate.

On the GE OPC UA Server host, open the OPC UA Server configuration and go to:

```text
OPC UA Server -> Security Certificate / User Association
```

Add or select the taosX client certificate identifier and associate it with a privileged user, for example:

| Field | Value |
| --- | --- |
| Certificate Identifier | SHA1 fingerprint of `client_cert.pem`, without colons |
| Privileges User Name | A privileged GE Cimplicity user (for example `admin`) |

After the certificate is associated with the user, return to taosX Explorer and run **Check Connection** again.

## 4. Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `read header failed: EOF` on the first Check Connection | The GE OPC UA Server rejected or closed the secure-channel handshake because the taosX client certificate is not trusted or not mapped to a user yet. | Add the taosX certificate identifier on the server and associate it with a privileged user, then retry. |
| `StatusBadSecurityChecksFailed` | Security mode, security policy, certificate, or private key is incorrect. | Confirm `SignAndEncrypt`, `Basic128Rsa15`, `client_cert.pem`, and `client_key.pem`. |
| `StatusBadCertificateUriInvalid` | The certificate does not contain the taosX OPC UA Application URI. | Regenerate the certificate and confirm SAN contains `URI:urn:taosx-opc:client`. |
| Authentication-related error | The taosX Authentication tab does not match the user-auth mode actually enabled on the server. | Confirm with the server administrator whether the server enabled Anonymous / Username / OPC UA user certificate, then switch to the matching tab in Explorer and provide the right credentials. |

## 5. Key Takeaways

- The recommended reference configuration is `SignAndEncrypt + Basic128Rsa15 + Anonymous`; if the server has been configured for a different user-auth mode, choose Username or Certificates accordingly.
- taosX still requires a secure-channel client certificate and private key when Security Mode is `SignAndEncrypt`.
- The secure-channel certificate is uploaded in the **Connection Configuration** section, **not** in the **Authentication → Certificates** tab.
- The GE OPC UA Server must map the taosX certificate identifier to a privileged user.
- After the server-side certificate association is complete, rerun **Check Connection** in taosX Explorer.
