---
sidebar_label: Security Guide
title: Security Guide
description: "Overview of TDengine security: authentication and authorization, transport encryption, data security, audit, hardening, and advisories"
---

TDengine provides multi-layer security for production environments, covering identity authentication and authorization, transport encryption, access control, data-at-rest protection, operational auditing, and hardening suggestions for each component. The Enterprise edition delivers full capabilities for user permissions (including RBAC / separation of duties), IP whitelisting, audit, transparent encryption, and tokens; the Community edition provides a subset of basic capabilities—see each topic for details.

This chapter is organized by security capability:

- [Authentication and Authorization](./01-user.md): security overview for users, privileges, and RBAC; refer to the SQL manual for syntax and privilege matrices.
- [Transport Security](./02-transport-security.md): server certificates and taosAdapter SSL/TLS configuration.
- [Data Security](./03-data-security.md): IP whitelisting, secure delete (`SECURE_DELETE`), and transparent data encryption (TDE).
- [Connector Security](./04-connector-security.md): client SSL/TLS, token authentication, and dynamic rotation.
- [Audit and Compliance](./05-audit-and-compliance.md): audit configuration, log access, and security-advisory linkage.
- [Security Deployment Configuration Suggestions](./06-security-suggestions.md): component exposure, hardening, and gateway deployment.
- [Security Advisories](./07-security-advisories.md): known vulnerabilities, affected versions, and fixed versions.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
