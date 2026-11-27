---
title: Supported Platforms
---

import PlatformSupported from "../10-developer-guide/08-connectors-reference/resources/_platform_supported.mdx";

For Community Edition releases after v3.0.7.1, use this page to check operating-system support. For domestic Chinese operating systems, use TDengine Enterprise or deploy with Docker.

## List of Platforms Supported by TDengine Server

|                            | **Version**          | **X64 64bit** | **ARM64** |
| ---------------------------|----------------------| ------------- | --------- |
| **CentOS**                 | **7.9 and above**    | ●             | ●         |
| **Ubuntu**                 | **18 and above**     | ●             | ●         |
| **RedHat**                 | **RHEL 7 and above** | ●             | ●         |
| **Debian**                 | **6.0 and above**    | ●             | ●         |
| **FreeBSD**                | **12 and above**     | ●             | ●         |
| **OpenSUSE**               | **All versions**     | ●             | ●         |
| **SUSE Linux**             | **11 and above**     | ●             | ●         |
| **Fedora**                 | **21 and above**     | ●             | ●         |
| **Windows Server**         | **2016 and above**   | ● / E           |           |
| **Windows**                | **10/11**            | ● / E           |           |
| **Galaxy Kirin**           | **V10 and above**    | ● / E           | ● / E       |
| **NeoKylin**               | **V7.0 and above**   | ● / E           | ● / E       |
| **UnionTech UOS**          | **V20 and above**    | ● / E           |           |
| **Inspur K-UX**            | **V8.0 and above**   | ● / E           |           |
| **CEOS**                   | **All releases**     | ● / E           | ● / E       |
| **Huawei Euler openEuler** | **V20.03 and above** | ● / E           | ● / E       |
| **Anolis OS**              | **V8.6 and above**   | ● / E           |           |
| **macOS**                  | **14.0 and above**   |               | ●         |

Note: 1) ● indicates officially tested and verified, ○ indicates unofficially tested, E indicates only supported by the TDengine TSDB-Enterprise.

   1) The TDengine TSDB-OSS only supports newer versions of mainstream operating systems, including Ubuntu 18+/CentOS 7+/CentOS Stream/Rocky(v3.4.3.0+)/AlmaLinux(v3.4.3.0+)/RedHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS, etc. For other operating systems and versions, please contact enterprise support.

## Supported CPU Architectures

The Enterprise server supports the following CPU architectures. On one operating system, use the package built for that architecture. The features are the same.

| Architecture | Typical processors | Package architecture | Support |
| --- | --- | --- | --- |
| X64 | Intel and AMD 64-bit processors | `x64` | ● |
| Hygon C86 | Hygon C86 series, such as 7280 and 7390 | `x64` | ● / E |
| ARM64 | 64-bit ARM processors | `arm64` | ● / E |

**Notes**

- Hygon C86 processors are compatible with the X64 instruction set, so use the `x64` package. No separate build is required. The full test suite has been verified on Hygon C86 with Huawei Euler openEuler and Anolis OS.
- On an ARM64 server, use the `arm64` package for that operating system.

## Domestic Environments and Private Cloud Deployment

TDengine Enterprise follows domestic information-technology security adaptation requirements. It can run on combinations of domestic operating systems and domestic CPUs, including CEOS (国能磐石), Galaxy Kirin, Huawei Euler openEuler, and Inspur K-UX, on X64, ARM64, and Hygon C86. The table above lists the supported versions and architectures. Contact Enterprise support to confirm a combination that is not listed.

In an enterprise private cloud, TDengine can run on physical machines, virtual machines (such as KVM and VMware), and containers (Docker and Kubernetes). The deployment steps are the same as on a physical machine. In a virtualized environment:

- Give each dnode dedicated CPU, memory, and data disks. Do not colocate it with a high-load workload, or write and query latency will fluctuate.
- Put the data directory on persistent storage. Do not use a temporary disk of a virtual machine or container.
- When transparent data encryption is enabled, keys are bound to the machine code. Some virtualization or container environments cannot provide a stable machine code. Verify this before deployment. See [Data-at-Rest Protection](../11-security-guide/06-data-security.md).
- For containers, see [Docker Deployment](./02-operations/03-deployment/02-docker.md) and [Kubernetes Deployment](./02-operations/03-deployment/03-kubernetes.md).

## List of Platforms Supported by TDengine Client and Connectors

<PlatformSupported />

## Supported Network Environments

TDengine supports both IPv4 and IPv6. For IPv6 configuration, see [Network and FQDN Configuration](./02-operations/09-network.md).
