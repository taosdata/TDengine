---
title: Client Libraries
description: Detailed introduction to connectors for various languages and REST API
slug: /tdengine-reference/client-libraries
---

import Image from '@theme/IdealImage';
import imgClientLib from '../../assets/client-libraries-01.png';

TDengine provides a rich set of application development interfaces. To help users quickly develop their applications, TDengine supports connectors for multiple programming languages. Official connectors include those for C/C++, Java, Python, Go, Node.js, C#, and Rust. These connectors support both native interfaces (taosc) and WebSocket interfaces to connect to the TDengine cluster. Community developers have also contributed several unofficial connectors, such as ADO.NET, Lua, and PHP connectors.

<figure>
<Image img={imgClientLib} alt="TDengine client library architecture"/>
<figcaption>Figure 1. TDengine client library architecture</figcaption>
</figure>

## Supported Platforms

Currently, TDengine's native interface connectors support the following platforms: X64/ARM64 hardware and Linux/Win64 development environments. The compatibility matrix is as follows:

| **CPU**       | **OS**    | **Java** | **Python** | **Go** | **Node.js** | **C#** | **Rust** | C/C++ |
| ------------- | --------- | -------- | ---------- | ------ | ----------- | ------ | -------- | ----- |
| **X86 64bit** | **Linux** | ●        | ●          | ●      | ●           | ●      | ●        | ●     |
| **X86 64bit** | **Win64** | ●        | ●          | ●      | ●           | ●      | ●        | ●     |
| **X86 64bit** | **macOS** | ●        | ●          | ●      | ○           | ○      | ●        | ●     |
| **ARM64**     | **Linux** | ●        | ●          | ●      | ●           | ○      | ○        | ●     |
| **ARM64**     | **macOS** | ●        | ●          | ●      | ○           | ○      | ●        | ●     |

Here, ● indicates official testing and verification, ○ indicates unofficial testing and verification, and -- means untested.

Using the REST connection, which does not depend on client drivers, enables support for a broader range of operating systems.

## Version Support

TDengine updates often introduce new features. The connector versions listed correspond to the optimal versions for each TDengine release.

| **TDengine Version** | **Java**    | **Python**                                  | **Go**       | **C#**        | **Node.js**     | **Rust** | **C/C++**            |
| -------------------- | ----------- | ------------------------------------------- | ------------ | ------------- | --------------- | -------- | -------------------- |
| **3.3.0.0 and above** | 3.3.0+      | taospy 2.7.15+, taos-ws-py 0.3.2+           | 3.5.5+       | 3.1.3+        | 3.1.0+          | Latest   | Same as TDengine     |
| **3.0.0.0 and above** | 3.0.2+      | Latest                                     | 3.0 branch   | 3.0.0         | 3.1.0           | Latest   | Same as TDengine     |
| **2.4.0.14 and above** | 2.0.38     | Latest                                     | develop branch | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | Latest   | Same as TDengine     |
| **2.4.0.4 - 2.4.0.13** | 2.0.37     | Latest                                     | develop branch | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | Latest   | Same as TDengine     |
| **2.2.x.x**           | 2.0.36     | Latest                                     | master branch | n/a           | 2.0.7 - 2.0.9   | Latest   | Same as TDengine     |
| **2.0.x.x**           | 2.0.34     | Latest                                     | master branch | n/a           | 2.0.1 - 2.0.6   | Latest   | Same as TDengine     |

## Feature Support

The following tables show the feature support for TDengine connectors.

### Using Native Interface (taosc)

| **Feature**          | **Java** | **Python** | **Go** | **C#** | **Rust** | **C/C++** |
| -------------------- | -------- | ---------- | ------ | ------ | -------- | --------- |
| **Connection Management** | Supported | Supported | Supported | Supported | Supported | Supported |
| **Execute SQL**      | Supported | Supported | Supported | Supported | Supported | Supported |
| **Parameter Binding**| Supported | Supported | Supported | Supported | Supported | Supported |
| **Data Subscription (TMQ)** | Supported | Supported | Supported | Supported | Supported | Supported |
| **Schemaless Write** | Supported | Supported | Supported | Supported | Supported | Supported |

:::info
Different programming languages follow their own database framework standards, so not all C/C++ interfaces require corresponding wrappers.
:::

### Using HTTP REST Interface

| **Feature**          | **Java** | **Python** | **Go** |
| -------------------- | -------- | ---------- | ------ |
| **Connection Management** | Supported | Supported | Supported |
| **Execute SQL**      | Supported | Supported | Supported |

### Using WebSocket Interface

| **Feature**          | **Java** | **Python** | **Go** | **C#** | **Node.js** | **Rust** | **C/C++** |
| -------------------- | -------- | ---------- | ------ | ------ | ----------- | -------- | --------- |
| **Connection Management** | Supported | Supported | Supported | Supported | Supported | Supported | Supported |
| **Execute SQL**      | Supported | Supported | Supported | Supported | Supported | Supported | Supported |
| **Parameter Binding**| Supported | Supported | Supported | Supported | Supported | Supported | Supported |
| **Data Subscription (TMQ)** | Supported | Supported | Supported | Supported | Supported | Supported | Supported |
| **Schemaless Write** | Supported | Supported | Supported | Supported | Supported | Supported | Supported |

:::warning
For TDengine version 2.0 and above, it is recommended that each thread in the application establish its own connection or use a connection pool based on threads to avoid interference between thread states caused by the "USE statement." However, query and write operations on connections are thread-safe.
:::

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import InstallOnLinux from "./_linux_install.mdx";
import InstallOnWindows from "./_windows_install.mdx";
import InstallOnMacOS from "./_macos_install.mdx";
import VerifyWindows from "./_verify_windows.mdx";
import VerifyLinux from "./_verify_linux.mdx";
import VerifyMacOS from "./_verify_macos.mdx";

## Installing Client Drivers

:::info
Client drivers are only required when using native interface connectors on systems without TDengine server software installed.
:::

### Installation Steps

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <InstallOnLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <InstallOnWindows />
  </TabItem>
  <TabItem value="macos" label="MacOS">
    <InstallOnMacOS />
  </TabItem>
</Tabs>

### Installation Verification

After completing the installation and configuration, and ensuring the TDengine service is running properly, you can log in using the TDengine CLI tool.

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <VerifyLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <VerifyWindows />
  </TabItem>
  <TabItem value="macos" label="MacOS">
    <VerifyMacOS />
  </TabItem>
</Tabs>

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
