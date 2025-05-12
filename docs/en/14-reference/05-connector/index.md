---
title: Client Libraries
slug: /tdengine-reference/client-libraries
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Image from '@theme/IdealImage';
import imgClientLib from '../../assets/client-libraries-01.png';
import InstallOnLinux from "../../assets/resources/_linux_install.mdx";
import InstallOnWindows from "../../assets/resources/_windows_install.mdx";
import InstallOnMacOS from "../../assets/resources/_macos_install.mdx";
import VerifyWindows from "../../assets/resources/_verify_windows.mdx";
import VerifyLinux from "../../assets/resources/_verify_linux.mdx";
import VerifyMacOS from "../../assets/resources/_verify_macos.mdx";

TDengine provides a rich set of application development interfaces. To facilitate users in quickly developing their own applications, TDengine supports connectors for multiple programming languages, including official connectors for C/C++, Java, Python, Go, Node.js, C#, and Rust. These connectors support connecting to the TDengine cluster using the native interface (taosc) and WebSocket interface. Community developers have also contributed several unofficial connectors, such as the ADO.NET connector, Lua connector, and PHP connector.

<figure>
<Image img={imgClientLib} alt="TDengine client library architecture"/>
<figcaption>Figure 1. TDengine client library architecture</figcaption>
</figure>

## Supported Platforms

Currently, the native interface connectors for TDengine support platforms including: X64/ARM64 hardware platforms, as well as Linux/Win64 development environments. The compatibility matrix is as follows:

| **CPU**       | **OS**    | **Java** | **Python** | **Go** | **Node.js** | **C#** | **Rust** | C/C++ |
| ------------- | --------- | -------- | ---------- | ------ | ----------- | ------ | -------- | ----- |
| **X86 64bit** | **Linux** | ●        | ●          | ●      | ●           | ●      | ●        | ●     |
| **X86 64bit** | **Win64** | ●        | ●          | ●      | ●           | ●      | ●        | ●     |
| **X86 64bit** | **macOS** | ●        | ●          | ●      | ○           | ○      | ●        | ●     |
| **ARM64**     | **Linux** | ●        | ●          | ●      | ●           | ○      | ○        | ●     |
| **ARM64**     | **macOS** | ●        | ●          | ●      | ○           | ○      | ●        | ●     |

Where ● indicates official testing and verification passed, ○ indicates unofficial testing and verification passed, -- indicates unverified.

Using REST connections can support a wider range of operating systems as they do not depend on client drivers.

## Version Support

TDengine version updates often add new features. The list below shows the best matching connector versions for each TDengine version.

| **TDengine Version**   | **Java**    | **Python**                                  | **Go**       | **C#**        | **Node.js**     | **Rust** | **C/C++**            |
| ---------------------- | ----------- | ------------------------------------------- | ------------ | ------------- | --------------- | -------- | -------------------- |
| **3.3.0.0 and above**  | 3.3.0 and above | taospy 2.7.15 and above, taos-ws-py 0.3.2 and above | 3.5.5 and above  | 3.1.3 and above   | 3.1.0 and above     | Current version | Same as TDengine version |
| **3.0.0.0 and above**  | 3.0.2 and above   | Current version                            | 3.0 branch     | 3.0.0         | 3.1.0           | Current version | Same as TDengine version |
| **2.4.0.14 and above** | 2.0.38      | Current version                            | develop branch | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | Current version | Same as TDengine version |
| **2.4.0.4 - 2.4.0.13** | 2.0.37      | Current version                            | develop branch | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | Current version | Same as TDengine version |
| **2.2.x.x**            | 2.0.36      | Current version                            | master branch  | n/a           | 2.0.7 - 2.0.9   | Current version | Same as TDengine version |
| **2.0.x.x**            | 2.0.34      | Current version                            | master branch  | n/a           | 2.0.1 - 2.0.6   | Current version | Same as TDengine version |

## Feature Support

The following table compares the support for TDengine features by the connector:

### Using Native Interface (taosc)

| **Feature**          | **Java** | **Python** | **Go** | **C#** | **Rust** | **C/C++** |
| -------------------- | -------- | ---------- | ------ | ------ | -------- | --------- |
| **Connection Management** | Supported | Supported | Supported | Supported | Supported | Supported |
| **Execute SQL**      | Supported | Supported | Supported | Supported | Supported | Supported |
| **Parameter Binding**| Supported | Supported | Supported | Supported | Supported | Supported |
| **Data Subscription (TMQ)** | Supported | Supported | Supported | Supported | Supported | Supported |
| **Schema-less Write**| Supported | Supported | Supported | Supported | Supported | Supported |

:::info
Due to different database framework specifications in various programming languages, it does not imply that all C/C++ interfaces need corresponding encapsulation support.
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
| **Schema-less Write**| Supported | Supported | Supported | Supported | Supported | Supported | Supported |

:::warning

- Regardless of the programming language connector chosen, it is recommended for database applications using TDengine version 2.0 and above to establish an independent connection for each thread, or to establish a connection pool based on threads, to avoid interference of the "USE statement" state variable among threads (however, the connection's query and write operations are thread-safe).

:::

## Install Client Driver

:::info
You only need to install the client driver if you are using a native interface connector on a system where the TDengine server software is not installed.

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

After completing the above installation and configuration, and confirming that the TDengine service has started running normally, you can now use the TDengine CLI tool to log in.

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
