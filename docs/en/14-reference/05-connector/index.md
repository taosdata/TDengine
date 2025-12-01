---
title: Client Libraries
slug: /tdengine-reference/client-libraries
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import InstallOnLinux from "../../assets/resources/_linux_install.mdx";
import InstallOnWindows from "../../assets/resources/_windows_install.mdx";
import InstallOnMacOS from "../../assets/resources/_macos_install.mdx";
import VerifyWindows from "../../assets/resources/_verify_windows.mdx";
import VerifyLinux from "../../assets/resources/_verify_linux.mdx";
import VerifyMacOS from "../../assets/resources/_verify_macos.mdx";
import ConnectorType from "../../assets/resources/_connector_type.mdx";
import PlatformSupported from "../../assets/resources/_platform_supported.mdx";

<ConnectorType /> 

## Supported Platforms

<PlatformSupported /> 

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

The following table outlines the support for TDengine TSDB features across different connectors:


### WebSocket/Native Connections

| **Feature**              | **Java** | **Python** | **Go** | **C#** | **Node.js** | **Rust** | **C/C++** |
| ------------------------- | -------- | ---------- | ------ | ------ | ----------- | -------- | --------- |
| **Connection Management** | Supported | Supported  | Supported | Supported | Supported  | Supported | Supported |
| **Execute SQL**         | Supported | Supported  | Supported | Supported | Supported  | Supported | Supported |
| **Parameter Binding**     | Supported | Supported  | Supported | Supported | Supported  | Supported | Supported |
| **Data Subscription (TMQ)**| Supported | Supported  | Supported | Supported | Supported  | Supported | Supported |
| **Schema-less Write**   | Supported | Supported  | Supported | Supported | Supported  | Supported | Supported |

**Note**: The Node.js connector does not support native connections.

:::info
Due to differences in database framework specifications across programming languages, it does not mean that all C/C++ interfaces require corresponding encapsulation support.
:::


:::warning

- Regardless of the programming language connector used, for TDengine TSDB version 2.0 and above, it is recommended that each thread of a database application establishes an independent connection or creates a thread-based connection pool. This avoids mutual interference of the "USE statement" state within the connection between threads (however, the connection's query and write operations are thread-safe).

:::

### REST API

Supports **Execute SQL**


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
