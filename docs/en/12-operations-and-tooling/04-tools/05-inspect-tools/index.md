---
sidebar_label: Inspection Tools
title: Inspection Tools
---

The TDengine Enterprise inspection toolkit supports deployment preparation and ongoing operational checks:

| Tool | Purpose |
| --- | --- |
| taosprecheck | Check host and cluster prerequisites before installation |
| taospreset | Apply required operating-system settings before installation |
| taosinstall | Install or upgrade TDengine on one host or a cluster |
| taosinspect | Run routine environment and TDengine health inspections |
| taosperf | Benchmark disk I/O and network performance |
| taossubscribe | Verify topic subscription delivery |

## Limitations

These tools are currently available to TDengine Enterprise users only.

## Supported Platforms

The toolkit has been validated on Kylin V10, Ubuntu 20.04.2, CentOS 7.9, LinxOS 6.0.99, openEuler 23.09, and Debian 12. Contact your delivery representative before using it on other platforms or versions.

TDengine v3.1 or later is required.

## Prerequisites

- Run as `root`, or use an account with `sudo` privileges.
- When using passwordless SSH, configure the current host to connect to itself as well.
- GLIBC 2.17 or later is required on x64; GLIBC 2.27 or later is required on ARM.
- Add all configured FQDN and IP mappings to `/etc/hosts`.
- Keep RESTful access to TDengine available while `taosinspect` runs.
- Enable SSH between cluster nodes. Otherwise, run each tool in local mode on every node.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
