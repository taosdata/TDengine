---
title: Insert Data
description: This document describes how to insert data into TDengine.
---

TDengine supports multiple protocols of inserting data, including SQL, InfluxDB Line protocol, OpenTSDB Telnet protocol, and OpenTSDB JSON protocol. Data can be inserted row by row, or in batches. Data from one or more collection points can be inserted simultaneously. Data can be inserted with multiple threads, and out of order data and historical data can be inserted as well. InfluxDB Line protocol, OpenTSDB Telnet protocol and OpenTSDB JSON protocol are the 3 kinds of schemaless insert protocols supported by TDengine. It's not necessary to create STables and tables in advance if using schemaless protocols, and the schemas can be adjusted automatically based on the data being inserted.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
