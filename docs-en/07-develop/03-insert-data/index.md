---
title: Insert
---

TDengine supports multiple protocols of inserting data, including SQL, InfluxDB Line protocol, OpenTSDB Telnet protocol, OpenTSDB JSON protocol. Data can be inserted row by row, or in batch. Data from one or more collecting points can be inserted simultaneously. In the meantime, data can be inserted with multiple threads, out of order data and historical data can be inserted too. InfluxDB Line protocol, OpenTSDB Telnet protocol and OpenTSDB JSON protocol are the 3 kinds of schemaless insert protocols supported by TDengine. It's not necessary to create stable and table in advance if using schemaless protocols, and the schemas can be adjusted automatically according to the data to be inserted.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```