---
title: Data Subscription
---

import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

To meet the needs of applications that require real-time access to data written into TDengine TSDB, or that need to process data in event-arrival order, TDengine TSDB provides a data subscription and consumption interface similar to that of a message queue system.

In many scenarios, by using TDengine TSDB as the time-series data platform, there is no longer a need to integrate an external message queue product, simplifying application design and reducing operational costs.

<DocCardList items={useCurrentSidebarCategory().items}/>
