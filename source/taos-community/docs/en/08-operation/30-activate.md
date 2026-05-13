---
title: Activate TDengine TSDB-Enterprise
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import { Enterprise } from '../assets/resources/_resources.mdx';

<Enterprise/>

This document describes how to activate a TDengine TSDB-Enterprise license.

## Prerequisites

- Contact TDengine or an authorized reseller to purchase TDengine TSDB-Enterprise.
- Install and deploy TDengine TSDB-Enterprise on the actual machines that you intend to license.

## Procedure

### Obtain Your Activation Code

1. Open the TDengine CLI as the `root` user:

   ```shell
   taos
   ```

1. Run the following SQL statement to obtain required information for your deployment:

   ```sql
   SHOW CLUSTER MACHINES;
   ```

   Sample output is displayed as follows:

   ```text
            id         | dnode_num |          machine         | version  |
   =======================================================================
   3609687158593567855 | 1         | Bdw+qvOCyvAOc3SS5GIyEOIi | 3.3.6.13 |
   ```

1. Copy the entire output of the statement and send it to your account representative or authorized reseller. Also include the following information:

   - The name of your company
   - The name and email address of the primary technical contact
   - The intended environment (production, PoC, or testing)
   - The desired term of the license

   Your account representative or reseller will send you an activation code that you use to activate your TDengine TSDB-Enterprise deployment.

### Activate Your Deployment

<Tabs>
<TabItem value="TDengine CLI">

1. Once you receive your activation code, open the TDengine CLI as the `root` user.

   ```shell
   taos
   ```

1. Apply the activation code to your cluster:

   ```sql
   ALTER CLUSTER 'activeCode' '<your-activation-code>';
   ```

Your TDengine TSDB-Enterprise deployment is now licensed. You can run the following SQL statement to view the details of your license, including expiration date:

```sql
SHOW GRANTS\G;
```

</TabItem>
<TabItem value="TDengine TSDB Explorer">

1. Once you receive your activation code, log in to TDengine TSDB Explorer as the `root` user. The default URL is `http://127.0.0.1:6060`.

1. From the main menu on the left, select **Management**. Open the **License** tab and click **Activate License**.

1. Enter your activation code and click **Confirm**.

   :::important
   Ensure that the activation code is not enclosed in single quotes.
   :::

Your TDengine TSDB-Enterprise deployment is now licensed. You can view the details of your license, including expiration date, on the **License** tab.

</TabItem>
</Tabs>
