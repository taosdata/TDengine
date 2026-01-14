---
title: Activate TDengine TSDB-Enterprise
---

This document describes how to activate a TDengine TSDB-Enterprise license.

## Prerequisites

- Contact TDengine or an authorized reseller to purchase TDengine TSDB-Enterprise.
- Install and deploy TDengine TSDB-Enterprise on the actual machines that you intend to license.

## Procedure

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
   
1. Once you receive your activation code, log in to TDengine TSDB Explorer. The default URL is `http://127.0.0.1:6060`.

1. From the main menu on the left, select **Management**. Open the **License** tab and click **Activate License**.

1. Enter your activation code and click **Confirm**.

Your TDengine TSDB-Enterprise deployment is now licensed. You can view the details of your license, including expiration date, on the **License** tab.