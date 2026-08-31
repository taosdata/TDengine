---
sidebar_label: 激活企业版
title: 激活 TDengine TSDB 企业版
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

本文说明如何通过激活码激活 TDengine TSDB 企业版授权。

也可通过 License Center（ELS + CLS）完成多实例配额与授权管理。单集群激活码流程见下文；若需统一管理多个 TSDB / IDMP 实例的额度与槽位，请参阅 [License Center 参考手册](../../03-components/09-license-center/index.md) 与 [配额与槽位](../../03-components/09-license-center/02-quota-and-slots.md)。

## 前提条件

- 已向 TDengine 或授权代理商购买 TDengine TSDB 企业版。
- 已在拟授权的实际机器上安装并部署 TDengine TSDB 企业版。

## 操作步骤

### 获取激活码

1. 以 `root` 用户打开 TDengine CLI：

   ```shell
   taos
   ```

1. 执行以下 SQL，获取部署所需信息：

   ```sql
   SHOW CLUSTER MACHINES;
   ```

   示例输出如下：

   ```text
            id         | dnode_num |          machine         | version  |
   =======================================================================
   3609687158593567855 | 1         | Bdw+qvOCyvAOc3SS5GIyEOIi | 3.3.6.13 |
   ```

1. 将上述语句的完整输出复制后，发给客户经理或授权代理商，并一并提供以下信息：

   - 公司名称
   - 主要技术联系人的姓名与邮箱
   - 使用环境（生产、PoC 或测试）
   - 期望的授权期限

   客户经理或代理商将向你提供用于激活的激活码。

### 激活部署

<Tabs>
<TabItem value="TDengine CLI" label="TDengine CLI">

1. 收到激活码后，以 `root` 用户打开 TDengine CLI：

   ```shell
   taos
   ```

1. 将激活码应用到集群：

   ```sql
   ALTER CLUSTER 'activeCode' '<your-activation-code>';
   ```

此时 TDengine TSDB 企业版部署已完成授权。可执行以下 SQL 查看授权详情（含到期时间）：

```sql
SHOW GRANTS\G;
```

</TabItem>
<TabItem value="TDengine TSDB Explorer" label="TDengine TSDB Explorer">

1. 收到激活码后，以 `root` 用户登录 TDengine TSDB Explorer。默认地址为 `http://127.0.0.1:6060`。

1. 在左侧主菜单选择 **系统管理**，打开 **许可证** 标签页，点击 **激活许可证**。

1. 输入激活码，点击 **确定**。

   :::important
   请确保激活码外侧没有单引号。
   :::

此时 TDengine TSDB 企业版部署已完成授权。可在 **许可证** 标签页查看授权详情（含到期时间）。

</TabItem>
</Tabs>
