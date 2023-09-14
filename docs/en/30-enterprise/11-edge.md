---
title: Edge-Cloud Orchestration
sidebar_label: Edge-Cloud Orchestration
---

## Introduction

You can implement edge-cloud orchestration by using taosX and TDengine data subscription. To do so, deploy taosX in the cloud or on the central TDengine Server. Then create topics that replicate the desired data from the edge to the cloud in real time.  This article describes how to use the taosX command line to implement edge-cloud orchestration. You can also implement edge-cloud orchestration through taosExplorer. For more information, see [taosExplorer](../explorer/). For information about installing TDengine, see [Installation](../../get-started/).

## Command-Line Parameters

For information about taosX command-line parameters, see [taosX](../../reference/taosx).

### Configuration

The following table describes the parameters that you configure to implement edge-cloud orchestration.

Parameter  | Description                                                             | Default                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | Specify the data subscription group ID.                                                 | If you do not specify a group, the group ID is automatically generated based on the hash value. |
| client.id | Specify the client ID for data subscription.                                               | taosx                      |
| timeout   | Specify a timeout for the connection. You can enter `never` to prevent taosX from timing out. | 500 ms                      |
| offset    | Specify an offset from which data subscription begins. Enter the offset in the format `<vgroup_id>:<offset>`. Separate multiple offsets with commas (,).  | If you do not specify an offset, data subscription begins at 0.  |
| token     | Specify the token for the target TDengine cluster. The token is used for authentication.                              | None                                     |

### DSN

**You must configure an existing and subscribable topic as the object of the DSN.**

```shell
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/tp1 ?group.id=taosx1&client.id=taosx&timeout=never&offset=2:10' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```

In this command, `tpl` indicates a topic that has been created in the specified database. You can use the topic to perform filtering such that only data that matches the specified conditions is replicated to the cloud.
