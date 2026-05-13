# TD-30442 流计算中checkpoint文件夹中info文件测试报告

## 1. 测试目标

测试的代码路径为：https://github.com/taosdata/TDengine/pull/26439。在该分支中新增一个功能：在流计算中的checkpoint文件夹中新增info文件，文件内容为`checkpointid checkpointver`， 在测试中我们将从info文件中读取的信息和通过`checkpoint_id, checkpoint_ver from information_schema.ins_stream_tasks`的信息进行比对。由于只有`level`为`agg`和`source`对应的流会产生checkpoint文件，因此我们只对这两类流进行验证。

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024.07.11 | 1.0 | @张志鹏 |  |

## 3. 测试范围以及测试流程

在本次测试中主要考虑在流回归情况下info文件是否还能正常使用。包含的测试范围为：
1. 对流进行反复暂停和恢复，查看info文件和`select`得到的是否一致
2. `redistribute vgroup`后，查看info文件和`select`是否一致
3. 使用`alter database replica`，查看info文件和`select`得到是否一致
值得注意的是，由于`checkpoint Interval`的范围只能设置为`60 - 1200`，因此测试代码运行的时间较长，在每次进行检验前，我们会暂停60s让checkpoint文件产生。

## 4. 测试结论

在三种情况下测试均通过。但在进行`redistribute vgroup`后需要等待较长时间才会产生checkpoint文件(~ 100s)，应该是迁移后等流进入`ready`状态需要花费一定的时间。
在测试中还发现了流会进入`stop`,`unint`的状态的问题，在@廖浩均帮助下成功解决，测试的版本为已经对该问题修正后的版本，具体的修正为在source/libs/stream/streamcheckpoint.c:458中加入： 帮助下成功解决，测试的版本为已经对该问题修正后的版本，具体的修正为在`source/libs/stream/streamcheckpoint.c:458`中加入：
```c
      if (streamMetaCommit(pMeta) < 0) {
        // persist to disk
      }
```
