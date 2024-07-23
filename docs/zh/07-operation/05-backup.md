---
sidebar_label: 备份和恢复
title: 数据备份和恢复
toc_max_heading_level: 4
---

为了防止数据丢失、误删操作，TDengine 提供全面的数据备份、恢复、容错、异地数据实时同步等功能，以保证数据存储的安全。本节简要说明备份和恢复功能。

## 基于 taosdump 进行数据备份恢复

taosdump 是一个开源工具，用于支持从运行中的 TDengine 集群备份数据并将备份的数据恢复到相同或另一个正在运行的 TDengine 集群中。taosdump 可以将数据库作为逻辑数据单元进行备份，也可以对数据库中指定时间段内的数据记录进行备份。在使用taosdump 时，可以指定数据备份的目录路径。如果不指定目录路径，taosdump 将默认将数据备份到当前目录。

以下为 taosdump 执行数据备份的使用示例。
```shell
taosdump -h localhost -P 6030 -D dbname -o /file/path
```

执行上述命令后，taosdump 会连接 localhost:6030 所在的 TDengine 集群，查询数据库 dbname 中的所有数据，并将数据备份到 /f ile/path 下。

在使用 taosdump 时，如果指定的存储路径已经包含数据文件，taosdump 会提示用户并立即退出，以避免数据被覆盖。这意味着同一存储路径只能用于一次备份。如果你看到相关提示，请谨慎操作，以免误操作导致数据丢失。

要将本地指定文件路径中的数据文件恢复到正在运行的 TDengine 集群中，可以通过指定命令行参数和数据文件所在路径来执行 taosdump 命令。以下为 taosdump 执行数据恢复的示例代码。
```shell
taosdump -i /file/path -h localhost -P 6030
```

执行上述命令后，taosdump 会连接 localhost:6030 所在的 TDengine 集群，并将 /file/path 下的数据文件恢复到 TDengine 集群中。

## 基于 TDengine Enterprise 进行数据备份恢复

TDengine Enterprise 提供了一个高效的增量备份功能，具体流程如下。

第 1 步，通过浏览器访问 taosExplorer 服务，访问地址通常为 TDengine 集群所在 IP 地址的端口 6060，如 http://localhost:6060。
第 2 步，在 taosExplorer 服务页面中的“系统管理 - 备份”页面新增一个数据备份任务，在任务配置信息中填写需要备份的数据库名称和备份存储文件路径，完成创建任务
后即可启动数据备份。
第 3 步，在数据备份任务完成后，在相同页面的已创建任务列表中找到创建的数据备份任务，直接执行一键恢复，就能够将数据恢复到 TDengine 中。

与 taosdump 相比，如果对相同的数据在指定存储路径下进行多次备份操作，由于TDengine Enterprise 不仅备份效率高，而且实行的是增量处理，因此每次备份任务都会很快完成。而由于 taosdump 永远是全量备份，因此 TDengine Enterprise 在数据量较大的场景下可以显著减小系统开销，而且更加方便。