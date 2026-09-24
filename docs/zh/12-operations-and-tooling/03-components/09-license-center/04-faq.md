---
title: 常见问题（FAQ）
sidebar_label: 常见问题（FAQ）
toc_max_heading_level: 4
---

本页汇总 License Center 使用过程中的常见问题。授权流程与基本概念见 [部署与授权](./01-deploy-and-activate.md) 与 [配额与槽位](./02-quota-and-slots.md)。

## 1. 许可证过期

### 1.1 all-in-one 免费版为什么 30 天就过期了，而 TSDB 还能继续使用？

这里包含两个问题：

1. **许可证提前过期**：免费 all-in-one 许可证的有效期通常为一年。若出现 30 天即过期的情况，通常是许可证的到期时间被后台调整过（例如测试目的）。如遇许可证异常提前过期，请联系 TDengine 服务商核对许可证信息。
2. **TSDB 过期后仍可使用**：IDMP 与 TSDB 本质上是两套系统。TSDB 在许可证过期后会进入宽限期，IDMP 当前未实现宽限期功能，因此可能出现 IDMP 已过期而 TSDB 仍可使用的现象。宽限期行为以具体产品版本及许可证类型为准。

## 2. 前端报错

### 2.1 IDMP 前端报错 `Request quota slot conflict` 是什么原因？

该错误表示所使用的槽位已被占用：同一张许可证可被多个集群使用，如果某个槽位已被其他集群占用，重复使用此槽位即会报此错误。此问题在版本升级过程或非正常安装使用的场景较常遇到。

解决方法：登录 CLS，在 **集群管理** 页面删除已被占用的槽位后重试。集群管理见 [部署与授权](./01-deploy-and-activate.md)。

## 3. 授权码导入

### 3.1 CLS 解析授权码失败怎么办？

CLS 解析授权码失败多数是手动输入授权码时粘贴错误导致的（例如复制不完整、混入空格或换行）。建议改用文件上传方式导入授权码，避免手动输入出错。

## 4. 离线环境授权

### 4.1 离线环境下如何激活免费版许可证？

可通过 ELS 生成离线授权文件，再导入 CLS 完成激活。具体步骤见 [部署与授权](./01-deploy-and-activate.md) 中的「离线授权」。

### 4.2 客户网络受限、无法连接 ELS，如何导入许可证？

使用离线模式导入许可证：由 ELS 生成离线授权文件后交付客户，客户在 CLS 的 **许可证** 页面通过 **离线导入** 完成授权。具体步骤见 [部署与授权](./01-deploy-and-activate.md)。

## 5. 授权流程与规则

### 5.1 ELS 签发许可证的流程是怎样的？CLS 离线场景如何处理？

ELS 侧的签发、续期、吊销等操作由 TDengine 服务商完成，整体链路见 [License Center 参考手册](./index.md)。CLS 离线场景下的授权处理方式见 [部署与授权](./01-deploy-and-activate.md) 中的「离线授权」。

## 6. 版本查询

### 6.1 如何判断 CLS 的版本号？

在部署 CLS 的主机上执行 `taoscls -V`：

```bash
taoscls -V
taoscls version: 1.0.5 (core-1.0.5)
git: 2befd19ee17194c8aa460eb30ee98a3508030d95
build: linux-x86_64 2026-07-31 10:24:41 +08:00
```

输出中的 `version` 即 CLS 版本号。

## 相关页面

- [License Center 参考手册](./index.md)
- [部署与授权](./01-deploy-and-activate.md)
- [配额与槽位](./02-quota-and-slots.md)
- [用量查看与可用性](./03-usage-and-availability.md)
