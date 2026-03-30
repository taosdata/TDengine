# 动态调整 LRU 分片数量以提高 Last 查询性能 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-19 | 2026-03-20 | 0.1 | 鲍之骁 | 初稿 |

## 2. 测试目标

<quote-container>
主要用于测试动态调整 tsdb cache 的分片数量功能的功能是否正确，性能是否有显著提升。
由于之前 last/last_row 缓存在代码中写死了只支持一个分片，高并发场景会出现性能瓶颈。
</quote-container>

## 3. 参考文档

<quote-container>

</quote-container>

## 4. 测试结论

<quote-container>
功能符合预期
</quote-container>

## 5. 测试环境

- OS: Linux ;CPU: 16 cores

## 6. 功能测试

### 6.1 测试修改数据库选项 CACHESHARDBITS

#### 6.1.1 测试要点

测试修改数据库选项 CACHESHARDBITS

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 创建数据库并设置 CACHESHARDBITS 为3 | 查看数据库选项 CACHESHARDBITS 是否被正确设置。 | 通过 |

### 6.2 测试数据库选项 CACHESHARDBITS

#### 6.2.1 测试要点

测试数据库选项 CACHESHARDBITS

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 创建数据库并修改 CACHESHARDBITS 为3 | 查看数据库选项 CACHESHARDBITS 是否被正确设置。 | 通过 |

### 6.3 查看数据库选项 CACHESHARDBITS

#### 6.3.1 测试要点

测试数据库选项 CACHESHARDBITS

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Show Create database db; | 可以正确显示 CACHESHARDBITS | 通过 |
| 2 | select * from information_schema.ins_database; | 可以正确显示 CACHESHARDBITS | 通过 |

## 7. 易用性测试

无。

## 8. 长期稳定性测试

无。

## 9. 性能测试

### 9.1 极限场景测试

1. 场景描述
  - Vnode数量 ：1
  - Buffer大小 : 512
  - 写入相关: 1w 张子表，每张子表 100条数据
  - 查询相关：64并发，查询超级表下每一张子表的 last

  | 3.3.6 | 优化分支 | 性能提升 |
| --- | --- | --- |
| 3.549 QPS | 53.353 QPS | 约15倍 |

### 9.2 通用场景测试

1. 场景描述
  - Vnode数量 ：8
  - Buffer大小 : 512
  - 写入相关: 1w 张子表，每张子表 100条数据
  - 查询相关：128并发，查询超级表下每一张子表的 last

  | 3.3.6 | 优化分支 | 性能提升 |
| --- | --- | --- |
| 14.208 QPS | 74.917 QPS | 约 5.3 倍 |

附上查询和写入的配置文件，可随时验证：
 
> ⚠ 嵌入文件，需在飞书中查看 (token: C3Jwb62rxoFpKoxTV0Nc5eNYnbc)

> ⚠ 嵌入文件，需在飞书中查看 (token: UMPmbS08foVFbQxqHN6cltn5n9e)

## 10. 安全测试

不涉及

## 11. 兼容性测试

无

## 12. 已知问题和限制

1. 修改 `CACHESHARDBITS` 后，缓存会失效，需要重新加载缓存。也就是说修改`CACHESHARDBITS`后，已缓存数据将在后续查询时重新从磁盘加载，期间查询延迟可能短暂升高。
