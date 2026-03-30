# TD-31113 round函数对负数取整时符号有问题测试报告

## 1. 测试目标

3.0版本的round和参数在(-1, 0)之间时的ceil函数的返回值为-0，但实际应该返回0

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024-08-02 | 1.0 | @黄帅 |  |

## 3. 测试结论

修复版本的
round()在之前版本会返回-0值的参数条件下返回值为0，**符合预期**
ceil()参数在在(-1, 0)之间时返回值为0，**符合预期**

## 4. 已知问题和限制

无

## 5. 测试环境

branch: 3.0
client info: 3.3.3.0.alpha
server info: ver:3.3.3.0.alpha
build:Linux-x64 2024-08-02 08:30:37 +0800
gitinfo:9f0a2ac3ba0f1fa46a130f10e65a16f3547f1fd8

## 6. 测试范围及方法

### 6.1 测试范围

1. round()函数参数在(-0.5, 0)
2. ceil()函数参数在(-1, 0)

### 6.2 测试方法

直接在taos shell里使用select命令测试

## 7. 测试用例

### 7.1 关于round()函数的测试用例

```sql
select round(-0.3);
```

```sql
select round(-10/103);
```

```sql
select round(10/-103);
```

```sql
select round(-0.3+0.1);
```


### 7.2 关于ceil()函数的测试用例

```sql
select ceil(-0.3);
```

```sql
select ceil(-10/103);
```

```sql
select ceil(10/-103);
```

```sql
select ceil(-0.3+0.1);
```


## 8. 测试计划

2024-08-02
