# 改进密码机制 FS

## 1. 背景

需求文档：[改进密码机制 RS](https://taosdata.feishu.cn/wiki/WnGNwtYYRiUFx6kIl2ic9uLunAd)

## 2. 变更历史


| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/2/11 | 0.1 | 陈东明 |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 新配置EnableStrongPassword

#### 4.1.1 EnableStrongPassword 开启

密码至少包含大写字母、小写字母、数字、特殊字符中的三类

#### 4.1.2 EnableStrongPassword 关闭

关于“大写字母、小写字母、数字、特殊字符”的种类限制不再需要

#### 4.1.3 EnableStrongPassword 默认值

默认开启

### 4.2 通过SQL修改EnableStrongPassword

```bash
alter all dnode 'EnableStrongPassword' '1'
alter all dnode 'EnableStrongPassword' '0'
```

### 4.3 查看EnableStrongPassword

```bash
show variables;
```

### 4.4 密码长度

密码支持 8 到 255 位

## 5. 性能

无

## 6. 兼容性

### 6.1 创建用户和修改密码的兼容性

旧版本的 taosc 能够在新版本 taosd 正常创建用户和修改密码。旧版本 taosc 仍然只支持 16 位密码。

### 6.2 登录的兼容性

旧版本的 taosc 在使用短密码的情况下可以正常登录新版本 taosd
新版本的 taosc 可以正常登录旧版本 taosd

## 7. 运维

无

## 8. 使用场景

### 8.1 StrongPassword 开启

1. 使用简单密码（只包含字母）不能 createUser、修改密码，但是能正常登录
2. 使用复杂密码能 createUser、修改密码、正常登录

### 8.2 密码长度场景

使用大于 8 位小于等于 255 位密码可以创建用户、修改密码、登录
使用大于 255 位密码不可以创建用户、修改密码、登录

### 8.3 兼容性场景

#### 8.3.1 创建用户、修改用户密码

1. 使用旧 taosc，在新版 taosd 上，使用 16 位密码可以创建用户、修改密码
2. 使用新 taosc，在新版 taosd 上，使用 255 位密码可以创建用户、修改密码
3. 使用新 taosc，在旧版 taosd 上，不可以创建用户、修改密码

#### 8.3.2 登录

1. 使用旧 taosc，在新版 taosd 上，使用 16 位密码可以正常登录
2. 使用旧 taosc，在新版 taosd 上，使用 255 位 密码可以*不*正常登录
3. 使用新 taosc，在新版 taosd 上，可以正常登录
4. 使用新 taosc，在旧版 taosd 上，可以正常登录

## 9. 约束和限制

### 9.1 创建用户和修改密码的限制

新版本的 taosc 即便使用短密码也不能在旧版本的 taosd create user 和修改密码。

### 9.2 登录的限制

taosd 使用新版本，并且将密码改为长密码，使用旧版 本taosc无法登录

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

修改[官网](https://docs.taosdata.com/reference/taos-sql/limit/#%E4%B8%80%E8%88%AC%E9%99%90%E5%88%B6)中关于密码长度的描述。

## 14. 参考文档

## 15. 附录

### 15.1 消息体的修改

```bash
typedef struct {
  int8_t      createType;
  int8_t      superUser;  // denote if it is a super user or not
  int8_t      sysInfo;
  int8_t      enable;
  char        user[TSDB_USER_LEN];
  char        pass[TSDB_USET_PASSWORD_LEN];
  int32_t     numIpRanges;
  SIpV4Range* pIpRanges;
  int32_t     sqlLen;
  char*       sql;
  int8_t      isImport;
  int8_t      createDb;
  char        longPass[TSDB_USET_PASSWORD_LONGLEN];
} SCreateUserReq;
```

```bash
typedef struct {
  int8_t alterType;
  int8_t superUser;
  int8_t sysInfo;
  int8_t enable;
  int8_t isView;
  union {
    uint8_t flag;
    struct {
      uint8_t createdb : 1;
      uint8_t reserve : 7;
    };
  };
  char        user[TSDB_USER_LEN];
  char        pass[TSDB_USET_PASSWORD_LEN];
  char        objname[TSDB_DB_FNAME_LEN];  // db or topic
  char        tabName[TSDB_TABLE_NAME_LEN];
  char*       tagCond;
  int32_t     tagCondLen;
  int32_t     numIpRanges;
  SIpV4Range* pIpRanges;
  int64_t     privileges;
  int32_t     sqlLen;
  char*       sql;
  char        longPass[TSDB_USET_PASSWORD_LONGLEN];
} SAlterUserReq;
```

### 15.2 兼容旧版本taosc

在消息体中添加一个新字段，longPass，新版本的 taosc 使用这个新字段，旧版本的 taosc 仍然使用 pass 字段，taosd 会优先检查 longPass 字段，如果 longPass 字段为空，则会检查 pass 字段。
