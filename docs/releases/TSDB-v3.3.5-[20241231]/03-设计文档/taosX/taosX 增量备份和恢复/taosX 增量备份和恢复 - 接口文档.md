# taosX 增量备份和恢复 - 接口文档

## 1. 创建备份计划

### 1.1 Request

```shell {wrap}
POST http://127.0.0.1:6050/tasks?lang=zh
content-type: application/json

{
  "labels": [
    "type::backup",
    "cluster-id::7085856653048130971"
  ],
  "trigger": {
    "upcoming": "2024-11-30T00:00:00Z",
    "interval": "1d"
  },
  "from": "tmq+http://root:taosdata@192.168.0.201:6041/zyyang?stable=stb&max_retry=3&retry_interval=10s",
  "to": "local:/app2/zyyang/backup?max_size=1G&compression_level=fastest",
  "after_delete": "clear"
}
```

说明：
1. 使用创建任务，即： `POST /tasks`接口，来创建备份计划。
2. `trigger.upcoming`为：下次执行任务的日期时间，即：任务第一次执行的时间。
3. `trigger.interval`为：两次任务执行之间的时间间隔。
4. database、stable、max_try、retry_interval 在`from`中指定；
5. backup_path、max_size、compression_level 在`to`中指定；

### 1.2 Response

Response 就是 `POST /tasks`接口的返回值，没有变更。

## 2. 查看备份计划列表

### 2.1 Request

使用`GET ``/``tasks` 接口
```http {wrap}
GET http://127.0.0.1:6050/tasks?lang=zh&detail=true&labels=type::backup,cluster-id::7085856653048130971
```

### 2.2 Response

`GET ``/``tasks` 接口的返回值，没有变更

## 3. 查看备份计划

查看备份计划详情，使用备份计划列表中的详情，不需要请求后端。

## 4. 编辑备份计划

### 4.1 Request

使用`patch ``/tasks/{id}`接口
```http {wrap}
PATCH http://127.0.0.1:6050/tasks/11
content-type: application/json

{
  "trigger": {
    "upcoming": "2024-11-26T00:00:00Z",
    "interval": "5s"
  },
  "from": "tmq+http://root:taosdata@192.168.0.201:6041/zyyang?stable=stb&max_retry=3&retry_interval=10s",
  "to": "local:/Users/yangzy/taosx/zyyang/backup?max_size=1G&compression_level=fastest"
}
```

### 4.2 Response

`patch ``/tasks/{id}`接口的返回值，没有变更

## 5. 删除备份计划

### 5.1 Request

使用`DELETE ``/tasks/{id}`接口
```http {wrap}
DELETE http://127.0.0.1:6050/tasks/11?after_delete=clear
```

### 5.2 Response

`DELETE ``/tasks/{id}`接口的返回值，没有变更

## 6. 复制备份计划

不需要新增接口，没有接口变动。
在操作中点击“复制”，弹出和创建备份计划相同的表单。除了数据库和超级表被置为空外，其他配置项和被复制的计划相同。

## 7. 立即执行一次备份计划

// TODO：功能待补充

## 8. 查看备份计划的统计信息

// TODO：功能待补充

## 9. 查看备份文件

### 9.1 Request

获取备份计划的点位列表
```http {wrap}
GET http://127.0.0.1:6050/backup/1/points
```

### 9.2 Response

```json {wrap}
[
  {
    "point": "2024-12-11T11:27:43.602017Z",
    "vgroup_id": 1,
    "file_size": "30.00 MB",
    "file_count": 2
  },
  ...
]
```

### 9.3 Swagger

http://192.168.2.13:6050/swagger-ui/#/backup/get_backup_points

## 10. 创建恢复任务

### 10.1 Request

```http {wrap}
POST http://127.0.0.1:6050/tasks?lang=zh
content-type: application/json

{
  "labels": [
    "type::restore",
    "cluster-id::7085856653048130971"
  ],
  "trigger": {
    "schedule": "oneshot",
    "resume": "never"
  },
  "from": "local:/Users/yangzy/taosx/backup?topic=xxxx&points=2024-12-11T11:27:43.602017Z,2024-12-11T11:37:43.602017Z",
  "to": "taos+ws://192.168.0.201:6041/zyyang"
}
```

### 10.2 Response

Response 就是 `POST /tasks`接口的返回值，没有变更。

## 11. 查看恢复任务列表

### 11.1 Request

使用`GET ``/``tasks` 接口
```http {wrap}
GET http://127.0.0.1:6050/tasks?lang=zh&detail=true&labels=type::restore,cluster-id::7085856653048130971
```

### 11.2 Response

`GET ``/``tasks` 接口的返回值，没有变更

## 12. 查看恢复任务的统计信息

// TODO：功能待补充
