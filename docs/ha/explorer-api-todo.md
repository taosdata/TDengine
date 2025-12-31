# Ha explorer api

# XNode API 文档

## 任务管理接口 (Tasks)

| 方法 | 端点          | 描述         | 参数                                          | 状态 |
| :--- | :------------ | :----------- | :-------------------------------------------- | :--- |
| GET  | /tasks        | 获取任务列表 | `lang`, `detail=true`, `labels=type::${type}` | ✅   |
| POST | /tasks        | 创建任务     | -                                             | ✅   |
| POST | /tasks/start  | 启动所有任务 | -                                             | ✅   |
| POST | /tasks/stop   | 停止所有任务 | -                                             | ✅   |
| POST | /tasks/delete | 删除任务     | -                                             | ✅   |
| GET  | /tasks/export | 导出任务     | `ids`                                         | ✅   |
| POST | /tasks/import | 导入任务     | -                                             | ✅   |

### 单个任务操作

| 方法   | 端点                       | 描述             | 参数       | 状态                        |
| :----- | :------------------------- | :--------------- | :--------- | :-------------------------- |
| GET    | /tasks/:id                 | 获取任务详情     | -          | ✅                          |
| GET    | /tasks/:id/activities      | 获取任务活动日志 | -          | 需 taosd 支持 activities 表 |
| GET    | /tasks/:id/metrics         | 获取任务指标     | -          | 需 taosd 支持 metrics 表    |
| GET    | /tasks/:id/table_progress  | 获取表进度       | 优先级: 低 | -                           |
| GET    | /tasks/:id/vgroup_progress | 获取虚拟组进度   | 优先级: 低 | -                           |
| PATCH  | /tasks/:id                 | 更新任务         | -          | ✅                          |
| DELETE | /tasks/:id                 | 删除任务         | -          | ✅                          |
| POST   | /tasks/:id/start           | 启动任务         | -          | ✅                          |
| POST   | /tasks/:id/stop            | 停止任务         | -          | ✅                          |

## 数据源接口 (DataSource)

### 验证和采样

| 方法 | 端点            | 描述       | 状态 |
| :--- | :-------------- | :--------- | :--- |
| POST | /ds/in/validate | 验证数据源 | ✅   |
| POST | /ds/in/sample   | 采样数据   | ✅   |

### 文件下载

| 方法 | 端点                              | 描述             | 参数          |
| :--- | :-------------------------------- | :--------------- | :------------ |
| GET  | /ds/in/download/pi_default_config | 下载 PI 默认配置 | -             |
| GET  | /ds/in/download/all_data_sets     | 下载所有数据集   | `from`, `via` |

### 点数据文件处理

| 方法 | 端点                            | 描述             | 参数             |
| :--- | :------------------------------ | :--------------- | :--------------- |
| POST | /ds/in/point/file/download/task | 下载点数据任务   | -                |
| GET  | /ds/in/point/file/are/you/ready | 检查文件准备就绪 | `ticket`         |
| GET  | /ds/in/point/file/async         | 异步获取文件     | `ticket`         |
| GET  | /ds/in/point/file/template      | 获取文件模板     | `driver`, `lang` |
| POST | /ds/in/point/file/is_valid      | 验证文件有效性   | -                |

### OPC CSV 点数据

| 方法 | 端点                         | 描述            | 参数      |
| :--- | :--------------------------- | :-------------- | :-------- |
| POST | /ds/in/opc/csv/points        | 创建 OPC CSV 点 | -         |
| GET  | /ds/in/opc/csv/points/header | 获取 OPC CSV 头 | `task_id` |

### 点数据选项和页面

| 方法 | 端点                   | 描述           | 参数                          |
| :--- | :--------------------- | :------------- | :---------------------------- |
| POST | /ds/in/point/options   | 设置点数据选项 | -                             |
| GET  | /ds/in/point/data/page | 分页获取点数据 | `ticket`, `page`, `page_size` |
| POST | /ds/in/sets            | 创建数据集     | -                             |

## 文件管理接口

| 方法 | 端点      | 描述           | 参数                     |
| :--- | :-------- | :------------- | :----------------------- |
| POST | /upload   | 上传文件       | `multipart/form-data`    |
| GET  | /download | 下载文件       | `file_path`              |
| GET  | /filemeta | 获取文件元数据 | `file_path`, `file_type` |

## 数据转换接口 (Transform)

| 方法 | 端点                                   | 描述           | 参数 |
| :--- | :------------------------------------- | :------------- | :--- |
| POST | /transform/sample/flat                 | 采样扁平转换   | `tz` |
| POST | /transform/sample/flat/s_model/preview | 模型预览       | `tz` |
| GET  | /transform/parser/plugins              | 获取解析器插件 | -    |

## 监控和指标接口

| 方法 | 端点                 | 描述         | 参数   |
| :--- | :------------------- | :----------- | :----- |
| GET  | /metrics/description | 获取指标描述 | `lang` |

## Kafka 接口

| 方法 | 端点                   | 描述       | 优先级 |
| :--- | :--------------------- | :--------- | :----- |
| POST | /kafka/:id/seek_to_end | 移动到末尾 | 低     |
