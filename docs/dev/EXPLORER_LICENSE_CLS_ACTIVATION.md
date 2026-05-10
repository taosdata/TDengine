# Explorer License CLS Activation

`taos-explorer` 的许可证激活接口仍然使用 `POST /api/-/license`，但现在支持两种互斥模式：

## 1. 普通激活

请求体保持兼容：

```json
{
  "active_code": "cluster-active-code",
  "c_active_code": "connector-active-code"
}
```

当 taosd 当前 `clsEnabled=1` 时，Explorer 前端会阻止普通激活提交，要求用户先切到 CLS 激活页签关闭 CLS。

## 2. CLS 激活

请求体新增以下字段：

```json
{
  "cls_enabled": "1",
  "cls_refresh_interval": "15",
  "cls_url": "http://192.168.2.158:6072",
  "cls_license_id": "lic-7f858400-a21e-406b-8874-2cc98207ced0",
  "cls_quota_slot_id": "tsdb-9"
}
```

其中：

- `cls_quota_slot_id` 为可选字段；如果前端未填写，Explorer 后端会默认写入 `tsdb-1`
- CLS 服务地址输入框会展示默认占位值 `http://localhost:6072`

Explorer 后端按顺序执行：

1. `ALTER ALL DNODES 'clsRefreshInterval' '...'`
2. `ALTER ALL DNODES 'clsUrl' '...'`
3. `ALTER ALL DNODES 'clsLicenseId' '...'`
4. `ALTER ALL DNODES 'clsQuotaSlotId' '...'`
5. `ALTER ALL DNODES 'clsEnabled' '...'`

执行失败时不做回滚，直接把 taosd 返回错误透传给前端。

## 页面展示

- Explorer 会在提交后短暂等待，再执行 `SHOW VARIABLES;` 回读 CLS 变量，避免立刻读取到旧值
- Explorer 使用 `SHOW VARIABLES;` 读取 `clsEnabled`、`clsRefreshInterval`、`clsUrl`、`clsLicenseId`、`clsQuotaSlotId`、`clsLastSucTime`、`clsLastReqTime`、`clsLastFailReason`
- 当 `clsEnabled=0` 时，许可证页不渲染“CLS 配置信息”区块
- 当 `clsEnabled!=0` 时，页面单独渲染“CLS 配置信息”区块，并显示 8 个 CLS 参数/状态值
- 当 `clsLastFailReason` 为空时，页面使用本地化文案兜底显示：中文为 `无`，英文为 `None`
- CLS 页签默认回填当前变量值
