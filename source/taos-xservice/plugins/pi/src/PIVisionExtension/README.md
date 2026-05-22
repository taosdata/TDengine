# TDengine CDR Auto-Refresh — PI Vision 扩展

## 问题

PI Vision 对 CDR（Custom Data Reference）数据源使用 120 秒固定轮询间隔，而原生 PI Point 使用 DataPipe 机制实现 5 秒刷新。该 120 秒间隔硬编码在 `OSIsoft.PIVisualization.Data.DataService.dll` 中（`PIDataQuery.DefaultFutureUpdatingExtraTime = 120`），无法通过任何配置修改。

## 解决方案

通过 PI Vision 的**自定义 Symbol 扩展机制**注入 JavaScript，拦截 `DiffForData` XHR 请求，修改 `EndTime` 参数使每次请求唯一，绕过后端 120 秒缓存，实现实时刷新。

## 安装步骤

### 方法一：使用安装脚本（推荐）

```powershell
# 在 PI Vision 服务器上以管理员身份运行 PowerShell
.\install.ps1
```

### 方法二：手动安装

1. 找到 PI Vision 安装目录（默认 `C:\Program Files\PIPC\PIVision`）

2. 将以下文件复制到 `Scripts\app\editor\symbols\ext\` 目录：

   ```
   sym-tdrefresh.js
   sym-tdrefresh-template.html
   ```

3. 重启 IIS：

   ```cmd
   iisreset
   ```

4. 刷新浏览器页面（Ctrl+F5）

## 验证

1. 打开 PI Vision 中包含 TDengine CDR 数据源的画面
2. 打开浏览器开发者工具（F12）→ Network 选项卡
3. 观察 `DiffForData` 请求，`StartTime` 和 `EndTime` 参数应该每 5 秒变化一次（`-3601s`, `-3602s`... 和 `*+1s`, `*+2s`...）
4. Value 和 Trend 控件应每 5 秒更新数据

## 工作原理

```
PI Vision 前端 ──5s DiffForData──▶ [XHR 拦截：修改 EndTime] ──▶ PI Vision 后端
                                                                     │
                                                              缓存 miss（EndTime 不同）
                                                                     │
                                                              调用 CDR GetValue()
                                                                     │
                                                              TDengine 返回最新数据
```

### 核心机制

1. **请求修改**：每次 `DiffForData` 请求将 `StartTime` 转换为秒并加递增偏移（如 `-1h` → `-3601s`, `-3602s`...），将 `EndTime` 从 `"*"` 改为 `"*+Ns"`（N 递增），使后端缓存无法命中
2. **ForceUpdate**：设置 `ForceUpdate = true` 进一步确保后端重新查询
3. **智能避让**：检测用户是否在查看历史数据（`EndTime` 不以 `"*"` 开头），查看历史时不干预

## 配置

在 `sym-tdrefresh.js` 文件顶部可以调整以下参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MAX_CYCLES` | 300 | 步进计数器重置阈值（每次递增 1 秒偏移，达到此值后重置为 1） |

## 卸载

删除 PI Vision 的 `Scripts\app\editor\symbols\ext\` 目录中的以下文件，然后 `iisreset`：

```
sym-tdrefresh.js
sym-tdrefresh-template.html
```

## 兼容性

- PI Vision 2021 及以上版本
- 扩展会拦截所有 `DiffForData` 请求（包括原生 PI Point），但仅对实时模式生效（`EndTime` 以 `*` 开头）。对 PI Point 的影响是每次请求的时间参数有微小偏移（1-300 秒），不影响 DataPipe 机制的正常刷新
- 查看历史数据时自动暂停（不干预非实时请求）
