# PI Vision 使用 CDR 数据源时刷新频率问题分析

## 问题描述

在 PI Vision 中，使用 TDengine Custom Data Reference（CDR）作为数据源时，无论是 **Value 控件** 还是 **Trend 控件**，数据刷新间隔均为 **固定 120 秒（2 分钟）**，无法实现实时刷新。

而使用 PI Data Archive 原生 PI Point 作为数据源时，Value 和 Trend 控件均能 **每 5 秒** 自动刷新一次。

## 结论

> **这是 PI Vision 后端 DLL 中的硬编码限制，不是 TDengine CDR 代码的 Bug。**
>
> PI Vision 对 CDR 类型的数据源使用 GetValue/PlotValues 轮询机制，轮询间隔 120 秒写死在
> `OSIsoft.PIVisualization.Data.DataService.dll` 中（`PIDataQuery.DefaultFutureUpdatingExtraTime = 120`），
> 无法通过任何配置修改，也无法通过修改 CDR 代码来提升刷新频率。

## 实验验证

| 数据源类型     | 控件类型 | 刷新间隔    | 数据获取机制       |
| -------------- | -------- | ----------- | ------------------ |
| PI Point（原生） | Value    | **5 秒**    | DataPipe 事件推送  |
| PI Point（原生） | Trend    | **5 秒**    | DataPipe 事件推送  |
| CDR（TDengine）  | Value    | **120 秒**  | GetValue 轮询      |
| CDR（TDengine）  | Trend    | **120 秒**  | PlotValues 轮询    |

**验证环境**：PI Server `<pi-server-ip>`，AF Database `Meters`，TDengine CDR Element `TDengineCDR`

**关键实验**：将 PI Vision 的显示时间范围从 1h 缩短到 10m，CDR 的刷新间隔仍然是 120 秒，不受时间范围影响。

## 根本原因分析

### PI Point 的数据流（5 秒刷新）

```
PI Data Archive 内核 ──Event Pipe──▶ AF SDK AFDataPipe ──▶ PI Vision（5s 轮询管道）
```

PI Data Archive 内核内置了**事件管道（Event Pipe）**机制。当 PI Point 有新数据写入时，Data Archive 内核直接将事件推入管道。PI Vision 通过 AF SDK 的 `AFDataPipe` 订阅这些事件，每 5 秒轮询一次管道获取增量数据。这是 PI 系统的原生高效路径——**数据变化由内核感知并推送，不需要外部轮询查询。**

### CDR 的数据流（120 秒刷新）

```
TDengine ──HTTP REST──▶ CDR GetValue() ◀──120s 轮询──PI Vision 后端 ◀──5s──PI Vision 前端
```

CDR 是第三方扩展插件，PI Data Archive 内核**无法感知**外部数据源的变化。尽管我们在 CDR 中实现了完整的 `AFEventSource`（DataPipe），并且通过 PowerShell AF SDK 测试验证了 DataPipe 功能正常（20 秒内获取 4 个事件），但 **PI Vision 从不调用 `CreateDataPipe()` 来订阅 CDR 的 DataPipe**。

PI Vision 对 CDR 属性只使用以下方法获取数据：

- `GetValue()` — 用于 Value 控件，获取当前快照值
- `PlotValues()` — 用于 Trend 控件，获取时间范围内的绘图数据
- `RecordedValues()` — 偶尔用于历史查询

**轮询间隔固定 120 秒，由 PI Vision 后端 DLL 硬编码控制，CDR 侧无法影响。**

### 为什么 PI Vision 不使用 CDR 的 DataPipe？

PI Vision 的实时推送路径是为 PI Data Archive 的原生事件管道优化设计的。CDR 作为第三方插件，其 DataPipe 实现依赖 AF SDK 的反射调用（`CreateDataPipe` 是静态方法），PI Vision 选择不走这条非标准路径，而是使用更稳定的 GetValue 轮询方式。

关键区别在于：

- **PI Point DataPipe**：内核级事件管道，零开销，高可靠，PI Vision 信任并默认使用
- **CDR DataPipe**：用户代码实现的 `AFEventSource`，通过反射加载，PI Vision 不信任/不使用

## 深入分析：120 秒轮询间隔的来源

### 分析方法

通过 PowerShell 反射逐层分析 PI Vision 后端 DLL（`OSIsoft.PIVisualization.Data.DataService.dll`），定位 120 秒轮询间隔的确切来源。

### PI Vision 数据更新架构

PI Vision 的数据更新分为两层：

**第一层：前端 Data Pump**（`PV.data-pump.js`）

前端有一个定时器，每 `UpdateRate` 秒向后端发送数据请求。`UpdateRate` 来自 `ClientSettings.Default.UpdateRate`（定义在 `OSIsoft.PBViewer.Web.dll`），**默认值为 5 秒**。这不是瓶颈。

```powershell
# 验证前端 Data Pump 的 UpdateRate 值
Add-Type -Path 'C:\Program Files\PIPC\PIVision\bin\OSIsoft.PBViewer.Web.dll'
[OSIsoft.PBViewer.Web.Models.ClientSettings]::Default | Format-List UpdateRate, EventFrameUpdateRate

# 输出：
# UpdateRate              : 5      ← 前端每 5 秒请求一次
# EventFrameUpdateRate    : 15
```

前端向后端发送数据请求的 API 端点为 `Data/{requestId}/DiffForData`，定义在 `PV.web-services.js` 中，由 `PV.editor-data.js` 的 `requestDataUpdate()` 函数调用。

**第二层：后端数据服务**（`OSIsoft.PIVisualization.Data.DataService.dll`）

后端收到前端请求后，由 `UpdateManager` 和 `PIDataQuery` 控制是否真正去 AF SDK 拉取新数据。

### 排除 SlidingScaleIntervalRefresh

最初怀疑 `SlidingScaleIntervalRefresh` 类控制了 CDR 的轮询间隔。该类根据显示时间范围动态计算刷新间隔：

```powershell
Add-Type -Path 'C:\Program Files\PIPC\PIVision\bin\OSIsoft.PIVisualization.Data.DataService.dll'
@(600, 1800, 3600, 7200, 14400, 28800, 86400) | ForEach-Object {
    $interval = [OSIsoft.PIDataServices.DataService.SlidingScaleIntervalRefresh]::GetRefreshIntervalSeconds($_)
    Write-Host ("TimeRange={0}s ({1:N1}h) => RefreshInterval={2}s" -f $_, ($_ / 3600), $interval)
}

# 输出：
# TimeRange=600s (0.2h)   => RefreshInterval=0s     ← 10 分钟范围返回 0
# TimeRange=1800s (0.5h)  => RefreshInterval=0s     ← 30 分钟范围返回 0
# TimeRange=3600s (1.0h)  => RefreshInterval=45s
# TimeRange=7200s (2.0h)  => RefreshInterval=90s
# TimeRange=14400s (4.0h) => RefreshInterval=144s
# TimeRange=28800s (8.0h) => RefreshInterval=288s
# TimeRange=86400s (24.0h)=> RefreshInterval=648s
```

> ⚠️ **但实验证明 `SlidingScaleIntervalRefresh` 不适用于 CDR！**
>
> 将 PI Vision 时间范围改为 10 分钟（该函数返回 0，理论上应该每 5 秒刷新），
> CDR 仍然是 120 秒刷新。**该类仅用于 PI Point 的 interval-based 查询优化。**

### 定位真正的 120 秒来源

通过反射扫描 `OSIsoft.PIVisualization.Data.DataService.dll` 中所有类的静态数值字段：

```powershell
Add-Type -Path 'C:\Program Files\PIPC\PIVision\bin\OSIsoft.PIVisualization.Data.DataService.dll'
$types = [System.Reflection.Assembly]::GetAssembly(
    [OSIsoft.PIDataServices.DataService.UpdateManager]).GetTypes()
foreach ($t in $types) {
    $fields = $t.GetFields('Public,NonPublic,Instance,Static,DeclaredOnly')
    foreach ($f in $fields) {
        if ($f.FieldType -eq [int] -or $f.FieldType -eq [double]) {
            try {
                $val = $f.GetValue($null)
                if ($val -ne $null -and $val -ne 0) {
                    Write-Host ("$($t.Name).$($f.Name) = $val")
                }
            } catch {}
        }
    }
}
```

关键发现：

```
UpdateManager.defaultUpdateRateSeconds = 5        ← 后端定时器，每 5 秒触发一次
UpdateManager.minUpdateRateSeconds = 1
UpdateManager.maxUpdateRateSeconds = 5
PIDataQuery.DefaultUpdatingPollInterval = 60      ← 默认轮询间隔 60 秒
PIDataQuery.DefaultFutureUpdatingExtraTime = 120  ← ⚠️ 关键：120 秒！
TagInfo.maxAttrCacheTime = 30                     ← 属性缓存时间 30 秒
```

> 🔑 **`PIDataQuery.DefaultFutureUpdatingExtraTime = 120`** — 这就是 120 秒轮询间隔的来源。
>
> 该值是 `OSIsoft.PIVisualization.Data.DataService.dll` 中的编译时常量（static field），
> 不可通过 web.config 或任何配置修改。

### CDR 调试日志铁证

在 CDR 的 `GetValue()` 和 `PlotValues()` 方法中添加了时间戳日志。以下是 PI Vision 运行时的实际调用记录（时间范围已改为 10m）：

```
09:24:54.988 PlotValues [9:14:54..9:24:54] intervals=280
09:24:54.992 GetValue called
09:26:55.009 PlotValues [9:16:55..9:26:55] intervals=280
09:26:55.015 GetValue called
09:28:55.028 PlotValues [9:18:55..9:28:55] intervals=280
09:28:55.034 GetValue called
09:30:55.049 PlotValues [9:20:55..9:30:55] intervals=280
09:30:55.055 GetValue called
09:32:55.069 PlotValues [9:22:55..9:32:55] intervals=280
09:32:55.074 GetValue called
09:34:55.092 PlotValues [9:24:55..9:34:55] intervals=280
09:34:55.098 GetValue called
```

**每次调用间隔精确为 120 秒**，与 `DefaultFutureUpdatingExtraTime = 120` 完全吻合，且不受时间范围影响。

## 已验证但无效的配置项

| 配置项                                 | 值         | 作用                       | 能否提高刷新频率 |
| -------------------------------------- | ---------- | -------------------------- | ---------------- |
| web.config `AFDRSetting:TDEngine`      | `DisableInputs` | 控制 CDR 是否允许写入 | ❌ 不影响刷新 |
| `ClientSettings.Default.UpdateRate`    | 5 秒       | 前端 Data Pump 请求间隔    | ❌ 已足够快，不是瓶颈 |
| `SlidingScaleIntervalRefresh`          | 动态计算   | 基于时间范围的刷新间隔     | ❌ 仅用于 PI Point |
| `UpdateManager.defaultUpdateRateSeconds` | 5 秒     | 后端定时器间隔             | ❌ 已足够快，不是瓶颈 |
| PI Vision Admin 设置                   | —          | 无刷新频率配置项           | ❌ 无此选项 |
| PIVision 数据库 Settings 表            | —          | 无轮询间隔配置             | ❌ 无此字段 |
| 缩短显示时间范围至 10m                 | —          | 使 SlidingScaleIntervalRefresh 返回 0 | ❌ 不适用于 CDR |

## CDR DataPipe 验证（AF SDK 层面可用）

虽然 PI Vision 不使用 CDR 的 DataPipe，但我们的实现本身是正确的。以下 PowerShell 脚本可验证：

```powershell
Add-Type -Path 'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0\OSIsoft.AFSDK.dll'
$sys = [OSIsoft.AF.PISystems]::new()
$pi = $sys['piserver']
$db = $pi.Databases['Meters']

# 获取 CDR 属性
$el = $db.Elements['California'].Elements['TDengineCDR']
$attr = $el.Attributes['Vvv']

# 创建 DataPipe 并订阅
$pipe = [OSIsoft.AF.Data.AFDataPipe]::new()
$pipe.AddSignups(@($attr))

# 轮询事件
for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep 5
    $errors = $null
    $events = $pipe.GetUpdateEvents([ref]$errors)
    if ($events.Count -gt 0) {
        foreach ($ev in $events) {
            Write-Host "Event: $($ev.Value.Value) at $($ev.Value.Timestamp)"
        }
    }
}
$pipe.Dispose()
```

预期结果：在数据有更新时，每 5-20 秒即可获取到新事件。

## 总结

| 项目                          | 状态                                        |
| ----------------------------- | ------------------------------------------- |
| CDR GetValue（快照查询）      | ✅ 正常工作                                 |
| CDR PlotValues（趋势查询）    | ✅ 正常工作                                 |
| CDR RecordedValues（历史查询）| ✅ 正常工作                                 |
| CDR DataPipe（AF SDK 层面）   | ✅ 正常工作                                 |
| PI Vision 使用 CDR DataPipe   | ❌ PI Vision 不调用                         |
| PI Vision CDR 刷新频率        | ⚠️ 固定 120 秒（DLL 硬编码，不可配置）      |

**最终结论**：CDR 的所有数据访问方法均已正确实现并验证通过。PI Vision 对 CDR 的轮询间隔固定为 120 秒，由 `OSIsoft.PIVisualization.Data.DataService.dll` 中的 `PIDataQuery.DefaultFutureUpdatingExtraTime = 120` 硬编码控制。**该值无法通过 web.config、PI Vision Admin、数据库配置或修改 CDR 代码来改变。** 如需更快的刷新频率，需要使用自定义 Web 前端直接查询 TDengine，绕过 PI Vision。
