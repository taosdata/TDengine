# PI Vision 画面不刷新 — 解决方案

## 问题回顾

在 PI AF 中配置 TDengine 数据引用（CDR）后，PI Vision 画面中引用该点位的 Value 控件不会实时刷新。

## 架构分析

### 数据展示链路

```
TDengine ──HTTP REST SQL──▶ CDR (TDEngineDataReference) ──▶ PI AF (AFService) ──▶ PI Vision (IIS)
```

### PI Vision 对 Value 控件的更新机制

经过调试验证，PI Vision 对 CDR 的 Value 控件使用 **GetValue 轮询**模式，默认间隔约 **60–120 秒**。PI Vision 并不对 CDR 使用 DataPipe 通道——DataPipe 仅对原生 PI Point 数据源生效。

> **重要发现**：Trend（趋势图）控件能"实时"刷新，是因为 PI Vision 每次传入新时间范围调用 `PlotValues`/`RecordedValues`，查询结果自然不同。而 Value 控件只调用 `GetValue` 获取快照。

### 同事的临时方案（Tampermonkey 脚本）

通过浏览器插件拦截 DiffForData 请求，将 `EndTime` 从 `"*"` 改为 `"*+Nm"`（N 递增），并设置 `ForceUpdate = true`，强制绕过 PI Vision 前端的 diff 缓存。

**此方案不可用于生产**：需要每个用户安装浏览器插件。

## 约束条件

1. **不能修改** PI Vision 或 PI Server 的代码与配置。
2. **不能要求用户** 安装浏览器插件。
3. **只能修改** `plugins/pi/` 目录下我们自己的 CDR 代码。

## 代码修复总结

本次修复涉及以下文件，每个文件的修改和踩过的坑记录如下：

### 修改的文件清单

| 文件                       | 修改内容                                             |
| -------------------------- | ---------------------------------------------------- |
| `TDEventSource.cs`         | DataPipe 事件源修复（返回值、异常处理、时间边界）    |
| `TDEngineDataReference.cs` | `CreateDataPipe()` 签名修复                          |
| `TDPIStream.cs`            | SQL 查询反引号转义、`from from` 拼写修复、全限定表名 |
| `TDEngineResponse.cs`      | `ToTDValues()` 数组越界修复                          |

### 1. TDEventSource.cs — DataPipe 事件源

#### 问题 1：`GetEvents()` 始终返回 `false`

`AFEventSource.GetEvents()` 的契约：

- `return true` → 还有更多事件待处理，PI AF 立即再次调用
- `return false` → 暂无事件，PI AF 按自身调度间隔再调用

始终返回 `false` 导致即使有新数据，PI AF 也不会加速轮询。

**修复**：跟踪是否有新事件发布，有则返回 `true`。

#### 问题 2：`_startTime` 时间边界

`_startTime = new AFTime("*")` 导致第一次查询时，首条数据的时间戳与 `_lastTimes` 完全相等（`val.Timestamp > _lastTimes[att]` 为 `false`），第一批数据被跳过。

**修复**：改为 `_startTime = new AFTime("*-1s")`。

#### 问题 3：无异常处理

`att.GetValues()` 底层执行 HTTP 请求。网络异常会导致 `GetEvents()` 抛异常，**可能导致 DataPipe 完全停止**。

**修复**：每个属性的查询包裹 try-catch，单个失败不影响其他属性。

#### 问题 4：`TDEventSource` 类访问修饰符 ⚠️ 关键

原代码 `class TDEventSource : AFEventSource`（默认 internal）。AF SDK 通过反射实例化该类，**internal 类导致 AF 无法创建 DataPipe 实例**——`CreateDataPipe()` 静默失败，不报任何错误。

**修复**：改为 `public class TDEventSource : AFEventSource`。

**排查经验**：如果 `CreateDataPipe()` 存在且签名正确，但 DataPipe 不工作（`TDEventSource` 构造函数日志不出现），首先检查类的访问修饰符。

### 2. TDEngineDataReference.cs — CreateDataPipe 签名

`CreateDataPipe()` 是 AF SDK 通过反射调用的**静态工厂方法**，不是 override。

**正确签名**：

```csharp
public static AFEventSource CreateDataPipe()
{
    return new TDEventSource();
}
```

**错误尝试**：

- `public override AFEventSource CreateDataPipe()` → 编译错误 CS0115，`AFDataReference` 中没有虚方法 `CreateDataPipe`
- `static public object CreateDataPipe()` → 返回类型为 `object` 可能导致 AF 反射匹配失败

### 3. TDPIStream.cs — SQL 查询修复

#### 问题 1：TDengine 保留字未转义

TDengine 列名如 `value`、`status` 是保留字，查询时必须用反引号包裹，否则报语法错误。

**修复**：`GetAllStringColumnNames()` 返回 `` ts, `{ColumnValue}` `` 或 `` ts, `{ColumnValue}`, `{ColumnStatus}` ``。

同样，`interp()` 函数中的列名也需要反引号：``interp(`{ColumnValue}`)``。

#### 问题 2：表名大小写敏感 ⚠️ 关键

taosx 创建子表时使用了反引号保留大小写（如 `` `Meter_1000001_Current` ``）。TDengine 内部存储为大小写敏感。查询时如果不用反引号，TDengine 会将表名转为小写导致找不到表：

```sql
-- ❌ 报错 "Table does not exist"
select * from Meter_1000001_Current limit 1;

-- ✅ 正确
select * from `Meter_1000001_Current` limit 1;
```

**修复**：所有 SQL 中的表名都用反引号包裹：`` `{Table}` ``。

#### 问题 3：全限定表名 ⚠️ 关键

TDengine REST API 的 `from` 子句中，应使用 `` `Database`.`Table` `` 全限定名，否则可能因为连接的默认数据库不一致导致找不到表。

**修复**：所有 SQL 中使用 `` `{Database}`.`{Table}` ``。

**修复前**：`from {Table}`
**修复后**：`` from `{Database}`.`{Table}` ``

#### 问题 4：`from from` 拼写错误

第 141 行 `RecordedValuesAtTimes()` 方法中有 `from from {Table}` 的 typo。

### 4. TDEngineResponse.cs — 数组越界

`ToTDValues()` 方法假设每行数据有 3 列（ts, value, quality），但 Table 模式下只有 2 列（ts, value），导致 `IndexOutOfRangeException`。

**修复**：添加 `if (dataItem.Count >= 3)` 条件判断，与已有的 `ToTDValue()` 方法保持一致。

## 部署指南

### 构建流程

在 PI AF 服务器上（需要安装 Visual Studio 或 MSBuild）：

```powershell
# 1. 获取代码
cd C:\workspace\taosx
git fetch origin fix/pi-vision-flush
git reset --hard origin/fix/pi-vision-flush

# 2. 编译
& 'C:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\amd64\msbuild.exe' `
    plugins\pi\src\CustomDataReference\TDEngineDR\TDEngineDR\TDEngineDR.csproj `
    /p:Configuration=Release /t:Rebuild /v:q

# 产物路径：plugins\pi\dist\TDEngineDR.dll
```

### PI AF 插件签名（首次部署必须）

PI AF 有插件信任机制：只有经过 Authenticode 签名、且签名者在 AF 数据库 `PlugInTrustedProvider` 表中注册的 DLL 才能被加载。未签名的 DLL 会报错：

```
Failed to load 'TDengine' PlugIn: Attempting to load an assembly with an invalid signature.
```

**注意**：这不是 .NET 的 Strong Naming 问题，而是 Windows Authenticode 代码签名 + PI AF 的受信任提供商白名单机制。

#### 1. 创建自签名代码签名证书

```powershell
$cert = New-SelfSignedCertificate `
    -Type CodeSigningCert `
    -Subject 'CN=TDengine Data, O=TDengine Data, C=CN' `
    -CertStoreLocation Cert:\LocalMachine\My `
    -NotAfter (Get-Date).AddYears(10)

# 记录证书指纹，后续签名时使用
$cert.Thumbprint
```

#### 2. 将证书添加到信任存储

```powershell
# 添加到受信任的根证书
$store = New-Object System.Security.Cryptography.X509Certificates.X509Store('Root','LocalMachine')
$store.Open('ReadWrite')
$store.Add($cert)
$store.Close()

# 添加到受信任的发布者
$store2 = New-Object System.Security.Cryptography.X509Certificates.X509Store('TrustedPublisher','LocalMachine')
$store2.Open('ReadWrite')
$store2.Add($cert)
$store2.Close()
```

#### 3. 在 PI AF 数据库中注册受信任提供商

连接 PI AF 使用的 SQL Server 实例（通常为 `piserver\SQLEXPRESS`），在 `PIFD` 数据库中执行：

```sql
SET IDENTITY_INSERT PlugInTrustedProvider ON;
INSERT INTO PlugInTrustedProvider (rid, name)
VALUES ((SELECT MAX(rid) + 1 FROM PlugInTrustedProvider), 'TDengine Data');
SET IDENTITY_INSERT PlugInTrustedProvider OFF;
```

> **重要**：`name` 字段的值必须与证书 Subject 中的 `O=`（Organization）字段一致。

#### 4. 对 DLL 签名并上传

```powershell
# 签名
$cert = Get-ChildItem Cert:\LocalMachine\My | Where-Object { $_.Thumbprint -eq '<证书指纹>' }
Set-AuthenticodeSignature `
    -FilePath 'C:\workspace\taosx\plugins\pi\dist\TDEngineDR.dll' `
    -Certificate $cert `
    -TimestampServer 'http://timestamp.digicert.com'

# 验证签名（Status 应为 Valid）
Get-AuthenticodeSignature 'C:\workspace\taosx\plugins\pi\dist\TDEngineDR.dll'

# 上传到 AF 数据库
[System.Reflection.Assembly]::LoadWithPartialName('OSIsoft.AFSDK')
$sys = New-Object OSIsoft.AF.PISystems
$pi = $sys['piserver']  # 替换为实际 PI AF 服务器名
$pi.UploadPlugInAssembly('C:\workspace\taosx\plugins\pi\dist\TDEngineDR.dll', $true)
```

### Newtonsoft.Json 依赖部署 ⚠️ 关键

TDEngineDR.dll 依赖 Newtonsoft.Json.dll（v13.0）。**每次 `UploadPlugInAssembly` 上传新版本 DLL，PI AF 会创建新的版本缓存目录**，Newtonsoft.Json.dll 需要手动复制到对应的 `4.0\` 子目录中。

#### 插件缓存目录结构

```
C:\ProgramData\OSIsoft\AF\PlugIns\
├── 1.0.9239.20960\          ← 旧版本
│   └── 4.0\
│       ├── TDEngineDR.dll
│       └── Newtonsoft.Json.dll
├── 1.0.9239.22746\          ← 新版本（UploadPlugInAssembly 创建）
│   ├── Newtonsoft.Json.dll  ← UploadPlugInSupportAssembly 放在根目录
│   └── 4.0\
│       ├── TDEngineDR.dll   ← DLL 在这里
│       └── (缺少 Newtonsoft.Json.dll!)  ← ⚠️ 需要手动复制
```

#### 部署脚本

```powershell
# 找到最新版本的缓存目录
$latest = Get-ChildItem 'C:\ProgramData\OSIsoft\AF\PlugIns' -Directory |
    Where-Object { $_.Name -match '1\.0\.9239' } |
    Sort-Object Name -Descending |
    Select-Object -First 1

$fourDir = Join-Path $latest.FullName '4.0'

# 检查并复制 Newtonsoft.Json.dll
if (-not (Test-Path (Join-Path $fourDir 'Newtonsoft.Json.dll'))) {
    # 从 DLL 同级目录或已知位置复制
    Copy-Item 'C:\path\to\Newtonsoft.Json.dll' (Join-Path $fourDir 'Newtonsoft.Json.dll')
    Write-Host "已复制 Newtonsoft.Json.dll 到 $fourDir"
}
```

**症状**：如果缺少 Newtonsoft.Json.dll，`GetValue` 会返回如下异常：

```
System.IO.FileNotFoundException: Could not load file or assembly
'Newtonsoft.Json, Version=13.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed'
```

### 重启服务

```powershell
# 必须重启 AFService 使新 DLL 生效
Restart-Service AFService

# 如果 PI Vision 页面缓存了旧数据，也需要重置 IIS
iisreset
```

### 验证步骤

#### 1. 验证 GetValue 正常

```powershell
[System.Reflection.Assembly]::LoadWithPartialName('OSIsoft.AFSDK')
$sys = New-Object OSIsoft.AF.PISystems
$pi = $sys['piserver']
$db = $pi.Databases['<数据库名>']
$el = $db.Elements['<元素路径>']  # 如 $db.Elements['California'].Elements['Los Angeles'].Elements['TDengine_Test']
$attr = $el.Attributes['Current']
$val = $attr.GetValue()
Write-Host "Value: $($val.Value) @ $($val.Timestamp)"
# 应返回 TDengine 中的最新数据
```

#### 2. 验证 DataPipe 正常

```powershell
$pipe = New-Object OSIsoft.AF.Data.AFDataPipe
$list = New-Object 'System.Collections.Generic.List[OSIsoft.AF.Asset.AFAttribute]'
$list.Add($attr)
$errs = $pipe.AddSignups($list)
Write-Host "Signup errors: $($errs.Count)"

Start-Sleep 20  # 等待 DataPipe 轮询

$hasMore = $false
$evts = $pipe.GetUpdateEvents([ref]$hasMore)
Write-Host "Events: $($evts.Count)"
foreach ($e in $evts) {
    Write-Host "  $($e.Value.Timestamp) = $($e.Value.Value)"
}
$pipe.Dispose()
# 应返回新写入的数据事件
```

#### 3. 验证 PI Vision

1. 打开 PI Vision 页面，添加使用 TDengine CDR 的 Value 控件
2. 观察 Value 是否每 60-120 秒自动更新
3. （可选）添加 Trend 控件，观察是否显示实时趋势

## CDR 配置说明（ConfigString）

CDR 的 ConfigString 格式为：

```
Server=<AF中配置的TDengine服务器名>;Database=<TDengine数据库>;Table=<表名>;Column=<列名>
```

示例：

```
Server=local;Database=zyyang2;Table=Meter_1000001_Current;Column=value
```

### 服务器配置

TDengine 服务器连接信息存储在 PI AF 的 `Configuration` 数据库中：

```
Configuration → TDengine → Servers → <服务器名>
```

每个服务器元素包含属性：

- `Host`：TDengine REST API 地址（如 `localhost`）
- `Port`：端口号（默认 `6041`）
- `EncryptedUsername` / `EncryptedPassword`：加密的认证信息
- `Is Cloud`：是否为 TDengine Cloud

## 调试踩坑记录

以下是调试过程中遇到的各种问题及其解决方案，供后续部署参考。

### 踩坑 1：签名方案选型

| 尝试的方案                                    | 是否有效 | 说明                     |
| --------------------------------------------- | -------- | ------------------------ |
| .NET Strong Naming（snk 签名）                | ❌       | PI AF 不检查 .NET 强名称 |
| `sn -Vr *,*` 跳过强名称验证                   | ❌       | 无关，问题不在强名称     |
| `RegPlugIn64.exe /Force` 本地注册             | ❌       | 不绕过 Authenticode 检查 |
| 修改 `Assembly2.loadproperties` 值            | ❌       | 该字段不控制签名验证     |
| **Authenticode 签名 + PlugInTrustedProvider** | ✅       | **正确方案**             |

### 踩坑 2：TDEventSource 类不是 public

这是最隐蔽的问题。`class TDEventSource`（默认 internal）导致 AF SDK 通过反射无法实例化，`CreateDataPipe()` 静默返回 null，**不报任何错误**。只有手动用 PowerShell `AFDataPipe` 测试才能发现事件数为 0。

**检查方法**：查看 `C:\temp\TDEventSource-debug.log` 是否存在。如果 `CreateDataPipe` 被调用但日志文件不存在，说明 `TDEventSource` 构造函数未执行——检查类的 `public` 修饰符。

### 踩坑 3：Newtonsoft.Json.dll 丢失

每次 `UploadPlugInAssembly` 创建新版本目录后，Newtonsoft.Json.dll 必须手动复制到 `4.0\` 子目录。`UploadPlugInSupportAssembly` API 存在但会把文件放到版本根目录而非 `4.0\` 子目录。

**部署检查清单**：上传 DLL 后，始终执行：

1. 找到最新版本目录
2. 确认 `4.0\` 子目录下同时存在 `TDEngineDR.dll` 和 `Newtonsoft.Json.dll`
3. 重启 AFService

### 踩坑 4：TDengine 保留字和大小写

- `value`、`status` 是 TDengine 保留字，SQL 中必须用反引号包裹
- taosx 创建子表时用了反引号保留了大小写（如 `Meter_1000001_Current`），查询时也必须用反引号，否则表名被转为小写导致 `Table does not exist`
- SQL 的 `from` 子句应使用 `` `数据库`.`表名` `` 全限定格式

### 踩坑 5：PI Vision CDR 的刷新机制（深入分析）

**重要结论**：PI Vision 对 Custom Data Reference (CDR) 使用 GetValue/PlotValues **轮询**模式，**不使用 DataPipe 通道**。轮询间隔**固定 120 秒**，由 `OSIsoft.PIVisualization.Data.DataService.dll` 中的 `PIDataQuery.DefaultFutureUpdatingExtraTime = 120` 硬编码控制，无法通过任何配置修改。

#### PI Vision 数据更新架构

PI Vision 的数据更新分为两层：

1. **前端 Data Pump**（`PV.data-pump.js`）：每 `UpdateRate` 秒（默认 5 秒）向后端发送数据请求。`UpdateRate` 来自 `ClientSettings.Default.UpdateRate`，定义在 `OSIsoft.PBViewer.Web.dll` 的 `ClientSettings` 类中。这不是瓶颈。

2. **后端数据服务**（`OSIsoft.PIVisualization.Data.DataService.dll`）：后端收到请求后，由 `UpdateManager` 和 `PIDataQuery` 控制是否真正去 AF SDK 拉取新数据。对 CDR 类型属性，轮询间隔由 `PIDataQuery.DefaultFutureUpdatingExtraTime = 120` 控制。

#### 关于 SlidingScaleIntervalRefresh（不适用于 CDR）

`SlidingScaleIntervalRefresh` 类根据显示时间范围动态计算刷新间隔（如 ≤30min 返回 0，2h 返回 90s）。**但实验证明该算法仅用于 PI Point 的 interval-based 查询，不适用于 CDR**。将时间范围缩短到 10 分钟（该函数返回 0），CDR 仍然是 120 秒刷新。

#### 120 秒轮询间隔的确切来源

通过反射扫描 DLL 中所有静态数值字段，找到关键常量：

```
UpdateManager.defaultUpdateRateSeconds = 5        ← 后端定时器每 5 秒触发
PIDataQuery.DefaultUpdatingPollInterval = 60      ← 默认轮询间隔 60 秒
PIDataQuery.DefaultFutureUpdatingExtraTime = 120  ← ⚠️ 关键：CDR 轮询间隔 120 秒
TagInfo.maxAttrCacheTime = 30                     ← 属性缓存 30 秒
```

CDR 调试日志（`C:\temp\TDEngineDR-debug.log`）铁证：每次 GetValue/PlotValues 调用间隔精确 120 秒，不受时间范围影响。

#### PI Point vs CDR 的刷新差异

- **原生 PI Point**：PI Vision 后端使用 PI Data Archive 的 Event Pipe（内核级推送机制），数据变化时立即推送，Trend 和 Value 控件都能 5 秒内更新
- **CDR（Custom Data Reference）**：PI Vision 后端使用 GetValue/PlotValues 轮询，间隔固定 120 秒（`DefaultFutureUpdatingExtraTime`），与时间范围无关

#### DataPipe 验证

DataPipe（`TDEventSource`）通过 AF SDK 直接调用（如 PowerShell `AFDataPipe`）验证可以正常工作（20 秒内获取 4 个事件），但 PI Vision 对 CDR 不使用 DataPipe 路径。

#### 已验证但无效的配置项

| 配置项 | 能否提高刷新频率 | 原因 |
|---|---|---|
| `AFDRSetting:TDEngine` = `DisableInputs` | ❌ | 仅控制写入权限 |
| `ClientSettings.Default.UpdateRate` = 5s | ❌ | 前端已足够快 |
| 缩短显示时间范围至 10m | ❌ | `SlidingScaleIntervalRefresh` 不适用于 CDR |
| PI Vision Admin 设置 | ❌ | 无刷新频率配置 |
| PIVision 数据库 Settings 表 | ❌ | 无轮询间隔字段 |

**结论**：CDR 的轮询间隔固定 120 秒，由 `PIDataQuery.DefaultFutureUpdatingExtraTime = 120` 硬编码控制，无法通过修改 CDR 代码、web.config、PI Vision Admin 或数据库配置来改变。详细分析见 [pi-vision-flush-problem.md](pi-vision-flush-problem.md)。

### 踩坑 6：`from from` 拼写错误

`TDPIStream.cs` 第 141 行 `RecordedValuesAtTimes()` 方法中有 `from from {Table}` 的拼写错误，导致调用此方法时 SQL 报语法错误。

## 生产部署注意事项

1. **代码签名证书**：正式环境建议使用正规代码签名证书（DigiCert、GlobalSign 等），而非自签名证书。证书只需创建一次，之后每次更新 DLL 用同一证书重新签名。
2. **PlugInTrustedProvider**：`name` 字段必须与签名证书的 Organization（`O=`）字段完全一致。如果客户已有企业代码签名证书，可直接使用。
3. **Newtonsoft.Json 部署**：建议将 Newtonsoft.Json.dll 的复制步骤集成到自动化部署脚本中，避免每次升级遗漏。
4. **版本缓存清理**：长期运行后 `C:\ProgramData\OSIsoft\AF\PlugIns\` 下会积累大量旧版本目录，可定期清理。
5. **调试日志**：生产环境部署前，应移除 `C:\temp\` 调试日志代码，避免磁盘空间问题。

## 完整部署脚本参考

```powershell
# 一键部署脚本（在 PI AF 服务器上以管理员运行）
param(
    [string]$DllPath = 'C:\workspace\taosx\plugins\pi\dist\TDEngineDR.dll',
    [string]$CertThumbprint,  # 首次部署后记录的证书指纹
    [string]$PIServerName = 'piserver'
)

# 签名
$cert = Get-ChildItem Cert:\LocalMachine\My | Where-Object { $_.Thumbprint -eq $CertThumbprint }
Set-AuthenticodeSignature -FilePath $DllPath -Certificate $cert -TimestampServer 'http://timestamp.digicert.com'

# 上传
[System.Reflection.Assembly]::LoadWithPartialName('OSIsoft.AFSDK') | Out-Null
$sys = New-Object OSIsoft.AF.PISystems
$pi = $sys[$PIServerName]
$pi.UploadPlugInAssembly($DllPath, $true)

# 复制 Newtonsoft.Json.dll 到新版本目录
$latest = Get-ChildItem 'C:\ProgramData\OSIsoft\AF\PlugIns' -Directory |
    Where-Object { $_.Name -match '1\.0\.' } |
    Sort-Object Name -Descending |
    Select-Object -First 1
$fourDir = Join-Path $latest.FullName '4.0'
$jsonSrc = Join-Path (Split-Path $DllPath) 'Newtonsoft.Json.dll'
if (Test-Path $jsonSrc) {
    Copy-Item $jsonSrc (Join-Path $fourDir 'Newtonsoft.Json.dll') -Force
} else {
    Write-Warning "找不到 Newtonsoft.Json.dll，请手动复制到 $fourDir"
}

# 重启服务
Restart-Service AFService
iisreset

Write-Host "部署完成。请刷新 PI Vision 页面验证。"
```
