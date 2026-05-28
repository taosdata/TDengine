# PI UOM（单位）与超级表命名规则

## 什么是 UOM

UOM（Unit of Measure，计量单位）是 PI System 中 PI Point 的元数据属性，描述该点位值的物理单位，例如 `volt`（伏特）、`ampere`（安培）、`watt`（瓦特）、`kilowatt hour`（千瓦时）等。

- UOM 是逐点位配置的，取决于 PI Point 本身在 PI System 中是否设置了单位，**与任务配置或连接模式无关**。
- 通过 AF（Asset Framework）访问的点位通常都有 UOM，因为 AF Element Template 在定义属性时会指定单位。
- 通过纯 PI Data Archive 访问的点位可能没有 UOM，取决于点位自身的配置。

## UOM 如何影响超级表命名

taosx 在生成 PI Data In 任务的 CSV 配置（单列模型 / Point Mode）时，根据 UOM 生成超级表名称：

| 是否有 UOM | 命名规则 | 示例 |
|---|---|---|
| 有 | `{uom}_{pi_type}` | `volt_float32`、`ampere_float64` |
| 无 | `ts_{pi_type}` | `ts_float32`、`ts_float64` |

命名逻辑在 `get_point_mode_stable_name()` 函数中：

```rust
// taosx-core/src/plugins/runners/pi/transform/mod.rs:179
fn get_point_mode_stable_name(pi_type: &str, uom: Option<&str>) -> String {
    let pi_type = pi_type.to_lowercase();
    if let Some(uom) = uom {
        let uom = uom.to_lowercase();
        let uom = uom.replace(|c: char| !c.is_ascii_alphanumeric(), "_");
        format!("{}_{}", uom, pi_type)
    } else {
        format!("ts_{}", pi_type)
    }
}
```

PI System 返回的 UOM 值是完整英文名称（如 `volt`、`ampere`、`kilowatt`），而非缩写（如 `V`、`A`、`kW`）。名称中的非字母数字字符会被替换为 `_`（如 `dollars per kilowatt hour` → `dollars_per_kilowatt_hour`）。

## 真实 PI System 中的 UOM 示例

| PI Point 属性 | UOM | UOMABB（缩写） | 生成的超级表名 |
|---|---|---|---|
| Voltage（电压） | `volt` | `V` | `volt_float32` |
| Current（电流） | `ampere` | `A` | `ampere_float32` |
| Power（功率） | `watt` | `W` | `watt_float32` |
| Energy（能量） | `kilowatt hour` | `kWh` | `kilowatt_hour_float32` |
| Wind Speed（风速） | `meter per second` | `m/s` | `meter_per_second_float32` |
| Cost Rate（费率） | `dollars per kilowatt hour` | `$/kWh` | `dollars_per_kilowatt_hour_single` |
| （无 UOM） | `null` | `null` | `ts_float32` |

## 对数据路由的影响

当存在 UOM 时，多张超级表可以拥有相同的 `$value` 数据类型（例如都是 FLOAT），但名称不同。例如一个包含 Voltage（V）、Current（A）、Power（W）属性的 PI System 会生成三张超级表：

```
volt_float32      — 存放电压点位
ampere_float32    — 存放电流点位
watt_float32      — 存放功率点位
```

三张超级表的 schema 完全相同（`ts TIMESTAMP`、`val FLOAT`、`status INT`，加上若干 TAG 列），仅超级表名不同。每个 PI Point 通过 CSV 中的 POINT 行指定所属的超级表：

```csv
Meter_001_Voltage,POINT,volt_float32
Meter_001_Current,POINT,ampere_float32
Meter_001_Power,POINT,watt_float32
```

## 已知问题（已修复）：C# 连接器的 `_using` 忽略 UOM

C# 连接器（`taosx-pi.exe`）在发送数据时，`_using` 字段始终使用 `ts_{pi_type}`（如 `ts_float32`），**不考虑 UOM**。这意味着当多张超级表共享相同数据类型时，taosx 无法仅凭 `_using` 值判断数据应写入哪张超级表。

**修复方案**：在 `LushModelConfig` 中新增 `point_super_table_mapping`（`HashMap<String, String>`），从 CSV 的 POINT 行构建 `point_name → super_table` 映射。当该映射非空时：

- **Tables 路径**（建表）：按 `point_super_table_mapping` 分组，为每个超级表分别创建超级表和子表。
- **Insert 路径**（写入）：按 `point_name` 列对 IPC 数据分组，将每组数据路由到正确的超级表进行 transform 和写入。

这样绕过了 C# 连接器 `_using` 的限制，直接使用 CSV 中已有的点位→超级表映射关系。

**相关源码：**
- `taosx-core/src/plugins/runners/pi/transform/mod.rs` — `get_point_mode_stable_name()`，CSV 生成逻辑
- `taosx-core/src/plugins/sink/lush.rs` — `LushModelConfig`：`super_table_name_mapping`（类型映射）+ `point_super_table_mapping`（点位映射）
- `taosx-core/src/plugins/sink/mod.rs` — IPC 数据处理，Tables/Insert 路径的分组路由逻辑
