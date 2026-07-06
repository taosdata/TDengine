---
title: "Dataset Filter 配置"
sidebar_label: "Dataset Filter 配置"
---

本页说明 PI 数据接入任务中 **Dataset Filter** 的用法，包括三种过滤单位（point / element / template）的适用场景、通配符语法和常见示例。

## 1. 什么是 Dataset Filter

在 Explorer 中创建 PI 或 PI backfill 任务时，**数据模型配置**区域可以填写 Dataset Filter，用于在生成模型配置文件前筛选要同步的 PI Point 或 AF Element。

- 若希望同步全部点位或全部模板元素，使用默认配置即可。
- 若只需要同步命名符合特定规则的点位、元素或模板，可在点击 **下载默认配置** 前先填写 Dataset Filter。

## 2. System Configuration、Data Model 与 Dataset Filter 的关系

Dataset Filter 的可用选项取决于 **System Configuration**（连接方式）和 **Data Model**（数据模型）。

| System Configuration | Data Model | Dataset Filter 单位 | 说明 |
| --- | --- | --- | --- |
| PI Data Archive Only | 单列模型 | `point` | 直接按 PI Point 名称过滤 |
| PI Data Archive and AF Server | 单列模型 | `element` / `template` | 按 AF Element 路径或模板名过滤，最终每个 Point 仍落地为一张子表 |
| PI Data Archive and AF Server | 多列模型 | `element` / `template` | 按 AF Element 路径或模板名过滤，每个 Element 落地为一张子表 |

要点：

- **PI Data Archive Only** 不支持 AF，因此只能使用 `point` 过滤。
- **多列模型**依赖 AF 资产框架，因此只在 AF Server 模式下可用。
- **AF Server 的单列模型**使用 `element` 或 `template` 过滤，过滤后仍按 Point 粒度建子表；该模式不支持 `point` 过滤，因为 AF 单列需要 Element → Attribute 上下文来获取 UOM、Element 归属等元数据。

## 3. 通配符语法

三种 Dataset Filter 均支持以下通配符：

| 通配符 | 含义 |
| --- | --- |
| `*` | 匹配 0 个或多个字符 |
| `?` | 精确匹配 1 个字符 |

## 4. Point Filter

适用于 **System Configuration = PI Data Archive Only**，按 PI Point 名称过滤。

### 4.1 语法

- 仅支持名字模式，不支持路径写法。
- 表达式原样透传给 PI 后端，不会自动补 `*`。

### 4.2 示例

| 输入 | 含义 |
| --- | --- |
| `sinusoid` | 精确匹配名为 `sinusoid` 的 Point |
| `CDT15*` | 匹配以 `CDT15` 开头的 Point |
| `sinu?` | 匹配 `sinu` 后跟任意 1 个字符的 Point，如 `sinus` |

## 5. Element Filter

适用于 **System Configuration = PI Data Archive and AF Server**，按 AF Element 名称或路径过滤。它是三种 Filter 中**唯一支持路径匹配**的类型。

### 5.1 路径匹配规则

- 路径相对 AF 数据库根书写，**不要包含 AF Database 库名**。
- 前导 `\` 可有可无。
- 判断规则：看表达式中是否包含 `\`。
  - **不含 `\`**：整串作为元素名模式，在整库范围内按名称匹配。
  - **含 `\`**：在最后一个 `\` 处切开。
    - 左侧作为 `Root:` 路径（即父路径）。
    - 右侧作为 `Name:` 模式（为空时表示 `*`）。
    - 只返回叶子 Element（不含子元素的节点）。

### 5.2 示例

| 输入 | 含义 |
| --- | --- |
| `Meter_100001` | 精确匹配名为 `Meter_100001` 的 Element |
| `Meter_10000*` | 匹配以 `Meter_10000` 开头的 Element |
| `Meter_100000?` | 匹配 `Meter_100000` 后跟 1 个字符的 Element |
| `\California\San Diego\` | 匹配 `California\San Diego` 下的所有叶子 Element |
| `\California\Meter_*` | 匹配 `California` 子树下以 `Meter_` 开头的叶子 Element |
| `\California\San Diego\Meter_10000?` | 匹配指定路径下以 `Meter_10000` 开头且末尾为 1 个字符的叶子 Element |

### 5.3 注意事项

1. **圈选路径时，路径必须以 `\` 结尾**。例如 `\California\San Diego` 会被解析为在 `California` 下查找名为 `San Diego` 的叶子 Element，而 `San Diego` 通常是分组节点，结果可能为空。正确写法是 `\California\San Diego\`。
2. **路径中每一段必须是真实存在的精确节点名**，路径段本身不支持 `*` 或 `?` 通配。
3. **路径不要包含 AF Database 库名**。路径是相对库根书写的，误带库名会导致找不到 Element。
4. **路径模式只返回叶子 Element**。挂在非叶子分组节点上的 PI Point 不会被采集。

## 6. Template Filter

适用于 **System Configuration = PI Data Archive and AF Server**，按 AF Element Template 名称过滤。

### 6.1 语法

- 仅支持模板名字模式，不支持路径写法。
- 表达式原样透传给 PI 后端，不会自动补 `*`。
- 命中模板后，taosX 会自动采集所有套用该模板的 Element。

### 6.2 示例

| 输入 | 含义 |
| --- | --- |
| `MeterBasic` | 精确匹配名为 `MeterBasic` 的模板 |
| `Meter*` | 匹配以 `Meter` 开头的模板 |
| `Meter?` | 匹配 `Meter` 后跟 1 个字符的模板 |

## 7. Dataset Filter 与 CSV 配置文件的关系

Dataset Filter 只影响点击 **下载默认配置** 时生成的点位或元素范围。下载后的 CSV 模型配置文件中：

- **多列模型**：`Filter` 行保存的是 Dataset Filter 的值。
- **单列模型**：具体的 PI Point 会列在点位映射部分；如需进一步裁剪，可手动删除不需要的 Point 行。

## 8. 常见误区

| 误区 | 说明 |
| --- | --- |
| AF Server 单列模型支持 `point` 过滤 | 不支持。AF 单列需要从 Element → Attribute 获取 UOM、Element 归属等元数据，直接用 `point` 会丢失这些元数据。 |
| Element 路径段可以用 `*` 通配 | 不支持。`*` / `?` 只能用于最后一段名字。 |
| 路径中需要写 AF Database 名 | 不需要。路径相对库根书写，包含库名会报错。 |
| `point` / `template` 支持路径写法 | 不支持。只有 `element` 支持路径。 |
