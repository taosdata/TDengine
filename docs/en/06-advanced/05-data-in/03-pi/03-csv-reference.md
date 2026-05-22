---
title: "PI Model Configuration File Reference"
sidebar_label: "Model Configuration Reference"
---

This page provides a detailed description of the model configuration file (CSV format) used by PI data ingestion tasks, covering the complete definitions for both multi-column and single-column model formats.

## 1. Overview

The model configuration file is a CSV-format text file that defines the data mapping rules from the PI system to TDengine, including:

- The correspondence between PI data sources (PI Points or AF element templates) and TDengine supertables
- The mapping of PI attributes to TDengine columns (Metric columns, TAG columns)
- Data filter conditions
- Data transformation expressions

When creating a PI task in Explorer, click the **Download Default Configuration** button to automatically generate the default model configuration file. You can download it, edit it, and then upload to override the default configuration.

## 2. Multi-column Model Configuration File

The multi-column model maps one PI AF element to one TDengine subtable. All elements under one element template share the same supertable structure.

### 2.1 File Structure

A multi-column model configuration file consists of one or more **supertable definition blocks**, separated by blank lines. Each supertable definition block contains:

| Row | Format | Description |
| --- | ------ | ----------- |
| Supertable Name | `SuperTable,<name>` | TDengine supertable name |
| Subtable Name Rule | `SubTable,<template>` | Supports placeholders, e.g., `${element_name}_${element_id}` |
| Element Template | `Template,<template_name>` | Corresponds to the element template name in PI AF |
| Filter Condition | `Filter,<condition>` | Optional, used to filter elements |
| Column Definition | `<col_name>,KEY\|COLUMN\|TAG,<data_type>,<mapping_rule>` | Defines the mapping for each column |

### 2.2 Column Definition Details

| Column Type | Keyword | Description |
| ----------- | ------- | ----------- |
| Timestamp Column | `KEY` | Must have exactly one, data type is `TIMESTAMP` |
| Data Column | `COLUMN` | Corresponds to the value of a PI Point attribute |
| Tag Column | `TAG` | Corresponds to static attributes or metadata of the element |

### 2.3 Complete Example

The following configuration file defines two supertables: `metertemplate` (from the MeterTemplate template) and `farm` (from the Farm template).

```csv
SuperTable,metertemplate
SubTable,${element_name}_${element_id}
Template,MeterTemplate
Filter,
ts,KEY,TIMESTAMP,$ts
voltage,COLUMN,DOUBLE,$voltage
voltage_status,COLUMN,INT,$voltage_status
current,COLUMN,DOUBLE,$current
current_status,COLUMN,INT,$current_status
element_id,TAG,VARCHAR(100),$element_id
element_name,TAG,VARCHAR(100),$element_name
path,TAG,VARCHAR(100),$path
categories,TAG,VARCHAR(100),$categories

SuperTable,farm
SubTable,${element_name}_${element_id}
Template,Farm
Filter,
ts,KEY,TIMESTAMP,$ts
wind_speed,COLUMN,FLOAT,$wind_speed
wind_speed_status,COLUMN,INT,$wind_speed_status
power_production,COLUMN,FLOAT,$power_production
power_production_status,COLUMN,INT,$power_production_status
lost_power,COLUMN,FLOAT,$lost_power
lost_power_status,COLUMN,INT,$lost_power_status
farm_lifetime_production__weekly_,COLUMN,FLOAT,$farm_lifetime_production__weekly_
farm_lifetime_production__weekly__status,COLUMN,INT,$farm_lifetime_production__weekly__status
farm_lifetime_production__hourly_,COLUMN,FLOAT,$farm_lifetime_production__hourly_
farm_lifetime_production__hourly__status,COLUMN,INT,$farm_lifetime_production__hourly__status
element_id,TAG,VARCHAR(100),$element_id
element_name,TAG,VARCHAR(100),$element_name
path,TAG,VARCHAR(100),$path
categories,TAG,VARCHAR(100),$categories
```

### 2.4 Line-by-Line Explanation

Using the `metertemplate` supertable as an example:

- `SuperTable,metertemplate`: Defines the supertable name as `metertemplate`
- `SubTable,${element_name}_${element_id}`: Subtable name is composed of element name and element ID, e.g., `Meter001_12345`
- `Template,MeterTemplate`: Data comes from the element template named `MeterTemplate` in PI AF
- `Filter,`: No filter applied, syncs all elements under this template
- `ts,KEY,TIMESTAMP,$ts`: Timestamp column, takes the PI data timestamp
- `voltage,COLUMN,DOUBLE,$voltage`: Data column, takes the `voltage` attribute value of the element
- `voltage_status,COLUMN,INT,$voltage_status`: Data column, takes the quality status code of `voltage`
- `element_id,TAG,VARCHAR(100),$element_id`: Tag column, takes the unique ID of the element
- `path,TAG,VARCHAR(100),$path`: Tag column, takes the element's path in the AF hierarchy

### 2.5 Default Mapping Rules

For multi-column model tasks using AF Server:

- taosX maps **PI Point attributes** to TDengine **COLUMN** (Metric columns) by default
- taosX maps **other attributes** (static attributes) to TDengine **TAG** columns by default

## 3. Single-column Model Configuration File

The single-column model maps one PI Point to one TDengine subtable.

### 3.1 File Structure

The single-column model configuration file consists of two parts:

**Part 1: Supertable Definitions** (similar to multi-column model)

Defines the column structure of several supertables. The default generated configuration automatically groups points into different supertables by **UOM (engineering unit) + data type**.

#### Part 2: Point Mappings

Format: `<PointName>,POINT,<SuperTableName>`, defining which supertable each PI Point belongs to.

### 3.2 Complete Example

```csv
SuperTable,volt_float32
SubTable,${point_name}
Filter,
ts,KEY,TIMESTAMP,$ts
value,COLUMN,FLOAT,$value
status,COLUMN,INT,$status
path,TAG,VARCHAR(200),$path
point_name,TAG,VARCHAR(100),$point_name
ptclassname,TAG,VARCHAR(100),$ptclassname
sourcetag,TAG,VARCHAR(100),$sourcetag
tag,TAG,VARCHAR(100),$tag
descriptor,TAG,VARCHAR(100),$descriptor
exdesc,TAG,VARCHAR(100),$exdesc
engunits,TAG,VARCHAR(100),$engunits
pointsource,TAG,VARCHAR(100),$pointsource
step,TAG,VARCHAR(100),$step
future,TAG,VARCHAR(100),$future
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`

SuperTable,milliampere_float32
SubTable,${point_name}
Filter,
ts,KEY,TIMESTAMP,$ts
value,COLUMN,FLOAT,$value
status,COLUMN,INT,$status
path,TAG,VARCHAR(200),$path
point_name,TAG,VARCHAR(100),$point_name
ptclassname,TAG,VARCHAR(100),$ptclassname
sourcetag,TAG,VARCHAR(100),$sourcetag
tag,TAG,VARCHAR(100),$tag
descriptor,TAG,VARCHAR(100),$descriptor
exdesc,TAG,VARCHAR(100),$exdesc
engunits,TAG,VARCHAR(100),$engunits
pointsource,TAG,VARCHAR(100),$pointsource
step,TAG,VARCHAR(100),$step
future,TAG,VARCHAR(100),$future
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`

Meter_1000004_Voltage,POINT,volt_float32
Meter_1000004_Current,POINT,milliampere_float32
Meter_1000001_Voltage,POINT,volt_float32
Meter_1000001_Current,POINT,milliampere_float32
Meter_1000474_Voltage,POINT,volt_float32
Meter_1000474_Current,POINT,milliampere_float32
```

### 3.3 Line-by-Line Explanation

**Supertable Definition Part**:

- `SuperTable,volt_float32`: Supertable name is `volt_float32` (voltage-type float32 points)
- `SubTable,${point_name}`: Subtable name directly uses the point name
- `value,COLUMN,FLOAT,$value`: Data column, stores the point value
- `status,COLUMN,INT,$status`: Data column, stores the quality status code
- `point_name,TAG,VARCHAR(100),$point_name`: Tag column, stores the point name
- `engunits,TAG,VARCHAR(100),$engunits`: Tag column, stores the engineering unit
- `` element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")` ``: Tag column, uses an inline expression to replace the path separator from `\` to `.`

**Point Mapping Part**:

- `Meter_1000004_Voltage,POINT,volt_float32`: Point `Meter_1000004_Voltage` belongs to supertable `volt_float32`
- `Meter_1000004_Current,POINT,milliampere_float32`: Point `Meter_1000004_Current` belongs to supertable `milliampere_float32`

## 4. Mapping Rules and Expressions

### 4.1 Common Placeholders

The following placeholders can be used in the mapping rule of column definitions:

| Placeholder | Description | Applicable Scope |
| ----------- | ----------- | ---------------- |
| `$ts` | Data timestamp | Single/Multi-column |
| `$value` | Point value | Single-column model |
| `$status` | Quality status code | Single/Multi-column |
| `$point_name` | PI Point name | Single-column model |
| `$element_name` | AF element name | Multi-column model |
| `$element_id` | AF element unique ID | Multi-column model |
| `$path` | Element/point path | Single/Multi-column |
| `$categories` | AF element categories | Multi-column model |
| `$element_paths` | Point-associated element paths | Single-column model |
| `$<attribute_name>` | PI Point attribute or AF element attribute | Reference by actual attribute name |

### 4.2 Inline Expressions

For scenarios requiring data transformation, you can use backticks to wrap inline expressions:

```csv
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`
```

For more information about mapping rules and data transformation expression syntax, see the "Data Extraction, Filtering and Transformation" section in [Zero-Code Third-Party Data Ingestion](../).

### 4.3 Subtable Name Placeholders

The `SubTable` row template supports the following placeholders:

| Placeholder | Description |
| ----------- | ----------- |
| `${point_name}` | PI Point name (single-column model) |
| `${element_name}` | AF element name (multi-column model) |
| `${element_id}` | AF element unique ID (multi-column model) |

Placeholders can be combined, e.g., `${element_name}_${element_id}`.

## 5. Common Patterns and Best Practices

### 5.1 Group Supertables by UOM

The default generated single-column model configuration automatically groups by **UOM (engineering unit) + data type**. For example, all points with unit "Volt" and data type Float32 are grouped into the same supertable `volt_float32`.

This is the recommended default approach, ensuring that all subtables within the same supertable have exactly the same column structure.

### 5.2 Filter Specific Points or Templates

Fill in filter criteria in Explorer before clicking **Download Default Configuration** to generate configuration for only the points/templates matching the filter criteria.

You can also manually edit the CSV after downloading:

- **Multi-column model**: Modify the `Filter` row
- **Single-column model**: Delete unwanted point rows in the point mapping section

### 5.3 Customize Supertable Names

If the default UOM-based grouping naming does not meet your needs, you can directly modify the `SuperTable` row value and adjust the supertable references in the point mapping section.
