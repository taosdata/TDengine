//! 从 PI 连接返回的点或元素的数据生成配置对象。
//! 从配置对象生成配置文件，或者从配置对象生成 transform 对象。
//!
//! ## 配置文件设计概述
//!
//! 数据模型配置文件是一个不规则的具有多重功能的 CSV 文件。
//! 这个配置文件的第一个功能是描述超级表的结构，第二个重要功能是描述 PI 中的点或元素到超级表的映射。
//! 我们用两个不同构的表格分别实现了上述两个功能。描
//! 述超级表的表格在上，描述映射的表格在下，它们被编辑在同一个 CSV 文件。
//! 这两个表格都没有表头，每一列的含义会在文件的注释部分说明。
//! 配置文件包含了很多超级表的定义。
//! 我们定义了一些功能性的关键词来即用来标记某个超级表定义的开始，同时也用来完成一些特殊的功能。
//! 这些关键词都必须出现在超级表 schema 正式开始之前。
//! 第一个关键词是 “SuperTable”，它表示一个超级表定义的开始，它的右边紧跟这个超级表的名字。所以它有“标记开始”和“定义超级表名”两个作用。
//! 第二个关键词是“SubTable”，它出现的位置必须是 “SupterTable” 关键词行之后，超级表结构定义开始之前。它的作用是表示子表名映射规则。比如对于单列模型，默认的子表名是 $point_name, 你可以增加在  point_name 前后增加前缀或后缀。配置文件中所有以 $ 开头的值为对源数据中某个属性的引用。如果是单列模式的数据，$point_name 就是一个内置的属性。还有很多其它内置属性，在“单列模型配置文件”一节会做详细说明。
//! 第三个关键词“Filter”，它出现的位置同样必须是 “SupterTable” 关键词行之后，超级表结构定义开始之前。它定义了数据入库前的过滤规则。
//! 第四个关键词是“Template”，它出现的位置同样是“SupterTable” 关键词行之后，超级表结构定义开始之前。它定义了数据入库前的过滤规则。它只出现在多列模型的配置文件中，仅用来表示自动生成这个超级表定义的时候，参考的是 PI 系统中的哪个 Template。这个关键词是可选的。我们给用户自由从头开始自定义一个超级表，不参考任何已有的 Template。
//! 下面重点描述 schema 定义部分。这一部分为 4 列。
//! 第一列为列名；
//! 第二列为列类型分为：KEY、COLUMN和TAG。
//! 第三列为列的数据类型，为 TDengine 支持的数据类型。
//! 第四列本质上不属于 schema 定义，而是 transform 规则。
//! 在定义完超级表之后，对于单列模型配置文件，后面是点位列表；对于多列模型配置文件，后面是元素列表。
//! 点位列表的每一行都有关键字是 “POINT”，元素列表的每一行都有关键字 “ELEMENT”。
//! TAG 列默认类型为 NCHAR(100).
//! 最后需要说明的是，所有关键字都不区分大小写。
//
use crate::plugins::transform::filter::expr::ExprRecordFilter;
use crate::plugins::transform::filter::{Filter, FilterImpl};
use crate::plugins::transform::map::expr::ExprValueBuilder;
use crate::plugins::transform::modeler::{Modeler, Table};
use crate::plugins::transform::mutate::Mutate;
use crate::{
    expr::Expr,
    plugins::transform::{
        map::{FieldValue, FieldValueBuilder, Map},
        Parser, TableOptions,
    },
};
use anyhow::anyhow;
use linked_hash_map::LinkedHashMap;
use std::fmt::{self, Display};
use std::iter::{Peekable, SkipWhile};
use std::str::Lines;
use taosx_ipc::stream::writer::IpcDataType;

const PRE_ADDED_LABELS_FOR_ELEMENT: [&str; 4] =
    ["element_id", "element_name", "path", "categories"];
/// 单列模型配置对象
/// 从配置对象可以生成 csv 配置文件，反之亦然。
/// 从配置对象也可以生成 transform 相关对象。
#[derive(Debug)]
pub struct PIPointModelConfig {
    pub super_tables: Vec<SuperTableConfig>,
    pub points: Vec<PointRow>,
}

impl PIPointModelConfig {
    pub fn to_csv(&self) -> String {
        format!("{}", self)
    }

    /// 从 CSV 配置文件解析单列模型的数据，生成单列模型配置对象
    pub fn from_csv(file_name: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(file_name)?;
        let (super_table_csv_lines, point_lines) = split_csv_config(content, ",point,");
        let mut super_tables = Vec::<SuperTableConfig>::new();
        for csv in super_table_csv_lines {
            let super_table = SuperTableConfig::from_csv(csv)?;
            super_tables.push(super_table);
        }
        let mut points = Vec::<PointRow>::new();
        for csv in point_lines {
            let point = PointRow::from_csv(csv)?;
            points.push(point);
        }
        Ok(PIPointModelConfig {
            super_tables,
            points,
        })
    }

    /// 从连接器返回的单列模型的 JSON 数据，生成单列模型配置对象
    /// # Arguments
    /// * `point_data` - 从 PI 连接返回的点位数据
    /// * `is_af` - 是否是 AF 单列模式
    pub fn from_json(point_data: &str, is_af: bool) -> anyhow::Result<Self> {
        let point_data: serde_json::Value = serde_json::from_str(point_data)?;
        let super_tables = Self::parse_super_tables(&point_data, is_af)?;
        let points = Self::parse_points(&point_data)?;
        Ok(PIPointModelConfig {
            super_tables,
            points,
        })
    }

    fn parse_super_tables(
        point_data: &serde_json::Value,
        is_af: bool,
    ) -> anyhow::Result<Vec<SuperTableConfig>> {
        // 一个 Template 对应到一个 SuperTableConfig
        let templates = point_data["Templates"].as_array().unwrap();
        let super_tables: Vec<SuperTableConfig> = templates
            .iter()
            .map(|template| {
                let pi_type = template["Type"].as_str().unwrap();
                let uom = template["UOM"].as_str();
                let super_table_name = Self::get_point_mode_stable_name(pi_type, uom);
                let sub_table_name_pattern = "${point_name}".to_string();
                let tags = template["Tags"].as_object().unwrap();
                let mut schema = vec![
                    // 三个固定列
                    SchemaRow {
                        column_name: "ts".to_string(),
                        column_type: ColumnType::Key,
                        column_data_type: "TIMESTAMP".to_string(),
                        column_map: "$ts".to_string(),
                    },
                    SchemaRow {
                        column_name: "value".to_string(),
                        column_type: ColumnType::COLUMN,
                        column_data_type: template["TDType"].as_str().unwrap().to_string(),
                        column_map: "$value".to_string(),
                    },
                    SchemaRow {
                        column_name: "status".to_string(),
                        column_type: ColumnType::COLUMN,
                        column_data_type: "INT".to_string(),
                        column_map: "$status".to_string(),
                    },
                ];
                // 追加两固定 TAG
                schema.push(SchemaRow {
                    column_name: "path".to_string(), // 点的路径
                    column_type: ColumnType::TAG,
                    column_data_type: "VARCHAR(200)".to_string(),
                    column_map: "$path".to_string(),
                });
                schema.push(SchemaRow {
                    column_name: "point_name".to_string(), // 点的名称
                    column_type: ColumnType::TAG,
                    column_data_type: "VARCHAR(100)".to_string(),
                    column_map: "$point_name".to_string(),
                });
                // 追加内置 Tag 列
                for (tag_name, _) in tags {
                    schema.push(SchemaRow {
                        column_name: tag_name.to_string(),
                        column_type: ColumnType::TAG,
                        column_data_type: "VARCHAR(100)".to_string(),
                        column_map: format!("${}", tag_name),
                    });
                }
                // 对于 AF 单列模式，追加 1 个固定 TAG 列
                if is_af {
                    schema.push(SchemaRow {
                        column_name: "element_paths".to_string(),
                        column_type: ColumnType::TAG,
                        column_data_type: "VARCHAR(512)".to_string(),
                        column_map: r#"`$element_paths.replace("\\", ".")`"#.to_string(),
                    });
                }
                SuperTableConfig {
                    super_table_name,
                    sub_table_name_pattern,
                    template_name: None,
                    filter: None,
                    schema,
                }
            })
            .collect();
        Ok(super_tables)
    }

    /// 如果包含 UOM 则使用 UOM 加 类型作为超级表名，否则使用 pi_{Type} 作为超级表名
    #[inline]
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

    fn parse_points(point_data: &serde_json::Value) -> anyhow::Result<Vec<PointRow>> {
        let points: Vec<PointRow> = point_data["Points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|point| {
                let super_table = Self::get_point_mode_stable_name(
                    point["Type"].as_str().unwrap(),
                    point["UOM"].as_str(),
                );
                PointRow {
                    point_name: point["Name"].as_str().unwrap().to_string(),
                    super_table,
                }
            })
            .collect();
        Ok(points)
    }
}

impl Display for PIPointModelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for super_table in &self.super_tables {
            writeln!(f, "\nSuperTable,{}", super_table.super_table_name)?;
            writeln!(f, "SubTable,{}", super_table.sub_table_name_pattern)?;
            if let Some(template_name) = &super_table.template_name {
                writeln!(f, "Template,{}", template_name)?;
            }

            if let Some(filter) = &super_table.filter {
                writeln!(f, "Filter,{}", filter)?;
            } else {
                writeln!(f, "Filter,")?;
            }

            for schema_row in &super_table.schema {
                writeln!(
                    f,
                    "{},{},{},{}",
                    schema_row.column_name,
                    schema_row.column_type,
                    schema_row.column_data_type,
                    schema_row.column_map
                )?;
            }
        }
        writeln!(f, "\n")?;
        for point in &self.points {
            writeln!(f, "{},POINT,{}", point.point_name, point.super_table)?;
        }
        Ok(())
    }
}

/// 多列模型配置对象
/// 从配置对象可以生成配置文件，反之亦然。
/// 从配置对象也可以生成 transform 相关对象。
#[derive(Debug)]
pub struct PIElementModelConfig {
    pub super_tables: Vec<SuperTableConfig>,
    // pub elements: Vec<ElementRow>,
}

impl PIElementModelConfig {
    pub fn to_csv(&self) -> String {
        format!("{}", self)
    }

    /// 从 CSV 配置文件解析多列模型的数据，生成多列模型配置对象
    pub fn from_csv(file_name: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(file_name)?;
        // 第一步：将配置文件切分成不同的超级表定义和元素列表
        let (super_table_csv_lines, _) = split_csv_config(content, ",element,");
        // 第二步：将第一步切分好的各个部分逐个解析成对象
        let mut super_tables = Vec::<SuperTableConfig>::new();
        for csv in super_table_csv_lines {
            let super_table = SuperTableConfig::from_csv(csv)?;
            Self::check_schema(&super_table.schema)?;
            super_tables.push(super_table);
        }
        // let mut elements = Vec::<ElementRow>::new();
        // for csv in element_lines {
        //     let element = ElementRow::from_csv(csv)?;
        //     elements.push(element);
        // }
        Ok(PIElementModelConfig {
            super_tables,
            // elements,
        })
    }

    fn check_schema(schema: &[SchemaRow]) -> anyhow::Result<()> {
        let mut column_names: LinkedHashMap<&str, ()> = LinkedHashMap::new();
        let mut has_element_id = false;
        for row in schema {
            let column_name = row.column_name.as_str();
            if column_names.contains_key(column_name) {
                return Err(anyhow!("Duplicate column name: {}", column_name));
            }
            column_names.insert(column_name, ());
            if column_name == "element_id" && row.column_type == ColumnType::TAG {
                has_element_id = true;
            }
        }
        if !has_element_id {
            return Err(anyhow!("Missing element_id in TAGs"));
        }

        Ok(())
    }

    /// 解析多列模型的数据，生成多列模型配置对象
    /// # Arguments
    /// * `element_data` - 从 PI 连接返回的元素数据
    pub fn from_json(element_data: &str) -> anyhow::Result<Self> {
        let element_data: serde_json::Value = serde_json::from_str(element_data)?;
        let super_tables = Self::parse_super_tables(&element_data)?;
        // let mut elements: Vec<ElementRow> = Self::parse_elements(&element_data)?;
        // Self::append_single_elements(&mut super_tables, &mut elements, &element_data);
        Ok(PIElementModelConfig {
            super_tables,
            // elements,
        })
    }

    #[inline]
    fn parse_super_tables(
        element_data: &serde_json::Value,
    ) -> anyhow::Result<Vec<SuperTableConfig>> {
        // 一个 Template 对应到一个 SuperTableConfig
        let super_tables: Vec<SuperTableConfig> = element_data["Templates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|template| {
                let template_name = template["TemplateName"].as_str().unwrap();
                let super_table_name = Self::default_stable_name(template_name);
                let sub_table_name_pattern = "${element_name}_${element_id}".to_string();
                let mut schema: Vec<SchemaRow> = Vec::new();
                // 添加主键列
                schema.push(SchemaRow {
                    column_name: "ts".to_string(),
                    column_type: ColumnType::Key,
                    column_data_type: "TIMESTAMP".to_string(),
                    column_map: "$ts".to_string(),
                });
                // 追加普通列
                let attributes = template["Attributes"].as_array().unwrap();
                if attributes.is_empty() {
                    // 添加一个伪列，否则无法建表
                    schema.push(SchemaRow {
                        column_name: "_c1".to_string(),
                        column_type: ColumnType::COLUMN,
                        column_data_type: "INT".to_string(),
                        column_map: "0".to_string(),
                    });
                } else {
                    for attribute in attributes {
                        let column_name = Self::attribute_name_to_column_name(
                            attribute["Name"].as_str().unwrap(),
                        );
                        schema.push(SchemaRow {
                            column_name: column_name.clone(),
                            column_type: ColumnType::COLUMN,
                            column_data_type: attribute["Type"].as_str().unwrap().to_string(),
                            column_map: format!("${}", column_name),
                        });
                        schema.push(SchemaRow {
                            column_name: column_name.clone() + "_status",
                            column_type: ColumnType::COLUMN,
                            column_data_type: "INT".to_string(),
                            column_map: format!("${}_status", column_name),
                        });
                    }
                }
                // 追加固定 TAG 列
                for label in &PRE_ADDED_LABELS_FOR_ELEMENT {
                    schema.push(SchemaRow {
                        column_name: label.to_string(),
                        column_type: ColumnType::TAG,
                        column_data_type: "VARCHAR(100)".to_string(),
                        column_map: format!("${}", label),
                    });
                }
                // 追加其它静态属性作为 Tag 列
                let static_attributes = template["StaticAttributes"].as_array().unwrap();
                for attribute in static_attributes {
                    let column_name =
                        Self::attribute_name_to_column_name(attribute["Name"].as_str().unwrap());
                    let lower_name = column_name.to_lowercase();
                    if PRE_ADDED_LABELS_FOR_ELEMENT.contains(&lower_name.as_str()) {
                        continue;
                    }
                    if lower_name.contains("path") {
                        schema.push(SchemaRow {
                            column_name: column_name.clone(),
                            column_type: ColumnType::TAG,
                            column_data_type: "VARCHAR(200)".to_string(),
                            column_map: format!("${}", column_name),
                        });
                    } else {
                        schema.push(SchemaRow {
                            column_name: column_name.clone(),
                            column_type: ColumnType::TAG,
                            column_data_type: "VARCHAR(50)".to_string(),
                            column_map: format!("${}", column_name),
                        });
                    }
                }
                SuperTableConfig {
                    super_table_name,
                    sub_table_name_pattern,
                    template_name: Some(template_name.to_string()),
                    filter: None,
                    schema,
                }
            })
            .collect();

        Ok(super_tables)
    }

    /// Attribute 名字转列名
    #[inline]
    fn attribute_name_to_column_name(attribute_name: &str) -> String {
        let column_name = attribute_name.to_lowercase();
        column_name.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
    }

    /// 模板名转超级表名
    #[inline]
    pub fn default_stable_name(template_name: &str) -> String {
        template_name
            .to_lowercase()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
    }

    // #[inline]
    // fn parse_elements(element_data: &serde_json::Value) -> anyhow::Result<Vec<ElementRow>> {
    //     let elements = element_data["Elements"]
    //         .as_array()
    //         .unwrap()
    //         .iter()
    //         .map(|element| {
    //             let template_name = element["TemplateName"].as_str().unwrap();
    //             let super_table = Self::default_stable_name(template_name);
    //             ElementRow {
    //                 element_name: element["Name"].as_str().unwrap().to_string(),
    //                 super_table: super_table,
    //                 element_id: element["ID"].as_str().unwrap().to_string(),
    //                 path: element["Path"].as_str().map(|s| s.to_string()),
    //             }
    //         })
    //         .collect();
    //     Ok(elements)
    // }

    // 处理 SingleElements
    // fn append_single_elements(
    //     super_tables: &mut Vec<SuperTableConfig>,
    //     elements: &mut Vec<ElementRow>,
    //     element_data: &serde_json::Value,
    // ) {
    //     let single_elements = element_data["SingleElements"].as_array().unwrap();
    //     for element in single_elements {
    //         let element_name = element["Name"].as_str().unwrap();
    //         let element_id = element["ID"].as_str().unwrap();
    //         let super_table_name =
    //             Self::default_stable_name(element_name) + "_" + element_id;
    //         let path = element["Path"].as_str().map(|s| s.to_string());
    //         // 添加到 Element列表
    //         elements.push(ElementRow {
    //             element_name: element_name.to_string(),
    //             super_table: super_table_name.clone(),
    //             element_id: element_id.to_string(),
    //             path,
    //         });
    //         let mut schema: Vec<SchemaRow> = Vec::new();
    //         // 添加主键列
    //         schema.push(SchemaRow {
    //             column_name: "ts".to_string(),
    //             column_type: ColumnType::Key,
    //             column_data_type: "TIMESTAMP".to_string(),
    //             column_map: "$ts".to_string(),
    //         });
    //         // 追加普通列
    //         let attributes = element["Attributes"].as_array().unwrap();
    //         for attribute in attributes {
    //             let column_name = attribute["Name"].as_str().unwrap();
    //             schema.push(SchemaRow {
    //                 column_name: column_name.to_string(),
    //                 column_type: ColumnType::COLUMN,
    //                 column_data_type: attribute["Type"].as_str().unwrap().to_string(),
    //                 column_map: format!("${}", column_name),
    //             });
    //         }
    //         // 追加其它静态属性作为 Tag 列
    //         let static_attributes = element["StaticAttributes"].as_array().unwrap();
    //         for attribute in static_attributes {
    //             let column_name = attribute["Name"].as_str().unwrap();
    //             schema.push(SchemaRow {
    //                 column_name: column_name.to_string(),
    //                 column_type: ColumnType::TAG,
    //                 column_data_type: "NCHAR(100)".to_string(),
    //                 column_map: format!("${}", column_name),
    //             });
    //         }
    //         // 添加到超级表列表
    //         super_tables.push(SuperTableConfig {
    //             super_table_name,
    //             sub_table_name_pattern: "$element_id".to_string(),
    //             template_name: None,
    //             filter: None,
    //             schema,
    //         });
    //     }
    // }
}

/// 解析配置文件，将其切分成超级表定义和元素列表
/// # Arguments
/// * `content` - 配置文件内容
/// * `object_filter` - 用于过滤出对象列表的关键字，对于单列模型是 "point"，对于多列模型是 "element"
/// # Returns
/// * 返回一个元组，第一个元素是超级表定义列表，第二个元素是对象列表
fn split_csv_config(content: String, object_filter: &str) -> (Vec<String>, Vec<String>) {
    let lines: Lines<'_> = content.lines();
    let lines: SkipWhile<Lines, _> =
        lines.skip_while(|line| line.trim().is_empty() || line.trim().starts_with('#'));
    let mut super_table_csv_lines = Vec::<String>::new();
    let mut object_lines = Vec::<String>::new();
    let mut current_super_table = String::new();
    let mut peeker: Peekable<SkipWhile<Lines, _>> = lines.peekable();
    loop {
        let line = peeker.next();
        let line = line.map(|line| line.trim());
        match line {
            Some(line) => {
                let lower_line = line.to_lowercase();
                // 非 element 即 supertable
                if lower_line.contains(object_filter) {
                    object_lines.push(line.to_string());
                } else if !line.is_empty() {
                    current_super_table.push_str(line);
                    current_super_table.push('\n');
                }
                // 观察下一行，判断当前的 suptertable 是否结束
                let next_line = peeker.peek();
                let next_line = next_line.map(|line| line.trim());
                match next_line {
                    Some(next_line) => {
                        let lower_next_line = next_line.to_lowercase();
                        if lower_next_line.starts_with("supertable") {
                            current_super_table.pop(); // remove the last '\n'
                            super_table_csv_lines.push(current_super_table);
                            current_super_table = String::new();
                        }
                    }
                    None => {
                        super_table_csv_lines.push(current_super_table);
                        break;
                    }
                }
            }
            None => {
                unreachable!();
            }
        }
    }
    (super_table_csv_lines, object_lines)
}
impl Display for PIElementModelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for super_table in &self.super_tables {
            writeln!(f, "\nSuperTable,{}", super_table.super_table_name)?;
            writeln!(f, "SubTable,{}", super_table.sub_table_name_pattern)?;
            if let Some(template_name) = &super_table.template_name {
                writeln!(f, "Template,{}", template_name)?;
            }

            if let Some(filter) = &super_table.filter {
                writeln!(f, "Filter,{}", filter)?;
            } else {
                writeln!(f, "Filter,")?;
            }

            for schema_row in &super_table.schema {
                writeln!(
                    f,
                    "{},{},{},{}",
                    schema_row.column_name,
                    schema_row.column_type,
                    schema_row.column_data_type,
                    schema_row.column_map
                )?;
            }
        }
        // writeln!(f)?;
        // for element in &self.elements {
        //     if element.path.is_some() {
        //         writeln!(
        //             f,
        //             "{},{},{},{}",
        //             element.element_name,
        //             "ELEMENT",
        //             element.super_table,
        //             element.element_id,
        //             // element.path.as_ref().unwrap()
        //         )?;
        //     } else {
        //         writeln!(
        //             f,
        //             "{},{},{},{}",
        //             element.element_name, "ELEMENT", element.super_table, element.element_id,
        //         )?;
        //     }
        // }
        Ok(())
    }
}

/// 配置文件 schema 定义部分第 2 列的类型
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    TAG,
    COLUMN,
    Key,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::TAG => write!(f, "TAG"),
            ColumnType::COLUMN => write!(f, "COLUMN"),
            ColumnType::Key => write!(f, "KEY"),
        }
    }
}

/// 代表配置文件中 schema 定义部分的一行
#[derive(Debug, Clone)]
pub struct SchemaRow {
    // schema 部分第 1 列
    pub column_name: String,
    // schema 部分第 2 列
    pub column_type: ColumnType,
    // schema 部分第 3 列
    pub column_data_type: String,
    // schema 部分第 4 列
    pub column_map: String,
}

use std::str::FromStr;
impl SchemaRow {
    /// 从配置文件的一行生成一个 FieldValue 对象
    fn try_to_map_field(&self) -> Option<FieldValue> {
        let column_name = self.column_name.as_str();
        let column_td_type = self.column_data_type.as_str();
        // 临时代码，测试发现
        // 1. 如果对 Timestamp 再 cast，且目标类型为 Timestamp 值会变成 null
        // 2. 如果对 Timestamp 做 cast，且目标类型为 None，则入库类型会是 “NULL”
        // 所以暂时忽略 Timestamp 类型的列
        if column_td_type.to_lowercase().contains("timestamp") {
            return None;
        }
        let column_expr = self.column_map.as_str();
        let column_expr = column_expr.replace('$', "");
        if column_expr == column_name {
            return None;
        }
        let expr = Expr::try_new(column_expr, true).ok()?;
        let expr_builder = ExprValueBuilder::new(expr);
        let field_value_builder = FieldValueBuilder::Expr(expr_builder);
        let ipc_data_type = match IpcDataType::from_str(column_td_type) {
            Ok(ipc_data_type) => Some(ipc_data_type),
            Err(err) => {
                tracing::error!(
                    "Invalid data type: {} for column {}. Err:{}",
                    column_td_type,
                    column_name,
                    err
                );
                None
            }
        };
        Some(FieldValue::new(field_value_builder, ipc_data_type))

        // if column_expr[1..] == column_name[..] {
        //     None
        // } else {
        //     let column_expr = column_expr.replace('$', "");
        //     let expr = Expr::try_new(column_expr, true).ok()?;
        //     let expr_builder = ExprValueBuilder::new(expr);
        //     let field_value_builder = FieldValueBuilder::Expr(expr_builder);
        //     Some(FieldValue::new(field_value_builder, None))
        // }
    }
}

/// 代表配置文件中的一个超级表, 即： 以 SuperTable 关键词开头的一段
#[derive(Debug, Clone)]
pub struct SuperTableConfig {
    // 超级表名
    pub super_table_name: String,
    // 子表名映射规则
    pub sub_table_name_pattern: String,
    // 关联的模板名
    pub template_name: Option<String>,
    // 过滤规则
    pub filter: Option<String>,
    // schema 部分
    pub schema: Vec<SchemaRow>,
}

/// split line by ','
/// if the "," is between ``, it will be ignored, and the "," will be treated as a part of the string
fn split_csv_line(line: &str) -> Vec<&str> {
    let mut parts = Vec::<&str>::new();
    let mut start = 0;
    let mut in_quote = false;
    for (i, c) in line.char_indices() {
        match c {
            ',' => {
                if !in_quote {
                    parts.push(&line[start..i]);
                    start = i + 1;
                }
            }
            '`' => {
                in_quote = !in_quote;
            }
            _ => {}
        }
    }
    let sec = &line[start..];
    // trime the qutoe at beginning and end
    let pat = '`';
    if sec.starts_with(pat) && sec.ends_with(pat) {
        let set = sec.trim_matches(pat);
        parts.push(set);
    } else {
        parts.push(sec);
    }
    parts
}

impl SuperTableConfig {
    fn from_csv(csv: String) -> anyhow::Result<Self> {
        let lines = csv.lines();
        let mut super_table_name: Option<String> = None;
        let mut sub_table_name_pattern: Option<String> = None;
        let mut template_name: Option<String> = None;
        let mut filter: Option<String> = None;
        let mut schema: Vec<SchemaRow> = Vec::<SchemaRow>::new();
        for line in lines {
            let parts = split_csv_line(line);
            let part_0 = parts[0].to_lowercase();
            match part_0.as_str() {
                "supertable" => {
                    super_table_name = Some(parts[1].to_string());
                }
                "subtable" => {
                    sub_table_name_pattern = Some(parts[1].to_string());
                }
                "template" => {
                    template_name = Some(parts[1].to_string());
                }
                "filter" => {
                    let filter_expr = parts[1].trim();
                    let filter_expr = filter_expr.replace('$', "");
                    if !filter_expr.is_empty() {
                        filter = Some(filter_expr);
                    }
                }
                _ => {
                    if parts.len() < 4 {
                        return Err(anyhow::anyhow!(
                            "Invalid schema row, expect 4 columns: {}",
                            line
                        ));
                    }
                    let column_name = parts[0].to_string();
                    let obj_type = parts[1].to_string();
                    let obj_type = obj_type.to_lowercase();
                    let column_type = match obj_type.as_str() {
                        "tag" => ColumnType::TAG,
                        "column" => ColumnType::COLUMN,
                        "key" => ColumnType::Key,
                        _ => return Err(anyhow::anyhow!("Invalid column type {}", parts[1])),
                    };
                    let column_data_type = parts[2].to_string();
                    let column_map = parts[3].to_string();
                    schema.push(SchemaRow {
                        column_name,
                        column_type,
                        column_data_type,
                        column_map,
                    });
                }
            }
        }
        if super_table_name.is_none() {
            return Err(anyhow::anyhow!("SuperTable name is required"));
        }
        if sub_table_name_pattern.is_none() {
            return Err(anyhow::anyhow!("SubTable name pattern is required"));
        }
        let super_table_name = super_table_name.unwrap();
        let sub_table_name_pattern = sub_table_name_pattern.unwrap();
        Ok(SuperTableConfig {
            super_table_name,
            sub_table_name_pattern,
            template_name,
            filter,
            schema,
        })
    }

    /// 从超级表的配置中获取需要做 transform 的列（目前仅支持“映射”类型的 transform）
    fn get_map_transform(&self) -> Option<Map> {
        let mut map: LinkedHashMap<String, FieldValue> = LinkedHashMap::new();
        for row in &self.schema {
            if let Some(field_value) = row.try_to_map_field() {
                map.insert(row.column_name.clone(), field_value);
            }
        }
        if map.is_empty() {
            None
        } else {
            Some(Map::new(map))
        }
    }

    fn get_filter(&self) -> Option<Filter> {
        match &self.filter {
            Some(filter) => {
                let filter_impl = FilterImpl::Expr(ExprRecordFilter::new(filter.to_string()));
                Some(Filter::new(vec![filter_impl]))
            }
            None => None,
        }
    }

    fn get_table_model(&self) -> Table {
        let mut columns = Vec::<String>::new();
        let mut tags = Vec::<String>::new();
        for row in &self.schema {
            match row.column_type {
                ColumnType::TAG => tags.push(row.column_name.clone()),
                _ => columns.push(row.column_name.clone()),
            }
        }

        let options = std::sync::OnceLock::<std::sync::Arc<TableOptions>>::new();
        // let sub_table_name = self.sub_table_name_pattern.replace('$', "");
        // let sub_table_name = format!("${{{}}}", sub_table_name);
        Table {
            name: self.sub_table_name_pattern.clone(),
            using: Some(self.super_table_name.clone()),
            tags: Some(tags),
            columns: Some(columns),
            r#where: None,
            global: options,
        }
    }

    pub fn get_sql(&self) -> String {
        let mut sql = format!("create stable if not exists `{}` (", self.super_table_name);
        for row in &self.schema {
            match row.column_type {
                ColumnType::COLUMN | ColumnType::Key => {
                    sql.push_str(&format!("`{}` {},", row.column_name, row.column_data_type));
                }
                _ => {}
            }
        }
        sql.pop();
        sql.push(')');
        sql.push_str(" tags (");
        for row in &self.schema {
            if row.column_type == ColumnType::TAG {
                sql.push_str(&format!("`{}` {},", row.column_name, row.column_data_type));
            }
        }
        sql.pop();
        sql.push(')');
        sql
    }
}

impl From<SuperTableConfig> for Parser {
    fn from(val: SuperTableConfig) -> Self {
        let map = val.get_map_transform();
        let filter = val.get_filter();
        let mut mutate = Vec::<Mutate>::new();
        if let Some(filter) = filter {
            mutate.push(Mutate::Filter(filter));
        }
        if let Some(map) = map {
            mutate.push(Mutate::Map(map));
        }
        let table = val.get_table_model();
        let model = Modeler::new(vec![table]);
        // let parse: ParserImpl = serde_json::from_str(
        //     r#"{
        //         "ts": {"as": "TIMESTAMP(ms)"},
        //         "value": {"as": "FLOAT"},
        //         "status": {"as": "INT"},
        //         "path": {"as": "NCHAR(100)"},
        //         "tag": {"as": "NCHAR(100)"},
        //         "descriptor": {"as": "NCHAR(100)"},
        //         "exdesc": {"as": "NCHAR(100)"},
        //         "engunits": {"as": "NCHAR(100)"},
        //         "pointsource": {"as": "NCHAR(100)"},
        //         "step": {"as": "NCHAR(100)"},
        //         "future": {"as": "NCHAR(100)"},
        //         "element_paths": {"as": "NCHAR(100)"}
        //     }"#,
        // )
        // .expect("Deserialize ParserImpl failed");
        // Parser::new(Some(parse), mutate, model)
        Parser::new(None, mutate, model)
    }
}

/// 代表单列模型配置文件点位列表部分的一行
#[derive(Debug)]
pub struct PointRow {
    pub point_name: String,
    pub super_table: String,
}

impl PointRow {
    fn from_csv(csv: String) -> anyhow::Result<Self> {
        let parts = csv.split(',').collect::<Vec<&str>>();
        let obj_type = parts[1].to_lowercase();
        let obj_type = obj_type.as_str();
        if obj_type != "point" {
            return Err(anyhow::anyhow!("Invalid point row"));
        }
        if parts.len() != 3 {
            return Err(anyhow::anyhow!("Invalid point row, expect 3 columns"));
        }
        let point_name = parts[0].to_string();
        let super_table = parts[2].to_string();
        Ok(PointRow {
            point_name,
            super_table,
        })
    }
}

/// 代表多列模型配置文件元素列表部分的一行
#[derive(Debug)]
pub struct ElementRow {
    pub element_name: String,
    pub super_table: String,
    pub element_id: String,
    pub path: Option<String>,
}

// #[cfg(test)]
// impl ElementRow {
//     fn from_csv(csv: String) -> anyhow::Result<Self> {
//         let parts = csv.split(',').collect::<Vec<&str>>();
//         let obj_type = parts[1].to_lowercase();
//         let obj_type = obj_type.as_str();
//         if obj_type != "element" {
//             return Err(anyhow::anyhow!("Invalid element row"));
//         }
//         if parts.len() < 4 {
//             return Err(anyhow::anyhow!("Invalid element row, expect 4 columns"));
//         }
//         let element_name = parts[0].to_string();
//         let super_table = parts[2].to_string();
//         let element_id = parts[3].to_string();
//         let path = if parts.len() < 5 || parts[4].is_empty() {
//             None
//         } else {
//             Some(parts[4].to_string())
//         };
//         Ok(ElementRow {
//             element_name,
//             super_table,
//             element_id,
//             path,
//         })
//     }
// }

pub enum PiModelType {
    SingleColumn,
    MultiColumn,
}

impl TryFrom<&str> for PiModelType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "single-column" => Ok(PiModelType::SingleColumn),
            "multi-column" => Ok(PiModelType::MultiColumn),
            _ => Err(anyhow!(
                "Invalid PI model type,  only single-column and multi-column are supported"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sink::lush::LushModelConfig;

    use super::*;

    #[test]
    fn test_csv_line_split() {
        let line = r#"element_paths,TAG,NCHAR(512),`$element_paths.replace("\", ".")`"#;
        let parts = split_csv_line(line);
        for part in parts {
            println!("{}", part);
        }
    }
    fn test_parse_point_data() {
        let config = PIPointModelConfig::from_json(POINT_DATA, true).unwrap();
        std::fs::write("point_model.csv", config.to_string()).unwrap();
        let config = PIPointModelConfig::from_csv("point_model.csv").unwrap();
        println!("{}", config.to_csv());
    }

    fn test_parse_element_data() {
        let config = PIElementModelConfig::from_json(ELEMENT_DATA);
        std::fs::write("element_model.csv", config.unwrap().to_string()).unwrap();
        let config = PIElementModelConfig::from_csv("element_model.csv").unwrap();
        println!("{}", config.to_csv());
    }

    #[test]
    fn test_from_csv() {
        test_parse_point_data();
        test_parse_element_data();

        let config = PIPointModelConfig::from_csv("point_model.csv").unwrap();
        let config: LushModelConfig = config.into();
        println!("{:?}", config);

        let config = PIElementModelConfig::from_csv("element_model.csv").unwrap();
        let config: LushModelConfig = config.into();
        println!("{:?}", config);
    }

    const POINT_DATA: &str = r#"
    {
        "Templates": [
            {
                "TemplateName": "TS_OSIsoft.AF.Asset.AFEnumerationValue",
                "TDType": "NCHAR(100)",
                "Type": "AFEnumerationValue",
                "UOMABB": null,
                "UOM": null,
                "Tags": {
                    "tag": "string",
                    "descriptor": "string",
                    "exdesc": "string",
                    "engunits": "string",
                    "pointsource": "string",
                    "step": "string",
                    "future": "string"
                }
            },
            {
                "TemplateName": "TS_System.Single_$/kWh",
                "TDType": "FLOAT",
                "Type": "Single",
                "UOMABB": "$/kWh",
                "UOM": "dollars per kilowatt hour",
                "Tags": {
                    "tag": "string",
                    "descriptor": "string",
                    "exdesc": "string",
                    "engunits": "string",
                    "pointsource": "string",
                    "step": "string",
                    "future": "string"
                }
            },
            {
                "TemplateName": "TS_System.Single_kW",
                "TDType": "FLOAT",
                "Type": "Single",
                "UOMABB": "kW",
                "UOM": "kilowatt",
                "Tags": {
                    "tag": "string",
                    "descriptor": "string",
                    "exdesc": "string",
                    "engunits": "string",
                    "pointsource": "string",
                    "step": "string",
                    "future": "string"
                }
            },
            {
                "TemplateName": "TS_System.Single_kWh",
                "TDType": "FLOAT",
                "Type": "Single",
                "UOMABB": "kWh",
                "UOM": "kilowatt hour",
                "Tags": {
                    "tag": "string",
                    "descriptor": "string",
                    "exdesc": "string",
                    "engunits": "string",
                    "pointsource": "string",
                    "step": "string",
                    "future": "string"
                }
            },
            {
                "TemplateName": "TS_System.Single_m/s",
                "TDType": "FLOAT",
                "Type": "Single",
                "UOMABB": "m/s",
                "UOM": "meter per second",
                "Tags": {
                    "tag": "string",
                    "descriptor": "string",
                    "exdesc": "string",
                    "engunits": "string",
                    "pointsource": "string",
                    "step": "string",
                    "future": "string"
                }
            }
        ],
        "Points": [
            {
                "ID": 64685,
                "Name": "OSIDemo_GE001.Lost Revenue Rate",
                "Path": "\\\\WIN-2OA23UM12TN\\OSIDemo_GE001.Lost Revenue Rate",
                "Type": "Single",
                "TDType": "FLOAT",
                "UOMABB": "$/kWh",
                "UOM": "dollars per kilowatt hour",
                "Template": "TS_System.Single_$/kWh",
                "Tags": {
                    "tag": "OSIDemo_GE001.Lost Revenue Rate",
                    "descriptor": "",
                    "exdesc": "",
                    "engunits": "",
                    "pointsource": "OSIDemo_AFAnalysis",
                    "step": "0",
                    "future": "0"
                },
                "Elements": [
                    {
                        "ID": "1ab37258-d57e-11ee-bf13-00505695feda",
                        "Name": "GE001",
                        "TemplateName": "Turbine",
                        "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Scirocco\\Santaella\\GE001"
                    }
                ]
            },
            {
                "ID": 64682,
                "Name": "OSIDemo_GE001.Status Cause",
                "Path": "\\\\WIN-2OA23UM12TN\\OSIDemo_GE001.Status Cause",
                "Type": "AFEnumerationValue",
                "TDType": "NCHAR(100)",
                "UOMABB": null,
                "UOM": null,
                "Template": "TS_OSIsoft.AF.Asset.AFEnumerationValue",
                "Tags": {
                    "tag": "OSIDemo_GE001.Status Cause",
                    "descriptor": "",
                    "exdesc": "",
                    "engunits": "",
                    "pointsource": "OSIDemo_AFAnalysis",
                    "step": "1",
                    "future": "0"
                },
                "Elements": [
                    {
                        "ID": "1ab37258-d57e-11ee-bf13-00505695feda",
                        "Name": "GE001",
                        "TemplateName": "Turbine",
                        "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Scirocco\\Santaella\\GE001"
                    }
                ]
            }
        ]
    }
    "#;

    const ELEMENT_DATA: &str = r#"
    {
        "Templates": [
          {
            "TemplateName": "Template_Beijing",
            "Attributes": [
              {
                "Name": "Current",
                "Type": "DOUBLE",
                "UOMABB": "mA",
                "UOM": "milliampere"
              },
              {
                "Name": "Voltage",
                "Type": "DOUBLE",
                "UOMABB": "V",
                "UOM": "volt"
              }
            ],
            "StaticAttributes": []
          }
        ],
        "SingleElements": [],
        "Elements": [
          {
            "ID": "d552ba74-cf9a-11ee-bf12-00505695feda",
            "Name": "Element_Beijing1",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Haidian\\Element_Beijing1"
          },
          {
            "ID": "87fb3759-cf9b-11ee-bf12-00505695feda",
            "Name": "Element_Beijing2",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Haidian\\Element_Beijing2"
          },
          {
            "ID": "9428635f-cf9b-11ee-bf12-00505695feda",
            "Name": "Element_Beijing3",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Haidian\\Element_Beijing3"
          },
          {
            "ID": "c2b8a281-cf9b-11ee-bf12-00505695feda",
            "Name": "Element_Beijing4",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Chaoyang\\Element_Beijing4"
          },
          {
            "ID": "cf7435e7-cf9b-11ee-bf12-00505695feda",
            "Name": "Element_Beijing5",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Chaoyang\\Element_Beijing5"
          },
          {
            "ID": "c959b906-cf9b-11ee-bf12-00505695feda",
            "Name": "Element_Beijing6",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Beijing\\Chaoyang\\Element_Beijing6"
          },
          {
            "ID": "cbc23626-f716-11ee-bf14-00505695feda",
            "Name": "Element_Beijing6",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Element_Beijing6"
          },
          {
            "ID": "cbc23629-f716-11ee-bf14-00505695feda",
            "Name": "Element_Beijing7",
            "TemplateName": "Template_Beijing",
            "Path": "\\\\WIN-2OA23UM12TN\\Meters\\Element_Beijing7"
          }
        ]
      }
        "#;
}
