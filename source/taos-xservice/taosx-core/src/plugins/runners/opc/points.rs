use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    DataSet, OptionSet,
    runners::opc::{OpcType, config::OPCConfig},
};

pub const OPC_NODE_CLASS: &str = "NodeClass";
pub const OPC_BROWSE_NAME: &str = "BrowseName";
pub const OPC_DISPLAY_NAME: &str = "DisplayName";
pub const OPC_DESCRIPTION: &str = "Description";
pub const OPC_PARENT_ID: &str = "ParentId";
pub const OPC_PATH: &str = "Path";
/// IsStatic：true 表示该节点不下发到 collect 订阅。
/// - Object：恒为 true（仅做拓扑容器）
/// - Property Variable（IsProperty=true）：恒为 true（值已 Read 一次塞父）
/// - 普通 Variable：false（动态订阅）
///   编码到 OptionSet.display 为 "true"/"false"。
pub const OPC_IS_STATIC: &str = "IsStatic";
/// IsProperty：true 表示该 Variable 是父 Variable 的元数据 Property（如 EURange、EngineeringUnits）。
/// 在 sink 阶段会被用于："不要把它建成独立子表，把它合并为父子表的 Tag"。
/// 编码到 OptionSet.display 为 "true"/"false"。
pub const OPC_IS_PROPERTY: &str = "IsProperty";
/// Properties：动态 Variable 收集到的、归属自身的 Property 名→已序列化字符串值。
/// 用于在 sink 阶段写入子表 Tag。
/// 编码到 OptionSet.display 为整个 map 的 JSON 字符串（"{}" 表示空）。
pub const OPC_PROPERTIES: &str = "Properties";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpcNode {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_static: Option<bool>,
    /// 是否为父 Variable 的元数据 Property（合并为 Tag，不建独立子表）。
    /// 仅对 NodeClass=Variable 有意义；其他 NodeClass 应保持 None / false。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_property: Option<bool>,
    pub name: Option<String>,         // BrowseName
    pub description: Option<String>,  // Description
    pub display_name: Option<String>, // DisplayName
    pub node_type: Option<String>,    // NodeClass: Object | Variable
    pub parent_id: Option<String>,    // Parent Node ID
    pub path: Option<String>,         // Full path
    /// 父 Variable 的 Property 名→已序列化字符串值（复杂值为 JSON 字符串）。
    /// 仅对动态 Variable（is_property=false / None）填充；
    /// Property 节点本身不携带（其值已塞进父）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

/// Convert OpcNode to DataSet
/// id: use OpcNode.id
/// name: use OpcNode.name = BrowseName
/// category: "OPC_NODE"
/// type: use OpcNode.node_type
/// options:
///   - BrowseName: use OpcNode.name
///   - Description: use OpcNode.description
///   - DisplayName: use OpcNode.display_name
///   - ParentId: use OpcNode.parent_id
///   - Path: use OpcNode.path
///   - IsStatic: "true"/"false"（仅当 Some 时写入）
///   - IsProperty: "true"/"false"（仅当 Some 时写入）
///   - Properties: JSON 字符串（仅当非空 map 时写入）
impl TryInto<DataSet> for OpcNode {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<DataSet, Self::Error> {
        let mut options = vec![];

        // add browse_name option
        if let Some(name) = &self.name {
            options.push(OptionSet {
                name: OPC_BROWSE_NAME.to_string(),
                display: name.clone(),
                description: None,
                required: false,
            });
        }
        // add description option
        if let Some(description) = &self.description {
            options.push(OptionSet {
                name: OPC_DESCRIPTION.to_string(),
                display: description.clone(),
                description: None,
                required: false,
            });
        }
        // add display_name option
        if let Some(display_name) = &self.display_name {
            options.push(OptionSet {
                name: OPC_DISPLAY_NAME.to_string(),
                display: display_name.clone(),
                description: None,
                required: false,
            });
        }
        // add parent_id option
        if let Some(parent_id) = &self.parent_id {
            options.push(OptionSet {
                name: OPC_PARENT_ID.to_string(),
                display: parent_id.clone(),
                description: None,
                required: false,
            });
        }
        // add path option
        if let Some(path) = &self.path {
            options.push(OptionSet {
                name: OPC_PATH.to_string(),
                display: path.clone(),
                description: None,
                required: false,
            });
        }
        // is_static / is_property / properties — Sink 侧 generate 阶段使用
        if let Some(is_static) = self.is_static {
            options.push(OptionSet {
                name: OPC_IS_STATIC.to_string(),
                display: if is_static { "true" } else { "false" }.to_string(),
                description: None,
                required: false,
            });
        }
        if let Some(is_property) = self.is_property {
            options.push(OptionSet {
                name: OPC_IS_PROPERTY.to_string(),
                display: if is_property { "true" } else { "false" }.to_string(),
                description: None,
                required: false,
            });
        }
        if let Some(properties) = &self.properties
            && !properties.is_empty()
        {
            let json = serde_json::to_string(properties)
                .map_err(|e| anyhow::anyhow!("serialize OpcNode properties: {}", e))?;
            options.push(OptionSet {
                name: OPC_PROPERTIES.to_string(),
                display: json,
                description: None,
                required: false,
            });
        }

        Ok(DataSet {
            id: self.id,
            name: self.name,
            category: Some("OPC_NODE".to_string()),
            r#type: self.node_type,
            options: if options.is_empty() {
                None
            } else {
                Some(options)
            },
            format: None,
        })
    }
}

impl TryFrom<DataSet> for OpcNode {
    type Error = anyhow::Error;

    fn try_from(dataset: DataSet) -> Result<OpcNode, Self::Error> {
        let mut name = None;
        let mut description = None;
        let mut display_name = None;
        let mut parent_id = None;
        let mut path = None;
        let mut is_static = None;
        let mut is_property = None;
        let mut properties = None;

        if let Some(options) = dataset.options {
            for option in options {
                match option.name.as_str() {
                    OPC_BROWSE_NAME => name = Some(option.display),
                    OPC_DESCRIPTION => description = Some(option.display),
                    OPC_DISPLAY_NAME => display_name = Some(option.display),
                    OPC_PARENT_ID => parent_id = Some(option.display),
                    OPC_PATH => path = Some(option.display),
                    OPC_IS_STATIC => is_static = Some(option.display == "true"),
                    OPC_IS_PROPERTY => is_property = Some(option.display == "true"),
                    OPC_PROPERTIES => {
                        // 容错：解析失败保持 None 不让上游崩溃
                        properties =
                            serde_json::from_str::<HashMap<String, String>>(&option.display).ok();
                    }
                    _ => {}
                }
            }
        }

        Ok(OpcNode {
            id: dataset.id,
            is_static,
            is_property,
            name,
            description,
            display_name,
            node_type: dataset.r#type,
            parent_id,
            path,
            properties,
        })
    }
}

impl OpcNode {
    // 过滤出 dynamic nodes 的函数
    pub fn variable_node_filter() -> fn(&OpcNode) -> bool {
        |node: &OpcNode| node.node_type.as_deref() == Some("Variable")
    }

    // 过滤出 static nodes 的函数
    pub fn object_node_filter() -> fn(&OpcNode) -> bool {
        |node: &OpcNode| node.node_type.as_deref() == Some("Object")
    }
}

/// OPC 点位缓存键
/// 命中条件：`connection`、`opc_type`、`ua_root`、`ua_namespaces` 一致。
/// 为保证稳定命中，`ua_namespaces` 在构造时会执行排序与去重。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OpcCacheKey {
    pub opc_type: OpcType,                  // OPC 类型
    pub connection: String,                 // OPC 连接配置（建议使用规范化后的字符串）
    pub ua_root: Option<String>,            // UA 根节点（可选）
    pub ua_namespaces: Option<Vec<String>>, // UA 命名空间列表（排序 + 去重 后参与哈希）
}

impl From<&OPCConfig> for OpcCacheKey {
    fn from(value: &OPCConfig) -> Self {
        // 构造连接标识：
        // - OPC UA 使用规范化的 endpoint
        // - OPC DA 使用 server + 排序后的节点列表 作为规范化字符串
        let connection = if let Some(ua) = value.connect.ua.as_ref() {
            ua.endpoint.clone()
        } else if let Some(da) = value.connect.da.as_ref() {
            let mut nodes = da.nodes.clone();
            nodes.sort();
            format!("opcda://{}|{}", da.server, nodes.join(","))
        } else {
            String::new()
        };

        // 提取 UA 的 root 与 namespaces，并进行排序/去重以保证稳定命中
        let (ua_root, ua_namespaces) = value
            .points
            .as_ref()
            .and_then(|p| p.ua.as_ref())
            .map(|ua| {
                let root = ua.root.as_ref().map(|s| s.to_string());
                let namespaces = ua.namespaces.as_ref().map(|ns| {
                    let mut v = ns.iter().map(|n| n.to_string()).collect::<Vec<String>>();
                    v.sort();
                    v.dedup();
                    v
                });
                (root, namespaces)
            })
            .unwrap_or((None, None));

        OpcCacheKey {
            opc_type: value.opc_type,
            connection,
            ua_root,
            ua_namespaces,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_dynamic_node() -> OpcNode {
        let mut props = HashMap::new();
        props.insert("EngineeringUnits".to_string(), "°C".to_string());
        props.insert("EURange".to_string(), r#"{"Low":0,"High":100}"#.to_string());
        OpcNode {
            id: "ns=2;s=G3.a96cd_1a".to_string(),
            is_static: Some(false),
            is_property: Some(false),
            name: Some("a96cd_1a".to_string()),
            description: Some("desc".to_string()),
            display_name: Some("a96cd_1a".to_string()),
            node_type: Some("Variable".to_string()),
            parent_id: Some("ns=2;s=G3".to_string()),
            path: Some("Objects/G3/a96cd_1a".to_string()),
            properties: Some(props),
        }
    }

    #[test]
    fn opcnode_dataset_roundtrip_dynamic() {
        let original = sample_dynamic_node();
        let ds: DataSet = original.clone().try_into().unwrap();
        let back = OpcNode::try_from(ds).unwrap();
        assert_eq!(back.id, original.id);
        assert_eq!(back.is_static, original.is_static);
        assert_eq!(back.is_property, original.is_property);
        assert_eq!(back.name, original.name);
        assert_eq!(back.description, original.description);
        assert_eq!(back.display_name, original.display_name);
        assert_eq!(back.node_type, original.node_type);
        assert_eq!(back.parent_id, original.parent_id);
        assert_eq!(back.path, original.path);
        assert_eq!(back.properties, original.properties);
    }

    #[test]
    fn opcnode_dataset_roundtrip_property_node() {
        // Property 节点：is_property=true，properties 为空
        let original = OpcNode {
            id: "ns=2;s=G3.a96cd_1a.EURange".to_string(),
            is_static: Some(true),
            is_property: Some(true),
            name: Some("EURange".to_string()),
            description: None,
            display_name: Some("EURange".to_string()),
            node_type: Some("Variable".to_string()),
            parent_id: Some("ns=2;s=G3.a96cd_1a".to_string()),
            path: Some("Objects/G3/a96cd_1a/EURange".to_string()),
            properties: None,
        };
        let ds: DataSet = original.clone().try_into().unwrap();
        let back = OpcNode::try_from(ds).unwrap();
        assert_eq!(back.is_static, Some(true));
        assert_eq!(back.is_property, Some(true));
        assert!(back.properties.is_none());
    }

    #[test]
    fn opcnode_dataset_roundtrip_object() {
        // Object 节点：is_static=Some(true)（恒静态），无 is_property / properties
        let original = OpcNode {
            id: "ns=2;s=G3".to_string(),
            is_static: Some(true),
            is_property: None,
            name: Some("G3".to_string()),
            description: None,
            display_name: Some("G3".to_string()),
            node_type: Some("Object".to_string()),
            parent_id: Some("ns=0;i=85".to_string()),
            path: Some("Objects/G3".to_string()),
            properties: None,
        };
        let ds: DataSet = original.clone().try_into().unwrap();
        let back = OpcNode::try_from(ds).unwrap();
        assert_eq!(back.node_type.as_deref(), Some("Object"));
        assert_eq!(back.is_static, Some(true));
        assert!(back.is_property.is_none());
    }

    #[test]
    fn opcnode_dataset_roundtrip_legacy_no_new_fields() {
        // 兼容老 Go 版本：完全没有 is_static/is_property/properties 字段
        let ds = DataSet {
            id: "ns=2;s=Foo".to_string(),
            name: Some("Foo".to_string()),
            category: Some("OPC_NODE".to_string()),
            r#type: Some("Variable".to_string()),
            options: Some(vec![OptionSet {
                name: OPC_BROWSE_NAME.to_string(),
                display: "Foo".to_string(),
                description: None,
                required: false,
            }]),
            format: None,
        };
        let back = OpcNode::try_from(ds).unwrap();
        assert!(back.is_static.is_none());
        assert!(back.is_property.is_none());
        assert!(back.properties.is_none());
    }

    #[test]
    fn opcnode_dataset_skip_empty_properties_map() {
        // properties 是空 map → 不应写入 OptionSet（避免 "{}" 噪音）
        let node = OpcNode {
            id: "x".to_string(),
            is_static: Some(false),
            is_property: Some(false),
            name: None,
            description: None,
            display_name: None,
            node_type: Some("Variable".to_string()),
            parent_id: None,
            path: None,
            properties: Some(HashMap::new()),
        };
        let ds: DataSet = node.try_into().unwrap();
        let opts = ds.options.unwrap();
        assert!(opts.iter().all(|o| o.name != OPC_PROPERTIES));
    }

    #[test]
    fn opcnode_dataset_roundtrip_corrupt_properties_json_is_none() {
        // OPC_PROPERTIES OptionSet display 不是合法 JSON → 反序列化后保持 None
        let ds = DataSet {
            id: "x".to_string(),
            name: None,
            category: Some("OPC_NODE".to_string()),
            r#type: Some("Variable".to_string()),
            options: Some(vec![OptionSet {
                name: OPC_PROPERTIES.to_string(),
                display: "not json".to_string(),
                description: None,
                required: false,
            }]),
            format: None,
        };
        let back = OpcNode::try_from(ds).unwrap();
        assert!(back.properties.is_none());
    }

    #[test]
    fn opcnode_deserialize_from_go_json() {
        // 模拟 Go side `taosx-opc points` stdout JSON
        let json = r#"[
            {
                "id": "ns=2;s=G3.a96cd_1a",
                "is_static": false,
                "name": "a96cd_1a",
                "description": "",
                "display_name": "a96cd_1a",
                "node_type": "Variable",
                "path": "Objects/G3/a96cd_1a",
                "properties": {
                    "EngineeringUnits": "\u00b0C",
                    "EURange": "{\"Low\":0,\"High\":100}"
                }
            },
            {
                "id": "ns=2;s=G3.a96cd_1a.EURange",
                "is_static": true,
                "is_property": true,
                "name": "EURange",
                "node_type": "Variable",
                "path": "Objects/G3/a96cd_1a/EURange"
            },
            {
                "id": "ns=2;s=G3",
                "is_static": true,
                "name": "G3",
                "node_type": "Object",
                "path": "Objects/G3"
            }
        ]"#;
        let nodes: Vec<OpcNode> = serde_json::from_str(json).expect("Go JSON should deserialize");
        assert_eq!(nodes.len(), 3);
        // dynamic Variable: is_property 缺省 -> None；properties 解析成 map
        assert_eq!(nodes[0].is_static, Some(false));
        assert_eq!(nodes[0].is_property, None);
        let props = nodes[0].properties.as_ref().unwrap();
        assert_eq!(
            props.get("EngineeringUnits").map(String::as_str),
            Some("°C")
        );
        assert_eq!(
            props.get("EURange").map(String::as_str),
            Some(r#"{"Low":0,"High":100}"#)
        );
        // Property Variable
        assert_eq!(nodes[1].is_property, Some(true));
        assert_eq!(nodes[1].is_static, Some(true));
        assert!(nodes[1].properties.is_none());
        // Object
        assert_eq!(nodes[2].node_type.as_deref(), Some("Object"));
        assert!(nodes[2].is_property.is_none());
        assert!(nodes[2].properties.is_none());
    }
}
