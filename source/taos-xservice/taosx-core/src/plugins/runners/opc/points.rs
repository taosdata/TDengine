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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpcNode {
    pub id: String,
    pub is_static: Option<bool>,
    pub name: Option<String>,         // BrowseName
    pub description: Option<String>,  // Description
    pub display_name: Option<String>, // DisplayName
    pub node_type: Option<String>,    // NodeClass: Object | Variable
    pub parent_id: Option<String>,    // Parent Node ID
    pub path: Option<String>,         // Full path
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

        if let Some(options) = dataset.options {
            for option in options {
                match option.name.as_str() {
                    OPC_BROWSE_NAME => name = Some(option.display),
                    OPC_DESCRIPTION => description = Some(option.display),
                    OPC_DISPLAY_NAME => display_name = Some(option.display),
                    OPC_PARENT_ID => parent_id = Some(option.display),
                    OPC_PATH => path = Some(option.display),
                    _ => {}
                }
            }
        }

        Ok(OpcNode {
            id: dataset.id,
            is_static: None,
            name,
            description,
            display_name,
            node_type: dataset.r#type,
            parent_id,
            path,
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
