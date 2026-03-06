use anyhow::Context;
use serde::{Deserialize, Serialize};
use taosx_core::{DataSet, OptionSet};

pub const PSPACE_NODE: &str = "PSPACE_NODE";

/// 对应 pSpce 的 Node，即 type = PS_NODE 的 Tag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PspaceNode {
    pub id: u64,
    pub name: String,
    pub long_name: String,
    pub is_leaf: bool,
}

impl From<PspaceNode> for DataSet {
    fn from(node: PspaceNode) -> Self {
        let mut ds = DataSet {
            id: node.id.to_string(),
            name: Some(node.name),
            category: Some(PSPACE_NODE.to_string()),
            r#type: None,
            options: Some(vec![]),
            format: None,
        };
        // map is_leaf to dataset option
        if let Some(options) = ds.options.as_mut() {
            options.push(OptionSet {
                name: "is_leaf".to_string(),
                display: node.is_leaf.to_string(),
                description: None,
                required: false,
            });
        }
        // map long_name to dataset option
        if let Some(options) = ds.options.as_mut() {
            options.push(OptionSet {
                name: "long_name".to_string(),
                display: node.long_name,
                description: None,
                required: false,
            });
        }
        ds
    }
}

impl TryFrom<DataSet> for PspaceNode {
    type Error = anyhow::Error;

    fn try_from(value: DataSet) -> Result<Self, Self::Error> {
        let id = value.id.parse::<u64>().context("invalid pSpace node id")?;
        let name = value
            .name
            .ok_or(anyhow::anyhow!("missing pSpace node name"))?;
        let options = value
            .options
            .ok_or(anyhow::anyhow!("missing pSpace node options"))?;
        let long_name = options
            .iter()
            .find(|o| o.name == "long_name")
            .ok_or(anyhow::anyhow!("missing pSpace node long_name option"))?
            .display
            .clone();
        let is_leaf: bool = options
            .iter()
            .find(|o| o.name == "is_leaf")
            .ok_or(anyhow::anyhow!("missing pSpace node is_leaf option"))?
            .display
            .clone()
            .parse()?;

        Ok(Self {
            id,
            name,
            long_name,
            is_leaf,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pspace_node_conversion() {
        // Deserialize from pSpace plugin JSON, then convert to DataSet
        let json = r#"{"id":150016,"name":"北京","long_name":"\\北京","is_leaf":false}"#;
        let node: PspaceNode =
            serde_json::from_str(json).expect("should deserialize pSpace node JSON");
        assert_eq!(node.id, 150016);
        assert_eq!(node.name, "北京");
        assert_eq!(node.long_name, r"\北京");
        assert!(!node.is_leaf);

        let ds: DataSet = node.clone().into();
        assert_eq!(ds.id, "150016");
        assert_eq!(ds.name.as_deref(), Some("北京"));
        assert_eq!(ds.category.as_deref(), Some(PSPACE_NODE));
        assert!(ds.r#type.is_none());

        let options = ds.options.as_ref().unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].name, "is_leaf");
        assert_eq!(options[0].display, "false");
        assert_eq!(options[1].name, "long_name");
        assert_eq!(options[1].display, r"\北京");

        // DataSet -> PspaceNode round-trip
        let restored = PspaceNode::try_from(ds).expect("should convert back to PspaceNode");
        assert_eq!(restored.id, node.id);
        assert_eq!(restored.name, node.name);
        assert_eq!(restored.long_name, node.long_name);
        assert_eq!(restored.is_leaf, node.is_leaf);

        // Error cases: missing name
        let bad_ds = DataSet {
            id: "1".to_string(),
            name: None,
            category: None,
            r#type: None,
            options: Some(vec![]),
            format: None,
        };
        assert!(PspaceNode::try_from(bad_ds).is_err());

        // Error cases: missing options
        let bad_ds2 = DataSet {
            id: "1".to_string(),
            name: Some("n".to_string()),
            category: None,
            r#type: None,
            options: None,
            format: None,
        };
        assert!(PspaceNode::try_from(bad_ds2).is_err());

        // Error cases: invalid id
        let bad_ds3 = DataSet {
            id: "not_a_number".to_string(),
            name: Some("n".to_string()),
            category: None,
            r#type: None,
            options: Some(vec![]),
            format: None,
        };
        assert!(PspaceNode::try_from(bad_ds3).is_err());
    }
}
