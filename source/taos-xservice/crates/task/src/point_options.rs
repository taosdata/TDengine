use anyhow::{Context, bail};
use taosx_core::{DataSetsReq, plugins::list_datasets_from};

pub async fn get_point_options(req: &DataSetsReq) -> anyhow::Result<serde_json::Value> {
    tracing::info!("try to get kinghistorian point options, req: {:?}", req);
    // 获取点位列表
    let result = if let Some(agent) = req.via {
        bail!(
            "via agent {} is not supported for getting point options",
            agent
        );
    } else {
        list_datasets_from(req).await
    };
    let datasets = result.context("Failed to list datasets when getting point options")?;

    // 将点位列表转换为 serde_json::Value
    let options = source_kinghistorian::to_point_options(datasets)?;

    Ok(options)
}
