//! 本模块提供一个执行指定命令启动连接器，并返回连接器输出的内容的通用函数：query_datasource。
//!
//! 因为数据源的多样性和复杂性，在任务开始之前，可能需要多次与连接器交互，获取示例数据和获取数据集都是其中的特例（见 point_loader 模块）。
//!
//! 本模块的 query_datasource 函数与 point_loader 模块使用 list_datasets 函数的不同主要体现在以下两点：
//! 1. 启动命令的具体选项和选项的值由 DatasourceRequest 中的参数确定，而不是固定的。
//! 2. query_datasource 函数的返回值是连接器原始输出内容，对输出内容的解析由调用者自行完成。
//!
//! 这个模块的函数仅适用于“外置”数据源（即有需要启动独立的连接器进程的数据源），且仅适用于能快速返回结果的启动命令。
//!
//! 注意本模块只是提供一个统一的入口，具体的连接器启动逻辑应该在各自的连接器插件中实现（即：taosx_core::plugins::runners 模块）。
//!

use taosx_core::QueryDataSourceReq;
use tracing::instrument;

use crate::serve::TaskController;

#[instrument(skip_all)]
pub async fn query_data_source(
    request: QueryDataSourceReq,
    via: Option<i64>,
    controller: Option<&TaskController>,
) -> anyhow::Result<String> {
    tracing::info!(?request, ?via);
    if let Some(agent_id) = via {
        // 通过 Agent 启动连接器
        if controller.is_none() {
            anyhow::bail!("controller is required when query datasource via agent");
        }
        let controller = controller.unwrap();
        controller
            .query_data_source_via_agent(request, agent_id)
            .await
    } else {
        // 直接启动连接器
        taosx_core::plugins::query_data_source(request).await
    }
}
