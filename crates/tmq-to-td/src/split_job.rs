use anyhow::Context;
use ha_core::types::{SplitJobResult, SplitJobTask};
use taosx_core::tmq::check_tmq_dsn;
use taosx_utils::dsn::dsn_to_json;
use tracing::instrument;

#[instrument(skip_all)]
pub async fn split_job(task: SplitJobTask) -> anyhow::Result<SplitJobResult> {
    let from = task.from;
    let (mut from, _, topics, with_meta_delete, with_meta_drop) = check_tmq_dsn(from).await?;

    if with_meta_delete {
        from.params.insert("with.meta.delete".into(), "true".into());
    }
    if with_meta_drop {
        from.params.insert("with.meta.drop".into(), "true".into());
    }

    let topic_value = serde_json::to_value(&topics).context("serialize tmq topics")?;

    let mut from_json = dsn_to_json(&from);
    if let Some(from) = from_json.as_object_mut() {
        from.insert("topics".into(), topic_value);
    }

    Ok(SplitJobResult {
        from: from_json,
        to: task.to.to_string(),
        parser: task.parser,
    })
}
