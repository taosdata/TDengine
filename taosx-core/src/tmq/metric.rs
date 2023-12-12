use crate::core_metrics::{CoreMetrics, TaosXMetrics};
pub struct TMQMetrics {}

impl TaosXMetrics for TMQMetrics {
    fn to_json(&self) -> String {
        String::from("TMQMetrics")
    }

    fn from_json(_json: &str) -> Self {
        TMQMetrics {}
    }

    fn reset(&self) {}

    fn update_total_execute_time(&self) {
        todo!()
    }
}

impl Into<CoreMetrics> for TMQMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::TMQ(self)
    }
}
