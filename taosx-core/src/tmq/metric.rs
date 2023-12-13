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

    fn total_execute_time(&self) -> u64 {
        todo!()
    }

    fn total_written_rows(&self) -> u64 {
        todo!()
    }

    fn written_rows(&self) -> u64 {
        todo!()
    }

    fn start_time(&self) -> i64 {
        todo!()
    }
}

impl Into<CoreMetrics> for TMQMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::TMQ(self)
    }
}
