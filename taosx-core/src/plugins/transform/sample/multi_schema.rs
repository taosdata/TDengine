use std::sync::Arc;

use anyhow::Context;
use arrow_schema::{Field, Schema};
use itertools::Itertools;
use serde_arrow::schema::SerdeArrowSchema;

use crate::plugins::transform::{
    modeler::{stable::STableModel, ModeledJsonOutput},
    to_json_valid_batches,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MultiSchemaSamples {
    parser: crate::Pipeline,
    samples: Vec<SampleWithSchema>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SampleWithSchema {
    schema: SerdeArrowSchema,
    input: Vec<serde_json::Value>,
}

impl SampleWithSchema {
    pub fn new<T>(schema: &Schema, input: Vec<T>) -> anyhow::Result<Self>
    where
        T: serde::ser::Serialize,
    {
        let input = input
            .iter()
            .map(|v| serde_json::to_value(v).context("sample input serialize to json value error"))
            .collect::<anyhow::Result<_>>()?;
        let schema = SerdeArrowSchema::try_from(schema.fields().as_ref())
            .context("build schema from sample input fields error")?;
        Ok(Self { schema, input })
    }
}

impl MultiSchemaSamples {
    pub fn new(parser: crate::Pipeline, samples: Vec<SampleWithSchema>) -> Self {
        Self { parser, samples }
    }
}

impl MultiSchemaSamples {
    pub fn transform(self, tz: Option<&str>) -> anyhow::Result<Vec<ModeledJsonOutput>> {
        if self.samples.is_empty() {
            anyhow::bail!("Input should not be empty");
        }

        let mut ret = Vec::new();
        for sample in self.samples {
            let fields: Vec<Field> = sample.schema.try_into().context("get schema field error")?;
            let schema = Schema::new(fields);
            let json = sample
                .input
                .iter()
                .filter_map(|v| serde_json::to_vec(v).ok())
                .flatten()
                .collect::<Vec<_>>();
            let mut reader = arrow::json::reader::ReaderBuilder::new(Arc::new(schema))
                .build(json.as_slice())
                .context("Could not build record reader from json stream")?;
            let batch = reader
                .next()
                .context("record batch not found from json stream")?
                .context("read record batch error")?;
            let output = self.parser.transform(&batch)?;
            let modeled = output
                .iter()
                .map(|batch| match tz {
                    Some(tz) => batch.to_modeled_json_with_tz(tz),
                    None => batch.to_modeled_json(),
                })
                .collect::<Vec<_>>();
            ret.extend(modeled);
        }
        Ok(ret)
    }

    pub fn stable_preview(&self) -> anyhow::Result<Vec<STableModel>> {
        if self.samples.is_empty() {
            anyhow::bail!("Samples should not be empty");
        }

        let mut ret = Vec::with_capacity(self.samples.len());
        for sample in &self.samples {
            let json = sample
                .input
                .iter()
                .flat_map(|value| serde_json::to_vec(value).unwrap())
                .collect_vec();

            let fields: Vec<Field> = sample
                .schema
                .clone()
                .try_into()
                .context("get schema field error")?;
            let schema = Schema::new(fields);

            let mut reader = arrow::json::reader::ReaderBuilder::new(Arc::new(schema))
                .build(json.as_slice())
                .context("Could not build record reader from json stream")?;
            let batch = reader.next().unwrap()?;

            let batch = self.parser.transform_records(&batch)?;

            let json_batches = to_json_valid_batches(&[batch]);

            let Some(records) = json_batches.first() else {
                return Ok(vec![]);
            };

            let stables = self
                .parser
                .s_model
                .as_ref()
                .map(|s| s.apply(records, &self.parser.global))
                .transpose()?
                .context("stable model not found")?;

            ret.extend(stables.values().cloned());
        }

        Ok(ret)
    }
}
