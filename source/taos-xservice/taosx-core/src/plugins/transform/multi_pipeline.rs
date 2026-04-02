use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use archive::ArchiveType;
use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_compute_ext::RecordBatchExt;
use chrono::{DateTime, Utc};
use flume::Sender;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;
use tracing::{error, instrument, warn};

use crate::core_metrics::CoreMetrics;
use crate::plugins::expr::ConditionExpr;

use super::handling_strategy::HandlingResult;
use super::modeler::Modeler;
use super::modeler::stable::{FastStrExpr, STableModel};
use super::mutate::Mutate;
use super::parse::{FieldParser, ParserImpl};
use super::{
    Error, Message, MessageArrowRecords, MessageTableMeta, Parser, ParserError, STable, Select,
    TableOptions, TransformExt, archive_records_blocking, generate_table_name, get_data_dir,
    get_primary_timestamp_ns, indices_to_ranges, pivot,
};

static EMPTY_MODELER: LazyLock<Modeler> = LazyLock::new(|| Modeler::new(vec![]));

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub matches: ConditionExpr,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mutate: Vec<Mutate>,
    pub model: Modeler,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiPipeline {
    #[serde(default)]
    global: Arc<TableOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parse: Option<ParserImpl>,
    pub rules: Vec<Rule>,
    #[serde(skip)]
    metrics: Option<Arc<CoreMetrics>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TransformConfig {
    Multi(MultiPipeline),
    Single(Parser),
}

impl PartialEq for MultiPipeline {
    fn eq(&self, other: &Self) -> bool {
        self.global == other.global && self.parse == other.parse && self.rules == other.rules
    }
}

impl MultiPipeline {
    pub fn global(&self) -> &TableOptions {
        &self.global
    }

    pub fn set_metrics(&mut self, metrics: Arc<CoreMetrics>) {
        self.metrics = Some(metrics);
    }

    pub fn set_maximum_timestamp(&mut self, ts: DateTime<Utc>) {
        Arc::make_mut(&mut self.global).maximum_timestamp = Some(ts);
    }

    pub fn set_minimum_timestamp(&mut self, ts: DateTime<Utc>) {
        Arc::make_mut(&mut self.global).minimum_timestamp = Some(ts);
    }

    pub fn organize_cache(&mut self, task_id: i64, job_id: i64) -> Result<(), ParserError> {
        let cache = &mut Arc::make_mut(&mut self.global).process_on_abnormal.cache;
        let data_dir = get_data_dir();
        cache
            .organize_params(task_id, job_id, data_dir, true)
            .map_err(|error| ParserError::OrganizeCacheError { error })?;
        Ok(())
    }

    pub fn organize_archive(&mut self, task_id: i64, job_id: i64) -> Result<(), ParserError> {
        let archive = &mut Arc::make_mut(&mut self.global).process_on_abnormal.archive;
        let data_dir = get_data_dir();
        archive
            .organize_params(task_id, job_id, data_dir, false)
            .map_err(|error| ParserError::OrganizeArchiveError { error })?;
        Ok(())
    }

    pub fn get_ipcdatatype_from_parser(&self, column_name: &str) -> Option<&IpcDataType> {
        get_ipcdatatype_from_parse(self.parse.as_ref(), column_name)
    }

    pub fn modeler(&self) -> &Modeler {
        self.rules
            .first()
            .map(|rule| &rule.model)
            .unwrap_or(&EMPTY_MODELER)
    }

    #[instrument(skip_all)]
    pub fn parse_message_from_records(
        &self,
        records: &RecordBatch,
        filter_ts: bool,
        archive_tx: Option<&Sender<ArchiveType>>,
    ) -> Result<Message, Error> {
        let parsed_batch = parse_batch_once(self.parse.as_ref(), records)?;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics
                .ipc()
                .add_parsed_rows(parsed_batch.num_rows() as u64);
        }

        let mut remaining_indices = (0..parsed_batch.num_rows()).collect_vec();
        let mut result_records = Vec::new();

        for (rule_index, rule) in self.rules.iter().enumerate() {
            if remaining_indices.is_empty() {
                break;
            }

            let remaining_batch = take_rows(&parsed_batch, &remaining_indices)?;
            let matched_positions = match rule.matches.eval(&remaining_batch) {
                Ok(mask) => mask
                    .into_iter()
                    .enumerate()
                    .filter_map(|(idx, matched)| matched.then_some(idx))
                    .collect_vec(),
                Err(err) => {
                    error!(
                        rule_index,
                        error = %err,
                        "failed to evaluate multi-pipeline match expression"
                    );
                    continue;
                }
            };

            if matched_positions.is_empty() {
                continue;
            }

            let matched_position_set =
                HashSet::<usize>::from_iter(matched_positions.iter().copied());
            let matched_indices = matched_positions
                .iter()
                .map(|position| remaining_indices[*position])
                .collect_vec();
            remaining_indices = remaining_indices
                .into_iter()
                .enumerate()
                .filter_map(|(position, index)| {
                    (!matched_position_set.contains(&position)).then_some(index)
                })
                .collect_vec();

            let matched_batch = take_rows(&parsed_batch, &matched_indices)?;
            let matched_rows = matched_batch.num_rows() as u64;
            let mutated_batch = match apply_mutations(&rule.mutate, matched_batch) {
                Ok(batch) => batch,
                Err(err) => {
                    error!(
                        rule_index,
                        matched_rows,
                        error = %err,
                        "failed to apply multi-pipeline rule mutations"
                    );
                    continue;
                }
            };

            if let Some(metrics) = self.metrics.as_ref() {
                let skipped_rows = matched_rows.saturating_sub(mutated_batch.num_rows() as u64);
                metrics.ipc().add_filter_skipped_rows(skipped_rows);
            }

            let message = match build_records_message(
                &self.global,
                self.metrics.as_ref(),
                &rule.model,
                None,
                &mutated_batch,
                filter_ts,
                archive_tx,
            ) {
                Ok(message) => message,
                Err(err) => {
                    error!(
                        rule_index,
                        matched_rows,
                        error = %err,
                        "failed to build message for multi-pipeline rule"
                    );
                    continue;
                }
            };
            match message {
                Message::Records(mut records) => result_records.append(&mut records),
                other => {
                    return Err(anyhow::anyhow!(
                        "multi-pipeline produced unsupported message variant: {other:?}"
                    )
                    .into());
                }
            }
        }

        if !remaining_indices.is_empty() {
            let unmatched_batch = take_rows(&parsed_batch, &remaining_indices)?;
            let sample = unmatched_row_sample(&unmatched_batch);
            warn!(
                unmatched_rows = remaining_indices.len(),
                sample = %sample,
                "multi-pipeline excluded unmatched rows"
            );
            if let Some(metrics) = self.metrics.as_ref() {
                metrics
                    .ipc()
                    .add_unmatched_rows(remaining_indices.len() as u64);
            }
        }

        Ok(Message::Records(result_records))
    }
}

impl TransformConfig {
    pub fn global(&self) -> &TableOptions {
        match self {
            Self::Single(parser) => parser.global(),
            Self::Multi(pipeline) => pipeline.global(),
        }
    }

    pub fn modeler(&self) -> &Modeler {
        match self {
            Self::Single(parser) => parser.modeler(),
            Self::Multi(pipeline) => pipeline.modeler(),
        }
    }

    pub fn set_metrics(&mut self, metrics: Arc<CoreMetrics>) {
        match self {
            Self::Single(parser) => parser.set_metrics(metrics),
            Self::Multi(pipeline) => pipeline.set_metrics(metrics),
        }
    }

    pub fn set_maximum_timestamp(&mut self, ts: DateTime<Utc>) {
        match self {
            Self::Single(parser) => parser.set_maximum_timestamp(ts),
            Self::Multi(pipeline) => pipeline.set_maximum_timestamp(ts),
        }
    }

    pub fn set_minimum_timestamp(&mut self, ts: DateTime<Utc>) {
        match self {
            Self::Single(parser) => parser.set_minimum_timestamp(ts),
            Self::Multi(pipeline) => pipeline.set_minimum_timestamp(ts),
        }
    }

    pub fn organize_cache(&mut self, task_id: i64, job_id: i64) -> Result<(), ParserError> {
        match self {
            Self::Single(parser) => parser.organize_cache(task_id, job_id),
            Self::Multi(pipeline) => pipeline.organize_cache(task_id, job_id),
        }
    }

    pub fn organize_archive(&mut self, task_id: i64, job_id: i64) -> Result<(), ParserError> {
        match self {
            Self::Single(parser) => parser.organize_archive(task_id, job_id),
            Self::Multi(pipeline) => pipeline.organize_archive(task_id, job_id),
        }
    }

    pub fn get_ipcdatatype_from_parser(&self, column_name: &str) -> Option<&IpcDataType> {
        match self {
            Self::Single(parser) => parser.get_ipcdatatype_from_parser(column_name),
            Self::Multi(pipeline) => pipeline.get_ipcdatatype_from_parser(column_name),
        }
    }

    pub fn parse_schema(
        &self,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Arc<arrow::datatypes::Schema> {
        match self {
            Self::Single(parser) => parser.parse_schema(schema),
            Self::Multi(_) => schema.clone(),
        }
    }

    pub fn parse_message_from_records(
        &self,
        records: &RecordBatch,
        filter_ts: bool,
        archive_tx: Option<&Sender<ArchiveType>>,
    ) -> Result<Message, Error> {
        match self {
            Self::Single(parser) => {
                parser.parse_message_from_records(records, filter_ts, archive_tx)
            }
            Self::Multi(pipeline) => {
                pipeline.parse_message_from_records(records, filter_ts, archive_tx)
            }
        }
    }
}

pub(super) fn parse_batch_once(
    parse: Option<&ParserImpl>,
    records: &RecordBatch,
) -> Result<RecordBatch, Error> {
    parse
        .map(|parse| parse.transform_record_batch(records))
        .transpose()?
        .map_or_else(|| Ok(records.clone()), Ok)
}

pub(super) fn apply_mutations(mutate: &[Mutate], batch: RecordBatch) -> Result<RecordBatch, Error> {
    mutate
        .iter()
        .try_fold(batch, |batch, mutate| mutate.transform_record_batch(&batch))
}

pub(super) fn build_records_message(
    global: &Arc<TableOptions>,
    metrics: Option<&Arc<CoreMetrics>>,
    model: &Modeler,
    s_model: Option<&STableModel>,
    transformed_batch: &RecordBatch,
    filter_ts: bool,
    archive_tx: Option<&Sender<ArchiveType>>,
) -> Result<Message, Error> {
    let schema = transformed_batch.schema();

    let pivot_fields = schema
        .fields()
        .iter()
        .filter_map(|f| {
            f.name()
                .strip_prefix("${")
                .and_then(|name| name.strip_suffix("}"))
                .map(|name| (name, f.name().as_str()))
        })
        .collect_vec();

    let stables = s_model
        .map(|s| s.apply(transformed_batch, global))
        .transpose()
        .context("apply stable name error")?;

    let mut data = vec![];
    let json_batch = std::cell::OnceCell::new();

    'table: for table in model {
        let mut archive_indices = HashMap::new();
        let mut skip_indices = HashMap::new();
        let mut use_current_time_indices = HashMap::new();

        let mut columns_indices = Vec::from_iter(0..transformed_batch.num_columns());
        let spec_columns = if let Some(cols) = &table.columns {
            let mut indices = Vec::new();
            for name in cols {
                if let Ok(index) = schema.index_of(name) {
                    indices.push(index);
                    continue;
                }
                if let Some((index, _)) =
                    Parser::get_schema_column_with_name(&schema, name.as_str())
                {
                    indices.push(index);
                    continue;
                }
                warn!("selected column {name} not found in stream message");
            }
            indices
        } else {
            Vec::new()
        };
        let (tags, columns) = if let Some(tags) = &table.tags {
            let mut indices = vec![];
            for name in tags {
                if let Ok(index) = schema.index_of(name) {
                    indices.push(index);
                    columns_indices[index] = usize::MAX;
                    continue;
                }
                let (i, _) = Parser::get_schema_column_with_name(&schema, name.as_str())
                    .ok_or_else(|| anyhow::format_err!("invalid field name `{name}`"))?;
                indices.push(i);
                columns_indices[i] = usize::MAX;
            }
            let tags = transformed_batch.project(&indices)?;
            let cols = if spec_columns.is_empty() {
                columns_indices
                    .into_iter()
                    .filter(|v| *v != usize::MAX)
                    .collect_vec()
            } else {
                spec_columns
            };
            (Some(tags), transformed_batch.project(&cols).unwrap())
        } else {
            let cols = if spec_columns.is_empty() {
                columns_indices
            } else {
                spec_columns
            };
            (None, transformed_batch.project(&cols).unwrap())
        };

        let all_fields: Vec<&Arc<arrow_schema::Field>> = if let Some(tags) = &tags {
            columns
                .schema_ref()
                .fields()
                .iter()
                .chain(tags.schema_ref().fields().iter())
                .collect()
        } else {
            columns.schema_ref().fields().iter().collect()
        };
        for field in all_fields.clone() {
            let field_name = field.name();
            if field_name.len() > 64 {
                match global
                    .process_on_abnormal
                    .field_name_length_overflow
                    .handle(
                        vec![field_name.clone()],
                        64,
                        format!("the length of field name '{field_name}' should not exceed 64"),
                    ) {
                    Ok((HandlingResult::Skip, err)) => {
                        warn!("skip the batch due to {err}");
                        break 'table;
                    }
                    Ok((HandlingResult::Archive, err)) => {
                        warn!("archive and skip the batch due to {err}");
                        let mut err_vec = vec![None; transformed_batch.num_rows()];
                        if let Some(v) = err_vec.first_mut() {
                            *v = Some(err.to_string());
                        }
                        if let Some(v) = err_vec.last_mut() {
                            *v = Some(err.to_string());
                        }
                        let mut err_timestamp_vec = vec![0; transformed_batch.num_rows()];
                        let now = Utc::now().timestamp_nanos_opt().unwrap_or_default();
                        if let Some(v) = err_timestamp_vec.first_mut() {
                            *v = now;
                        }
                        if let Some(v) = err_timestamp_vec.last_mut() {
                            *v = now;
                        }
                        archive_records_blocking(
                            transformed_batch,
                            err_vec,
                            err_timestamp_vec,
                            archive_tx,
                        )
                        .context("archive field name error")?;
                        break 'table;
                    }
                    Ok((HandlingResult::Modify(_), _)) => todo!(),
                    Ok((HandlingResult::ModifyAndArchive(_), _)) => todo!(),
                    Ok((HandlingResult::Retry, _)) => unreachable!(),
                    Err(e) => {
                        Err(Error::FieldNameLengthOverflowError(
                            field_name.to_string(),
                            e,
                        ))?;
                    }
                }
            }
        }

        if filter_ts {
            for row in 0..columns.num_rows() {
                let col = columns.column(0);
                let ts = get_primary_timestamp_ns(all_fields[0].name(), col, row)?;
                if ts.is_none() {
                    match global
                        .process_on_abnormal
                        .primary_timestamp_null
                        .handle("the primary timestamp should not be null".to_string())
                    {
                        Ok((HandlingResult::Skip, err)) => {
                            let _ = skip_indices.insert(row, err);
                        }
                        Ok((HandlingResult::Archive, err)) => {
                            let _ = skip_indices.insert(row, err.clone());
                            let _ = archive_indices.insert(row, err);
                        }
                        Ok((HandlingResult::Modify(_), err)) => {
                            let _ = use_current_time_indices.insert(row, err);
                        }
                        Ok((HandlingResult::ModifyAndArchive(_), err)) => {
                            let _ = use_current_time_indices.insert(row, err.clone());
                            let _ = archive_indices.insert(row, err);
                        }
                        Ok((HandlingResult::Retry, _)) => unreachable!(),
                        Err(_) => {
                            Err(Error::NullPrimaryKey(all_fields[0].name().clone()))?;
                        }
                    }
                    continue;
                }
                let ts = ts.unwrap() / 1_000_000;
                let mut primary_timestamp_overflow_flag = false;
                if let Some(max_ts) = global.maximum_timestamp
                    && ts > max_ts.timestamp_millis()
                {
                    primary_timestamp_overflow_flag = true;
                }
                if let Some(min_ts) = global.minimum_timestamp
                    && ts < min_ts.timestamp_millis()
                {
                    primary_timestamp_overflow_flag = true;
                }
                if primary_timestamp_overflow_flag {
                    match global
                        .process_on_abnormal
                        .primary_timestamp_overflow
                        .handle(format!("the primary timestamp {ts} overflow"))
                    {
                        Ok((HandlingResult::Skip, err)) => {
                            let _ = skip_indices.insert(row, err);
                        }
                        Ok((HandlingResult::Archive, err)) => {
                            let _ = skip_indices.insert(row, err.clone());
                            let _ = archive_indices.insert(row, err);
                        }
                        Ok((HandlingResult::Modify(_), _)) => unreachable!(),
                        Ok((HandlingResult::ModifyAndArchive(_), _)) => unreachable!(),
                        Ok((HandlingResult::Retry, _)) => unreachable!(),
                        Err(e) => {
                            Err(Error::PrimaryTimestampOverflow(format!("{e:#}")))?;
                        }
                    }
                }
            }
        }

        let using_expr = table
            .using
            .as_ref()
            .map(|name| FastStrExpr::new(name.clone().into()));

        let active_rows = (0..transformed_batch.num_rows())
            .filter(|row| !skip_indices.contains_key(row))
            .collect_vec();
        let tables = active_rows
            .into_iter()
            .map(|row| {
                match generate_table_name(
                    global.process_on_abnormal.clone(),
                    table,
                    row,
                    transformed_batch,
                    &table.name,
                    &json_batch,
                )? {
                    (HandlingResult::Skip, err) => {
                        let _ = skip_indices.insert(row, err);
                        anyhow::Ok((String::default(), row))
                    }
                    (HandlingResult::Archive, err) => {
                        let _ = skip_indices.insert(row, err.clone());
                        let _ = archive_indices.insert(row, err);
                        Ok((String::default(), row))
                    }
                    (HandlingResult::Modify(mut name), _) => {
                        Ok((name.pop().unwrap_or_default(), row))
                    }
                    (HandlingResult::ModifyAndArchive(mut name), err) => {
                        let _ = archive_indices.insert(row, err);
                        Ok((name.pop().unwrap_or_default(), row))
                    }
                    (HandlingResult::Retry, _) => unreachable!(),
                }
            })
            .try_collect::<_, Vec<_>, _>()
            .context("generate table name error")?
            .into_iter()
            .into_group_map();

        if !archive_indices.is_empty() {
            let mut archive_indices_vec = Vec::with_capacity(archive_indices.len());
            let mut err_vec = Vec::with_capacity(archive_indices.len());
            let mut err_timestamp_vec = Vec::with_capacity(archive_indices.len());
            let now = Utc::now().timestamp_nanos_opt().unwrap_or_default();
            archive_indices.iter().for_each(|(row, err)| {
                archive_indices_vec.push(*row);
                err_vec.push(Some(err.clone()));
                err_timestamp_vec.push(now);
            });
            let archive_batches = archive_indices_vec
                .iter()
                .map(|row| transformed_batch.slice(*row, 1))
                .collect_vec();
            let archive_batch = concat_batches(&transformed_batch.schema(), &archive_batches)
                .context("concat archive batch error")?;
            archive_records_blocking(&archive_batch, err_vec, err_timestamp_vec, archive_tx)
                .context("archive abnormal batch error")?;
        }

        let ts_field_name = table
            .columns
            .as_ref()
            .and_then(|v| v.first())
            .context("ts field not found")?;

        if let Some(metrics) = metrics {
            metrics
                .ipc()
                .add_check_skipped_rows(skip_indices.len() as u64);
        }

        for (name, indices) in tables {
            let indices = if skip_indices.is_empty() {
                indices
            } else {
                indices
                    .into_iter()
                    .filter(|row| !skip_indices.contains_key(row))
                    .collect_vec()
            };

            if name.is_empty() || indices.is_empty() {
                continue;
            }

            let columns = if use_current_time_indices.is_empty() {
                columns.clone()
            } else {
                let time_array: Vec<_> = (0..columns.num_rows())
                    .map(|row| {
                        if use_current_time_indices.contains_key(&row) {
                            Utc::now().timestamp_nanos_opt()
                        } else {
                            match get_primary_timestamp_ns(
                                all_fields[0].name(),
                                columns.column(0),
                                row,
                            ) {
                                Ok(Some(ts)) => Some(ts),
                                _ => None,
                            }
                        }
                    })
                    .collect();
                RecordBatch::try_new(
                    columns.schema(),
                    columns
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(i, col)| {
                            if i == 0 {
                                Arc::new(arrow::array::TimestampNanosecondArray::from(
                                    time_array.clone(),
                                )) as ArrayRef
                            } else {
                                col.clone()
                            }
                        })
                        .collect::<Vec<_>>(),
                )?
            };

            let ranges = indices_to_ranges(&indices);
            let name_row = indices[0];
            let using = using_expr
                .as_ref()
                .map(|expr| expr.eval(transformed_batch, name_row))
                .transpose()?
                .map(|using| global.canonical_table_name(&using).to_string());

            let tags = tags
                .as_ref()
                .map(|batch| Arc::new(batch.slice(name_row, 1)));
            let using = match (&stables, using) {
                (Some(map), Some(using)) => map
                    .get(&faststr::FastStr::from(using))
                    .map(|m| Arc::new(STable::Model(m.clone()))),
                (None, Some(using)) => Some(Arc::new(STable::Name(using))),
                (_, None) => None,
            };

            if pivot_fields.is_empty() {
                let sub_table_batches = ranges
                    .iter()
                    .map(|range| columns.slice(range.start, range.len()))
                    .collect_vec();
                let sub_table_batch = concat_batches(&columns.schema(), sub_table_batches.iter())?;
                if let Some(metrics) = metrics {
                    metrics
                        .ipc()
                        .add_write_ready_rows(sub_table_batch.num_rows() as u64);
                }
                let meta = MessageTableMeta::new(name, using, tags);
                data.push(MessageArrowRecords {
                    table: meta,
                    records: sub_table_batch,
                    opts: global.clone(),
                });
            } else {
                let meta = MessageTableMeta::new(name.clone(), using.clone(), tags.clone());
                let batches = ranges
                    .iter()
                    .map(|range| transformed_batch.slice(range.start, range.len()))
                    .collect_vec();
                let pivot_batch = concat_batches(transformed_batch.schema_ref(), batches.iter())?;
                let common_cols = table.columns.as_ref().map(|cols| {
                    cols.iter()
                        .filter(|col| !pivot_fields.iter().any(|(a, b)| a == col || b == col))
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                });
                let pivot_batches = pivot(
                    &pivot_batch,
                    ts_field_name,
                    &pivot_fields,
                    common_cols.as_deref(),
                )?;
                for batch in pivot_batches {
                    if let Some(metrics) = metrics {
                        metrics.ipc().add_write_ready_rows(batch.num_rows() as u64);
                    }
                    data.push(MessageArrowRecords {
                        table: meta.clone(),
                        records: batch,
                        opts: global.clone(),
                    });
                }
            }
        }
    }

    Ok(Message::Records(data))
}

fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch, Error> {
    if indices.is_empty() {
        return Ok(batch.slice(0, 0));
    }
    let idx_array = UInt32Array::from(
        indices
            .iter()
            .map(|idx| u32::try_from(*idx))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow::anyhow!("row index overflow: {err}"))?,
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &idx_array, None).map_err(Error::from))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn unmatched_row_sample(batch: &RecordBatch) -> String {
    batch
        .to_json_rows::<serde_json::Map<String, serde_json::Value>>()
        .map(|rows| rows.into_iter().take(3).collect_vec())
        .map(|rows| serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        .unwrap_or_else(|_| "[]".to_string())
}

fn get_ipcdatatype_from_parse<'a>(
    parse: Option<&'a ParserImpl>,
    column_name: &str,
) -> Option<&'a IpcDataType> {
    let payload = parse?.get("payload")?;
    match payload {
        FieldParser::Json(json) => match &json.json {
            Select::Include(incl) => incl.iter().find_map(|item| {
                if (item.alias().is_some() && item.alias().unwrap() == column_name)
                    || item.name() == column_name
                {
                    item.cast()
                } else {
                    None
                }
            }),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering::SeqCst;

    use arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use serde_json::json;

    use super::{MultiPipeline, TransformConfig};
    use crate::core_metrics::CoreMetrics;
    use crate::plugins::sink::ipc_metric::IpcMetrics;
    use crate::plugins::transform::Message;

    #[test]
    fn test_transform_config_deserializes_legacy_single() {
        let config: TransformConfig = serde_json::from_value(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "mutate": [{ "filter": ["value > 1"] }],
            "model": {
                "name": "legacy_table",
                "columns": ["ts", "value"]
            }
        }))
        .unwrap();

        assert!(matches!(config, TransformConfig::Single(_)));
    }

    #[test]
    fn test_transform_config_deserializes_multi() {
        let config: TransformConfig = serde_json::from_value(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [{
                "matches": { "expr": "kind == \"alpha\"" },
                "model": {
                    "name": "alpha_table",
                    "columns": ["ts", "value"]
                }
            }]
        }))
        .unwrap();

        assert!(matches!(config, TransformConfig::Multi(_)));
    }

    #[test]
    fn test_transform_config_deserializes_structured_condition_expr() {
        let config: TransformConfig = serde_json::from_value(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [{
                "matches": { "expr": "kind == \"alpha\"" },
                "mutate": [{ "filter": { "expr": "value > 1" } }],
                "model": {
                    "name": "alpha_table",
                    "columns": ["ts", "value"]
                }
            }]
        }))
        .unwrap();

        assert!(matches!(config, TransformConfig::Multi(_)));
    }

    #[test]
    fn test_multi_pipeline_first_match_wins() {
        let pipeline = multi_pipeline(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [
                {
                    "matches": { "expr": "value <= 2" },
                    "model": {
                        "name": "first_rule",
                        "columns": ["ts", "value"]
                    }
                },
                {
                    "matches": { "expr": "value >= 2" },
                    "model": {
                        "name": "second_rule",
                        "columns": ["ts", "value"]
                    }
                }
            ]
        }));

        let message = pipeline
            .parse_message_from_records(&sample_records(), false, None)
            .unwrap();

        assert_eq!(
            record_counts_by_table(message),
            HashMap::from([
                ("first_rule".to_string(), 2_usize),
                ("second_rule".to_string(), 1_usize)
            ])
        );
    }

    #[test]
    fn test_multi_pipeline_excludes_unmatched_rows() {
        let mut pipeline = multi_pipeline(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [{
                "matches": { "expr": "value == 1" },
                "model": {
                    "name": "matched_rule",
                    "columns": ["ts", "value"]
                }
            }]
        }));
        let metrics = Arc::new(CoreMetrics::IPC(IpcMetrics::new(
            "test_ipc".to_string(),
            -1,
            -1,
            None,
        )));
        pipeline.set_metrics(metrics.clone());

        let message = pipeline
            .parse_message_from_records(&sample_records(), false, None)
            .unwrap();

        assert_eq!(
            record_counts_by_table(message),
            HashMap::from([("matched_rule".to_string(), 1_usize)])
        );
        assert_eq!(metrics.ipc().unmatched_rows.load(SeqCst), 2);
    }

    #[test]
    fn test_multi_pipeline_failed_rule_does_not_stop_later_rules() {
        let pipeline = multi_pipeline(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [
                {
                    "matches": { "expr": "value == 1" },
                    "model": {
                        "name": "broken_rule",
                        "using": "broken_stable",
                        "tags": ["missing"],
                        "columns": ["ts", "value"]
                    }
                },
                {
                    "matches": { "expr": "value >= 1" },
                    "model": {
                        "name": "later_rule",
                        "columns": ["ts", "value"]
                    }
                }
            ]
        }));

        let message = pipeline
            .parse_message_from_records(&sample_records(), false, None)
            .unwrap();

        assert_eq!(
            record_counts_by_table(message),
            HashMap::from([("later_rule".to_string(), 2_usize)])
        );
    }

    #[test]
    fn test_multi_pipeline_accepts_structured_condition_expr_for_matches_and_filter() {
        let pipeline = multi_pipeline(json!({
            "parse": { "payload": { "json": ["kind", "value::int"] } },
            "rules": [{
                "matches": { "expr": "value >= 1" },
                "mutate": [{ "filter": { "expr": "value >= 2" } }],
                "model": {
                    "name": "structured_rule",
                    "columns": ["ts", "value"]
                }
            }]
        }));

        let message = pipeline
            .parse_message_from_records(&sample_records(), false, None)
            .unwrap();

        assert_eq!(
            record_counts_by_table(message),
            HashMap::from([("structured_rule".to_string(), 2_usize)])
        );
    }

    fn multi_pipeline(config: serde_json::Value) -> MultiPipeline {
        serde_json::from_value(config).unwrap()
    }

    fn sample_records() -> RecordBatch {
        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_i64, 2, 3]));
        let payload: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"kind":"alpha","value":1}"#,
            r#"{"kind":"beta","value":2}"#,
            r#"{"kind":"gamma","value":3}"#,
        ]));

        RecordBatch::try_from_iter(vec![("ts", ts), ("payload", payload)]).unwrap()
    }

    fn record_counts_by_table(message: Message) -> HashMap<String, usize> {
        match message {
            Message::Records(records) => records
                .into_iter()
                .map(|record| ((*record.table.name).clone(), record.records.num_rows()))
                .collect(),
            other => panic!("expected records message, got {other:?}"),
        }
    }
}
