use actix_web::web::{Data, Json, Query};
use actix_web::{get, post, HttpResponse, Responder};
use csv_async::AsyncWriter;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{Code, IntoDsn};
use utoipa::*;

use taosx_core::runners::opc::config::csv::column::CsvColumn;
use taosx_core::{get_data_dir, runners};
use taosx_ipc::prelude::IpcDataType;

use crate::serve::controller::TaskControllerRef;
use crate::serve::task::Failed;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct PointDetail {
    /// 在 csv 中的顺序
    index: usize,
    /// 列名，在 csv 的 header 中
    name: String,
    /// 是否必填
    required: bool,
    /// 列值，在 csv 的 row 中
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    /// 列的数据类型
    #[serde(skip_serializing_if = "Option::is_none")]
    r#type: Option<IpcDataType>,
    /// 是否是 tag
    is_tag: bool,
    /// 是否是 ts 主键
    is_primary_key: bool,
    /// 是否是时间戳列
    is_timestamp: bool,
    /// 是否是表达式
    is_expression: bool,
    /// 可选项，explorer 中的选择项
    #[serde(skip_serializing_if = "Option::is_none")]
    choices: Option<Vec<String>>,
    /// 默认值，explorer 中的默认值
    #[serde(rename = "defaultValue", skip_serializing_if = "Option::is_none")]
    default_value: Option<String>,
    /// English 描述
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// 中文描述
    #[serde(skip_serializing_if = "Option::is_none")]
    description_cn: Option<String>,
}

impl From<CsvColumn> for PointDetail {
    fn from(col: CsvColumn) -> Self {
        Self {
            index: col.index,
            name: col.name.clone(),
            required: match col.name.clone().as_str() {
                "point_id" | "tag_name" | "stable" | "tbname" => true,
                _ => false,
            },
            value: None,
            r#type: col.tag_type,
            is_tag: col.is_tag,
            is_primary_key: col.is_primary_key,
            is_timestamp: col.is_timestamp,
            is_expression: col.is_expression,
            choices: match col.name.clone().as_str() {
                "enabled" => Some(vec!["0".to_string(), "1".to_string()]),
                _ => None,
            },
            default_value: match col.name.clone().as_str() {
                "enabled" => Some("1".to_string()),
                _ => None,
            },
            description: {
                if col.is_tag {
                    Some("Tag column corresponding to the data point in the TDengine.".to_string())
                } else {
                    match col.name.clone().as_str() {
                        "point_id" => Some("The id of the data point on the OPC UA server".to_string()) ,
                        "tag_name" =>Some("The id of the data point on the OPC DA server".to_string()) ,
                        "stable" =>Some("The id of the data point on the OPC UA server".to_string()) ,
                        "tbname" =>Some("The id of the data point on the OPC UA server".to_string()) ,
                        "enabled" =>Some("Whether to collect data for this point, optional. If the enabled column is not configured, a uniform default value of 1 will be used as the value of enabled".to_string()) ,
                        "value_col" =>Some("The column name of the data point's collected value in TDengine. If the value_col is not configured, a uniform default value of val will be used as the value of value_col".to_string()) ,
                        "value_transform" =>Some("The transform function executed in taosX for the data point's collected value, optional. If value_transform is not configured, transform will not be applied uniformly".to_string()) ,
                        "type" =>Some("The data type of the data point's collected value, optional. If the type column is not configured, the original type of the collected value will be used as the data type in TDengine".to_string()) ,
                        "quality_col" =>Some("The column name of the data point's collected value quality in TDengine, optional. If quality_col is not configured, the quality column will not be added in TDengine".to_string()) ,
                        "ts_col" =>Some("The original timestamp of the data point corresponding to the timestamp column in TDengine, optional. If both ts_col and received_ts_col are present, ts_col will be as the timestamp column in TDengine; If only ts_col is present, it will be used as the timestamp column in TDengine".to_string()) ,
                        "ts_transform" =>Some("The transform function executed in taosX for the data point's timestamp, optional. If ts_transform is not configured, there will be no transform applied uniformly for the data point's original timestamp".to_string()) ,
                        "received_ts_col" =>Some("The timestamp column in TDengine corresponding to the time when the data point's collected value was received, optional. If both received_ts_col and ts_col are present, received_ts_col will be used as the timestamp column in TDengine; If only received_ts_col is present, it will be used as the timestamp column in TDengine".to_string()) ,
                        "received_ts_transform" =>Some("The transform function executed in taosX for the data point's received timestamp. If the received_ts_transform column is not configured, there will be no transform applied uniformly for the data point's received timestamp".to_string()) ,
                        _ => None,
                    }
                }
            },
            description_cn: {
                if col.is_tag {
                    Some("数据点位在 TDengine 中对应的 Tag 列".to_string())
                } else {
                    match col.name.clone().as_str() {
                        "point_id" => Some("数据点位在 OPC UA 服务器上的 id，必填".to_string()) ,
                        "tag_name" =>Some("数据点位在 OPC DA 服务器上的 id，必填".to_string()) ,
                        "stable" =>Some("数据点位在 TDengine 对应的超级表".to_string()) ,
                        "tbname" =>Some("数据点位在 TDengine 对应的子表".to_string()) ,
                        "enabled" =>Some("是否采集该点位数据，可选，不配置 enabled 列时，使用统一的默认值1作为 enabled 的值".to_string()) ,
                        "value_col" =>Some("数据点位采集值在 TDengine 中对应的列名，可选，不配置 value_col 列时，使用统一的默认值 val 作为 value_col 的值".to_string()) ,
                        "value_transform" =>Some("数据点位采集值在 taosX 中执行的变换函数，可选，不配置 value_transform 列时，统一不进行采集值的 transform".to_string()) ,
                        "type" =>Some("数据点位采集值的数据类型，可选，不配置 type 列时，统一使用采集值的原始类型作为 TDengine 中的数据类型".to_string()) ,
                        "quality_col" =>Some("数据点位采集值质量在 TDengine 中对应的列名，可选，不配置 quality_col 时，统一不在 TDengine 添加 quality 列".to_string()) ,
                        "ts_col" => Some("数据点位的原始时间戳在 TDengine 中对应的时间戳列，可选，ts_col，received_ts_col 按顺序同时存在，使用 ts_col 作 TDengine 中的时间戳列；ts_col 存在，使用 ts_col 作 TDengine 中的时间戳列".to_string()),
                        "ts_transform" =>Some("数据点位时间戳在 taosX 中执行的变换函数，可选，不配置 ts_transform 列时，统一不进行数据点位原始时间戳的 transform".to_string()) ,
                        "received_ts_col" => Some("接收到该点位采集值时的时间戳在 TDengine 中对应的时间戳列，可选，received_ts_col，ts_col 按顺序同时存在，使用 received_ts_col 作 TDengine 中的时间戳列；received_ts_col 存在，使用 received_ts_col 作 TDengine 中的时间戳列".to_string()) ,
                        "received_ts_transform" =>Some("数据点位接收时间戳在 taosX 中执行的变换函数，可选，不配置 received_ts_transform 列时，统一不进行数据点位接收时间戳的 transform".to_string()) ,
                        _ => None,
                    }
                }
            },
        }
    }
}

impl PointDetail {
    /// convert Vec<PointDetail> to csv string
    async fn to_csv(point: Vec<PointDetail>, with_header: bool) -> anyhow::Result<String> {
        // sort by index
        let point = point
            .iter()
            .sorted_by(|a, b| a.index.cmp(&b.index))
            .collect_vec();

        let mut writer = AsyncWriter::from_writer(vec![]);

        if with_header {
            let mut header = vec![];
            for p in point.iter() {
                if p.is_tag {
                    // example: tag::VARCHAR(200)::name
                    let tag_type = p
                        .r#type
                        .clone()
                        .ok_or(anyhow::anyhow!("tag type cannot be None"))?
                        .sql_repr_display();
                    let tag_header = format!("tag::{}::{}", tag_type, p.name);
                    header.push(tag_header);
                } else {
                    header.push(p.name.clone());
                }
            }
            writer.write_record(header).await?;
        }

        let mut line = vec![];
        for p in point.iter() {
            line.push(p.value.clone().unwrap_or("".to_string()));
        }
        writer.write_record(line).await?;

        Ok(String::from_utf8(writer.into_inner().await?)?)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPointsHeaderReq {
    task_id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AddPointReq {
    task_id: i64,
    point: Vec<PointDetail>,
    via: Option<i64>,
}

#[utoipa::path(
    tag = "data sources",
    params(
        ("task_id" = i64, Query, description = "task id")
    ),
    responses(
        (
            status = 200,
            description = "get the OPC-UA/OPC-DA point configuration header from the CSV config file",
            body = Vec<PointsHeader>
        ),
        (
            status = 500,
            description = "failed to get point configs",
            body = Failed
        ),
    )
)]
#[get("/ds/in/opc/csv/points/header")]
pub async fn get_point_header(
    req: Query<GetPointsHeaderReq>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let task_id = req.task_id;

    match get_point_header_impl(task_id, task_store).await {
        Ok(headers) => Ok(HttpResponse::Ok().json(headers)),
        Err(err) => Err(Failed::new(
            Code::FAILED,
            format!("failed to get point headers: {:?}", err),
            (),
        )),
    }
}

async fn get_point_header_impl(
    task_id: i64,
    task_store: Data<TaskControllerRef>,
) -> anyhow::Result<Vec<PointDetail>> {
    // find task detail
    let task = task_store
        .get(task_id)
        .await?
        .ok_or(anyhow::anyhow!("task: {} not found", task_id))?;

    // get dsn
    let dsn = task.from.clone().into_dsn()?;
    tracing::debug!("get point headers for task: {}, from: {:?}", task_id, dsn);

    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // get csv header
    let csv_headers = runners::opc::get_csv_headers(&dsn).await?;

    // to PointDetail list
    let mut point_details = Vec::new();
    // get headers from the first file
    if let Some((_filename, header)) = csv_headers.iter().next() {
        for column in header.get_columns() {
            let p = PointDetail::from(column.clone());
            point_details.push(p);
        }
    }

    Ok(point_details)
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (
            status = 200,
            description = "add one OPC-UA/OPC-DA point and its configuration to the current CSV config file",
            body = PointConfig
        ),
        (
            status = 500,
            description = "failed to add point config",
            body = Failed
        ),
    )
)]
#[post("/ds/in/opc/csv/points")]
pub async fn append_point(
    req: Json<AddPointReq>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let req = req.into_inner();
    match append_point_impl(req, task_store).await {
        Ok(_) => Ok::<HttpResponse, Failed>(HttpResponse::Ok().finish()),
        Err(err) => {
            tracing::error!("failed to add point: {:#?}", err);
            Err(Failed::new(
                Code::FAILED,
                format!("failed to add point: {}", err.to_string()),
                (),
            ))
        }
    }
}

async fn append_point_impl(
    req: AddPointReq,
    task_store: Data<TaskControllerRef>,
) -> anyhow::Result<()> {
    let task_id = req.task_id;
    let point = req.point.clone();

    // find task detail
    let task = task_store
        .get(task_id)
        .await?
        .ok_or(anyhow::anyhow!("task: {} not found", task_id))?;

    // get dsn
    let dsn = task.from.clone().into_dsn()?;
    tracing::debug!("add point for task: {}, dsn: {:?}", task_id, dsn);

    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // Vec<PointDetail> to csv
    let line = PointDetail::to_csv(point, true).await?;
    tracing::debug!("append opc point to csv, data: \n{}", line);

    // append point to the csv file
    runners::opc::append_point(&dsn, line.clone()).await?;

    // send the new csv to agent if via is not None
    if let Some(agent_id) = req.via {
        task_store.send_opc_csv_to_agnet(agent_id, &dsn).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_point_detail_vec_to_csv() {
        let json = r#"
[
  {
    "index": 0,
    "name": "No.",
    "required": false,
    "value": "1",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false
  },
  {
    "index": 1,
    "name": "point_id",
    "required": true,
    "value": "ns=3;i=1001",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "The id of the data point on the OPC UA server",
    "description_cn": "数据点位在 OPC UA 服务器上的 id，必填"
  },
  {
    "index": 2,
    "name": "enabled",
    "required": false,
    "value": "1",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false
  },
  {
    "index": 3,
    "name": "stable",
    "required": true,
    "value": "opc_{type}",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "The id of the data point on the OPC UA server",
    "description_cn": "数据点位在 TDengine 对应的超级表"
  },
  {
    "index": 4,
    "name": "tbname",
    "required": true,
    "value": "t_{ns}_{id}",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "The id of the data point on the OPC UA server",
    "description_cn": "数据点位在 TDengine 对应的子表"
  },
  {
    "index": 5,
    "name": "value_col",
    "required": false,
    "value": "val",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "The column name of the data point's collected value in TDengine. If the value_col is not configured, a uniform default value of val will be used as the value of value_col",
    "description_cn": "数据点位采集值在 TDengine 中对应的列名，可选，不配置 value_col 列时，使用统一的默认值 val 作为 value_col 的值"
  },
  {
    "index": 6,
    "name": "quality_col",
    "required": false,
    "value": "quality",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "The column name of the data point's collected value quality in TDengine, optional. If quality_col is not configured, the quality column will not be added in TDengine",
    "description_cn": "数据点位采集值质量在 TDengine 中对应的列名，可选，不配置 quality_col 时，统一不在 TDengine 添加 quality 列"
  },
  {
    "index": 7,
    "name": "ts_col",
    "required": false,
    "value": "ts",
    "is_tag": false,
    "is_primary_key": true,
    "is_timestamp": true,
    "is_expression": false,
    "description": "The original timestamp of the data point corresponding to the timestamp column in TDengine, optional. If both ts_col and received_ts_col are present, ts_col will be as the timestamp column in TDengine; If only ts_col is present, it will be used as the timestamp column in TDengine",
    "description_cn": "数据点位的原始时间戳在 TDengine 中对应的时间戳列，可选，ts_col，received_ts_col 按顺序同时存在，使用 ts_col 作 TDengine 中的时间戳列；ts_col 存在，使用 ts_col 作 TDengine 中的时间戳列"
  },
  {
    "index": 8,
    "name": "received_ts_col",
    "required": false,
    "value": "rts",
    "is_tag": false,
    "is_primary_key": false,
    "is_timestamp": true,
    "is_expression": false,
    "description": "The timestamp column in TDengine corresponding to the time when the data point's collected value was received, optional. If both received_ts_col and ts_col are present, received_ts_col will be used as the timestamp column in TDengine; If only received_ts_col is present, it will be used as the timestamp column in TDengine",
    "description_cn": "接收到该点位采集值时的时间戳在 TDengine 中对应的时间戳列，可选，received_ts_col，ts_col 按顺序同时存在，使用 received_ts_col 作 TDengine 中的时间戳列；received_ts_col 存在，使用 received_ts_col 作 TDengine 中的时间戳列"
  },
  {
    "index": 9,
    "name": "name",
    "required": false,
    "type": "varchar(200)",
    "value": "标签",
    "is_tag": true,
    "is_primary_key": false,
    "is_timestamp": false,
    "is_expression": false,
    "description": "Tag column corresponding to the data point in the TDengine.",
    "description_cn": "数据点位在 TDengine 中对应的 Tag 列"
  }
]
"#;
        let point = serde_json::from_str::<Vec<PointDetail>>(json).unwrap();
        let csv = PointDetail::to_csv(point, true).await.unwrap();
        assert_eq!("No.,point_id,enabled,stable,tbname,value_col,quality_col,ts_col,received_ts_col,tag::varchar(200)::name
1,ns=3;i=1001,1,opc_{type},t_{ns}_{id},val,quality,ts,rts,标签\n", csv.as_str());

        let point = serde_json::from_str::<Vec<PointDetail>>(json).unwrap();
        let csv = PointDetail::to_csv(point, false).await.unwrap();
        assert_eq!(
            "1,ns=3;i=1001,1,opc_{type},t_{ns}_{id},val,quality,ts,rts,标签\n",
            csv.as_str()
        )
    }
}
