use std::fs::File;
use std::io::Write;
use std::time::Duration;
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use crate::get_data_dir;
use crate::runners::opc::config::collect::da::DaNodeConfig;
use crate::runners::opc::config::collect::ua::UANodeConfig;
use crate::runners::opc::config::points::UpdateMode;
use crate::runners::opc::config::{OPCConfig, PointsMode};
use crate::runners::opc::{opc_datasets_by_command, opc_datasets_by_csv, OpcType};
use taosx_ipc::types::DataSet;

#[derive(Debug, Clone)]
pub enum UpdateBy {
    Command,
    Csv(String),
}

#[derive(Debug)]
pub struct PointsUpdater {
    origin_dsn: Dsn,
    opc_config: OPCConfig,
    opc_toml_path: String, // 生成 taosx-opc 的配置文件的路径
    update_by: UpdateBy,
    update_mode: UpdateMode,
    update_interval: tokio::time::Interval,
    cancel_token: CancellationToken,
    cur_list: Vec<DataSet>,
}

impl PointsUpdater {
    pub fn try_new(
        origin_dsn: Dsn,
        opc_config: OPCConfig,
        opc_toml_path: String,
        token: CancellationToken,
    ) -> anyhow::Result<Self> {
        let points_mode = opc_config
            .clone()
            .points_mode
            .ok_or(anyhow::anyhow!("points mode cannot be None"))?;

        let update_by = match points_mode {
            PointsMode::ByCsv => {
                let csv_config_files = OPCConfig::parse_csv_config_files(&origin_dsn).ok_or(
                    anyhow::anyhow!("csv config file not found in dsn: {:?}", origin_dsn),
                )?;
                let csv = csv_config_files.get(0).ok_or(anyhow::anyhow!(
                    "cannot found the first csv config file in dsn: {:?}",
                    origin_dsn
                ))?;
                UpdateBy::Csv(csv.clone())
            }
            PointsMode::ByCommand => UpdateBy::Command,
        };

        let update_mode = opc_config
            .clone()
            .points
            .map(|p| p.update_mode.clone().unwrap_or(UpdateMode::None))
            .unwrap_or(UpdateMode::None);
        let update_interval = opc_config
            .clone()
            .points
            .map(|p| p.update_interval.unwrap_or(600))
            .unwrap_or(600);

        Ok(Self {
            origin_dsn,
            opc_config,
            opc_toml_path,
            update_by,
            update_mode,
            update_interval: tokio::time::interval(Duration::from_secs(update_interval as u64)),
            cancel_token: token,
            cur_list: vec![],
        })
    }

    pub async fn run(&mut self) {
        if self.update_mode == UpdateMode::None {
            return;
        }
        // set current dir to DATA_DIR
        let _ = std::env::set_current_dir(get_data_dir());

        tracing::info!("update points thread started");
        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }
            self.update_interval.tick().await;
            if self.cancel_token.is_cancelled() {
                break;
            }

            //  1. 查询所有符合过滤条件的点位，形成点位列表：to_list；
            let to_list = match &self.update_by {
                UpdateBy::Command => opc_datasets_by_command(&self.opc_config).await,
                UpdateBy::Csv(csv) => {
                    let opc_type = self.opc_config.opc_type.clone();
                    let csv = csv.clone();
                    let csv_path = OPCConfig::parse_csv_origin(&self.origin_dsn);
                    opc_datasets_by_csv(opc_type, csv, csv_path).await
                }
            };
            if let Err(e) = to_list {
                tracing::error!("failed to get to_list, cause: {}", e.to_string());
                continue;
            }
            let to_list = to_list.unwrap();

            //  2. 对比 to_list 和当前点位列表cur_list，找出新增的点位：add_list，删除的点位：del_list；
            let add_list = diff(&to_list, &self.cur_list);
            let del_list = diff(&self.cur_list, &to_list);
            tracing::info!(
                "update points mode: {:?}, add list: {:?}, del list: {:?}",
                self.update_mode,
                add_list,
                del_list
            );

            let update_result = match self.update_mode {
                //  3. append 模式下，如果 add_list 为空，则等待进入下次点位检查；如果add_list不为空，将add_list写入配置文件的点位列表；
                UpdateMode::Append => {
                    if add_list.is_empty() {
                        continue;
                    } else {
                        self.cur_list.extend(add_list);
                        self.update_config_file(self.cur_list.clone())
                    }
                }
                //  4. update 模式下，如果 add_list 和 del_list 都为空，则等待进入下次点位检查；如果 add_list 或 del_list 不为空，用 to_list 替换 cur_list，写入配置文件的点位列表；
                UpdateMode::Update => {
                    if add_list.is_empty() && del_list.is_empty() {
                        continue;
                    } else {
                        self.cur_list = to_list.clone();
                        self.update_config_file(self.cur_list.clone())
                    }
                }
                _ => Ok(()),
            };

            match update_result {
                Ok(_) => {
                    tracing::info!("update points success");
                }
                Err(e) => {
                    tracing::error!("failed to update points, cause: {}", e.to_string());
                }
            }
        }
        tracing::info!("update points thread stopped");
    }

    /// Vec<DataSet> -> config_file
    fn update_config_file(&mut self, data_set: Vec<DataSet>) -> anyhow::Result<()> {
        let points = data_set
            .iter()
            .map(|ds| ds.id.clone())
            .collect::<Vec<String>>();

        let (ua, da) = match self.opc_config.opc_type {
            OpcType::OPCUA => {
                let mut ua_nodes = vec![];
                for p in points {
                    ua_nodes.push(UANodeConfig::new(p.clone()));
                }
                (Some(ua_nodes), None)
            }
            OpcType::OPCDA => {
                let mut da_nodes = vec![];
                for p in points {
                    da_nodes.push(DaNodeConfig::new(p.clone()));
                }
                (None, Some(da_nodes))
            }
            OpcType::FAKE => {
                unimplemented!("fake opc type")
            }
        };

        // 更行 new_opc_config 中的 collect.ua.nodes 和 collect.da.tags
        let mut new_opc_config = self.opc_config.clone();
        if let Some(collect) = &mut new_opc_config.collect {
            if let Some(ua_collect) = &mut collect.ua {
                ua_collect.nodes = ua.unwrap();
            }
            if let Some(da_collect) = &mut collect.da {
                da_collect.tags = da.unwrap();
            }
        }
        self.opc_config = new_opc_config;

        // 更新配置文件
        let toml = toml::to_string(&self.opc_config).map_err(|e| {
            anyhow::anyhow!(
                "failed to serialize opc config to toml during points updating, cause: {}",
                e.to_string()
            )
        })?;

        let temp_path = format!("{}.temp", &self.opc_toml_path);
        let mut temp_file = File::create(&temp_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to create temporary opc config file during points updating, cause: {}",
                e.to_string()
            )
        })?;
        write!(temp_file, "{}", toml)?;
        tracing::debug!("update points, write opc config file\n{toml}");
        temp_file.sync_all().map_err(|e| {
            anyhow::anyhow!(
                "failed to sync temporary opc config file during points updating, cause: {}",
                e.to_string()
            )
        })?;
        tracing::debug!(
            "rename temp: {} to the opc config file: {}",
            &temp_path,
            &self.opc_toml_path
        );

        std::fs::rename(temp_path, &self.opc_toml_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to rename temporary opc config file during points updating, cause: {}",
                e.to_string()
            )
        })?;

        Ok(())
    }
}

fn diff(s1: &Vec<DataSet>, s2: &Vec<DataSet>) -> Vec<DataSet> {
    s1.iter().filter(|x| !s2.contains(x)).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff() {
        let s1 = vec![
            DataSet::new("1"),
            DataSet::new("2"),
            DataSet::new("3"),
            DataSet::new("4"),
            DataSet::new("5"),
        ];
        let s2 = vec![
            DataSet::new("3"),
            DataSet::new("4"),
            DataSet::new("5"),
            DataSet::new("6"),
            DataSet::new("7"),
        ];
        let d1 = diff(&s1, &s2);
        assert_eq!(d1, vec![DataSet::new("1"), DataSet::new("2")]);

        let d2 = diff(&s2, &s1);
        assert_eq!(d2, vec![DataSet::new("6"), DataSet::new("7")]);
    }
}
