use std::fs;
use std::fs::File;
use std::io::Write;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use taosx_ipc::types::DataSet;

use crate::runners::opc::config::collect::da::DaNodeConfig;
use crate::runners::opc::config::collect::ua::UANodeConfig;
use crate::runners::opc::config::points::UpdateMode;
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::{opc_datasets_by_command, OpcType};

pub struct PointsUpdater {
    opc_config: OPCConfig,
    opc_toml_path: String,
    mode: UpdateMode,
    interval: tokio::time::Interval,
    cancel_token: CancellationToken,
    cur_list: Vec<DataSet>,
}

impl PointsUpdater {
    pub fn from_opc_config(
        config: OPCConfig,
        config_file: String,
        token: CancellationToken,
    ) -> Self {
        let mode = config
            .clone()
            .points
            .map(|p| p.update_mode.unwrap_or(UpdateMode::None))
            .unwrap_or(UpdateMode::None);
        let interval = config
            .clone()
            .points
            .map(|p| p.update_interval.unwrap_or(600))
            .unwrap_or(600);

        Self {
            opc_config: config,
            opc_toml_path: config_file,
            mode,
            interval: tokio::time::interval(Duration::from_secs(interval as u64)),
            cancel_token: token,
            cur_list: Vec::new(),
        }
    }

    pub async fn run(&mut self) {
        if self.mode == UpdateMode::None {
            return;
        }

        tracing::info!("update points start");
        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("update points stop");
                break;
            }
            self.interval.tick().await;
            if self.cancel_token.is_cancelled() {
                break;
            }

            //  1. 查询所有符合过滤条件的点位，形成点位列表：to_list；
            let to_list = opc_datasets_by_command(&self.opc_config).await;
            if let Err(e) = to_list {
                tracing::error!(
                    "failed to get points during points updating, opc config: {:?}, cause: {}",
                    &self.opc_config,
                    e.to_string()
                );
                continue;
            }
            let to_list = to_list.unwrap();

            //  2. 对比 to_list 和当前点位列表cur_list，找出新增的点位：add_list，删除的点位：del_list；
            let add_list = diff(&to_list, &self.cur_list);
            let del_list = diff(&self.cur_list, &to_list);
            tracing::info!(
                "update points mode: {:?}, add: {}, del: {}",
                self.mode,
                format!(
                    "{:?}",
                    add_list
                        .iter()
                        .map(|ds| ds.id.clone())
                        .collect::<Vec<String>>()
                ),
                format!(
                    "{:?}",
                    del_list
                        .iter()
                        .map(|ds| ds.id.clone())
                        .collect::<Vec<String>>()
                ),
            );

            let update_result = match self.mode {
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
                    tracing::error!(
                        "failed to update points during points updating, opc config: {:?}, cause: {}",
                        &self.opc_config,
                        e.to_string()
                    );
                }
            }
        }
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
        let mut opc_config_file = File::create(&temp_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to create temporary opc config file during points updating, cause: {}",
                e.to_string()
            )
        })?;
        write!(opc_config_file, "{}", toml)?;
        tracing::debug!("update points, write opc config file\n{toml}");
        opc_config_file.sync_all().map_err(|e| {
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
        fs::rename(&temp_path, &self.opc_toml_path).map_err(|e| {
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
