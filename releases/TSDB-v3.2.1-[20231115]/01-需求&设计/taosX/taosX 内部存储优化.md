# taosX 内部存储优化

1. 配置
   - 命令行参数：--data-dir <DIR>
   - 配置文件：dataDir <DIR>
   - 环境变量：TAOSX_DATA_DIR
  以上配置的顺序即为优先级，以下称这个配置指定的目录为 ROOT
1. 所有运行时产生的数据都存储在配置中指定的目录下
   - ${ROOT}/taosx.db (SQLite DB)
   - 所有与某个具体任务有关的文件存储在 ${ROOT}/${task_id} 这个子目录中
   - ${ROOT}/${task_id} 在创建任务成功时生成，如果任务创建失败则不生成
   - 在删除任务时删除 ${ROOT}/{task_id}
2. 此实现对一切外部用户行为透明
3. 卸载 taosX 时不删除  ${ROOT}
