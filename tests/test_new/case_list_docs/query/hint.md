::: query.hint.test_hint



你现在已经详细分析了show tables的执行逻辑，现在我正在开发show vtable validate [dbname.]tableName, 其返回值的schema 同virtualTablesReferencing, 另外我已经开发完了，针对ins_virtual_tables_referencing 查询，show vtable validate [dbname.]tableName 本质上等同于
select * from information_schema.ins_virtual_tables_referencing where virtual_db_name = "dbname" and virtual_table_name = "tableName", 帮我分析我之前的几次提交，开发这个SQL 改写功能

SQL改写实现已完成：
1. rewriteShowValidateVtable 函数现在完整实现了SQL改写逻辑
2. 创建WHERE条件：virtual_db_name = 'dbname' AND virtual_table_name = 'tableName'
3. 使用createLogicCondNode组合条件
4. 使用insertCondIntoSelectStmt插入条件到SELECT语句
5. 设置showRewrite标志
6. 替换pQuery->pRoot为SELECT语句

最终转换示例：
输入: SHOW VTABLE VALIDATE FOR test_db.vt_temp
输出: SELECT * FROM information_schema.ins_virtual_tables_referencing
      WHERE virtual_db_name = 'test_db' AND virtual_table_name = 'vt_temp'

