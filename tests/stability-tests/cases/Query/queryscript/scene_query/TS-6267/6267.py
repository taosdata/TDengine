import taos
import random
from datetime import datetime, timedelta

def generate_time_range():
    """生成最近7天内的随机时间范围"""
    # 获取当前时间和10天前的时间
    now = datetime.now()
    seven_days_ago = now - timedelta(days=10)
    
    # 生成7天内的随机起始时间
    start_timestamp = random.uniform(
        seven_days_ago.timestamp(),
        now.timestamp()
    )
    start_time = datetime.fromtimestamp(start_timestamp)
    
    # 生成结束时间（在起始时间到当前时间之间）
    max_end_timestamp = min(
        start_time.timestamp() + 10*24*3600,  # 最多10天
        now.timestamp()  # 不超过当前时间
    )
    end_timestamp = random.uniform(
        start_time.timestamp(),
        max_end_timestamp
    )
    end_time = datetime.fromtimestamp(end_timestamp)
    
    return start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')

def execute_queries(conn):
    """执行查询并返回各自的行数"""
    # 生成随机时间范围
    start_time, end_time = generate_time_range()
    val_threshold = random.randint(1, 19)
    operators = ['>', '<', '=', '>=', '<=', '!=']  # 可用的运算符列表
    val_operator = random.choice(operators)  # 随机选择一个运算符
    
    # 修改打印语句的格式
    print("\n查询条件:")
    print("=" * 50)
    print(f"使用时间范围: {start_time} 到 {end_time}")
    print(f"使用val条件: val  {val_operator} '{val_threshold}'")
    print("=" * 50)
    
    base_queries = [
        f"select count(*) from `hhfk_ts_db`.`{{}}` where ts between '{start_time}' and '{end_time}' and val{val_operator}'{val_threshold}';",  # 新增的count查询
        f"select * from `hhfk_ts_db`.`{{}}` where ts between '{start_time}' and '{end_time}' and val{val_operator}'{val_threshold}';",
        f"select ts, quality, val, rts from `hhfk_ts_db`.`{{}}` where ts between '{start_time}' and '{end_time}' and val{val_operator}'{val_threshold}';", 
        f"select ts, quality, val from `hhfk_ts_db`.`{{}}` where ts between '{start_time}' and '{end_time}' and val{val_operator}'{val_threshold}';", 
        f"select ts from `hhfk_ts_db`.`{{}}` where ts between '{start_time}' and '{end_time}' and val{val_operator}'{val_threshold}';" 
    ]
    
    results = {}
    # # 遍历所有子表
    # for table_index in range(1000):
    #     table_name = f"ADY2R_RKSF{table_index}"
    #     print(f"\n检查子表: {table_name}")
    #     print("=" * 50)
    
    # 随机选择20%的子表
    total_tables = 1000  # 总表数
    tables_to_check = random.sample(range(total_tables), total_tables // 50)  # 随机选择20%
    print(f"本次将检查 {len(tables_to_check)} 个子表:")
    print(", ".join(f"ADY2R_RKSF{i}" for i in tables_to_check))
    print("=" * 50)
    
    # 遍历选中的子表
    for table_index in sorted(tables_to_check):  # 排序以保证顺序输出
        table_name = f"ADY2R_RKSF{table_index}"
        print(f"\n检查子表: {table_name}")
        print("=" * 50)
            
        # 对每个子表执行所有查询
        for query_index, query_template in enumerate(base_queries, 1):
            query = query_template.format(table_name)
            try:
                result = conn.query(query)
                data = result.fetch_all()
                
                # 处理 count(*) 查询的特殊情况
                if query_index == 1:  # count(*) 查询
                    row_count = data[0][0] if data else 0  # count(*) 返回单个值
                else:
                    row_count = len(data) if data else 0
                
                result_key = f"Table_{table_index}_Query_{query_index}"
                results[result_key] = {
                    "table": table_name,
                    "sql": query,
                    "rows": row_count,
                    "is_count": query_index == 1  # 标记是否为count查询
                }
                
                print(f"查询 {query_index}: {query}")
                print(f"返回行数: {row_count}")
                # if row_count > 0:
                #     print("数据样例(前3行):")
                #     for row in data[:3]:
                #         print(row)
                print("-" * 50)
                
            except Exception as e:
                print(f"执行查询时出错: {e}")
                results[f"Table_{table_index}_Query_{query_index}"] = {
                    "table": table_name,
                    "sql": query,
                    "rows": 0,
                    "error": str(e),
                    "is_count": query_index == 1
                }
    
    return results

def compare_results(results):
    """比对每个子表的查询结果行数"""
    if not results:
        print("没有查询结果可供比较")
        return
    
    # 按表名分组结果
    table_results = {}
    for result_key, result in results.items():
        table_name = result["table"]
        if table_name not in table_results:
            table_results[table_name] = []
        table_results[table_name].append({
            "query_index": result_key.split("_")[-1],
            "rows": result["rows"],
            "sql": result["sql"],
            "is_count": result.get("is_count", False)
        })
    
    # 分析每个表的查询结果
    inconsistent_tables = []
    for table_name, queries in table_results.items():
        count_value = next((q["rows"] for q in queries if q["is_count"]), None)
        other_queries = [q for q in queries if not q["is_count"]]
        row_counts = [q["rows"] for q in other_queries]
        
        if count_value is not None and (len(set(row_counts)) > 1 or count_value != row_counts[0]):
            inconsistent_tables.append(table_name)
            print(f"\n表 {table_name} 的查询结果不一致:")
            print("=" * 50)
            print(f"count(*) 查询结果: {count_value} 行")
            for query in other_queries:
                print(f"查询 {query['query_index']}: {query['rows']} 行")
                print(f"SQL: {query['sql']}")
            print(f"与count(*)的最大差异: {max(abs(count_value - rc) for rc in row_counts)}")
            print("-" * 50)
    
    # 总结报告
    print("\n总结报告:")
    print("=" * 50)
    if inconsistent_tables:
        print(f"发现 {len(inconsistent_tables)} 个表的查询结果不一致:")
        for table in inconsistent_tables:
            print(f"- {table}")
    else:
        print("所有表的查询结果都一致！")

def main():
    try:
        # 直接使用配置文件路径连接数据库
        conn = taos.connect(
            host="127.0.0.1",
            config="/data/TS-6267/cfg/taos.cfg"
        )
        
        print("数据库连接成功")
        print("=" * 50)
        
        # 执行查询并比对结果
        results = execute_queries(conn)
        compare_results(results)
        
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()