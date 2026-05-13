import taos
import random
from datetime import datetime, timedelta

# def generate_time_range():
#     """生成随机的时间范围，间隔不超过1天"""
#     base_time = datetime(2025, 4, 1)  # 基准时间
    
#     start_delta = timedelta(
#         hours=random.randint(0, 23),
#         minutes=random.randint(0, 59),
#         seconds=random.randint(0, 59)
#     )
#     start_time = base_time + start_delta
    
#     max_delta = timedelta(days=1) - start_delta
#     end_delta = timedelta(
#         hours=random.randint(0, max_delta.seconds // 3600),
#         minutes=random.randint(0, 59),
#         seconds=random.randint(0, 59)
#     )
#     end_time = start_time + end_delta
    
#     return start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')

def generate_time_range():
    """生成最近7天内的随机时间范围"""
    # 获取当前时间和7天前的时间
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)
    
    # 生成7天内的随机起始时间
    start_timestamp = random.uniform(
        seven_days_ago.timestamp(),
        now.timestamp()
    )
    start_time = datetime.fromtimestamp(start_timestamp)
    
    # 生成结束时间（在起始时间到当前时间之间）
    max_end_timestamp = min(
        start_time.timestamp() + 7*24*3600,  # 最多7天
        now.timestamp()  # 不超过当前时间
    )
    end_timestamp = random.uniform(
        start_time.timestamp(),
        max_end_timestamp
    )
    end_time = datetime.fromtimestamp(end_timestamp)
    
    return start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')

def delete_data():
    try:
        # 连接数据库
        conn = taos.connect(
            host="127.0.0.1",
            config="/data/TS-6267/cfg/taos.cfg"
        )
        print("数据库连接成功")
        
        # 生成随机时间范围
        start_time, end_time = generate_time_range()
        print(f"将删除时间范围: {start_time} 到 {end_time} 的数据")
        
        # 随机选择10%的子表
        total_tables = 1000  # 总子表数量
        tables_to_delete = random.sample(range(total_tables), total_tables // 100)  # 随机选择10%
        print(f"将处理以下子表: {[f'ADY2R_RKSF{i}' for i in tables_to_delete]}")
        
        for table_index in tables_to_delete:
            table_name = f"ADY2R_RKSF{table_index}"
            
            # 先统计要删除的数据量
            count_query = f"select count(*) from hhfk_ts_db.`{table_name}` where ts between '{start_time}' and '{end_time}'"
            result = conn.query(count_query)
            count_data = result.fetch_all()
            count = count_data[0][0] if count_data else 0
            
            # 执行删除操作
            if count > 0:
                delete_query = f"delete from hhfk_ts_db.`{table_name}` where ts between '{start_time}' and '{end_time}'"
                conn.execute(delete_query)
                print(f"表 {table_name} 删除了 {count} 条数据")
            else:
                print(f"表 {table_name} 在指定时间范围内没有数据")
        
        print("\n删除操作完成")
        
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    delete_data()