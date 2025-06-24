import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream

class StreamGenerator:
    def __init__(self):
        # 定义窗口类型选项
        self.interval_windows = [
            "interval({interval}s) sliding({sliding}s)"
        ]
        
        self.state_windows = [
            "state_window ({col})"
        ]
        
        self.session_windows = [
            "session(ts, {gap}a)",  
            "session(ts, {gap}s)",
            "session(ts, {gap}m)",
            "session(ts, {gap}h)"
        ]
        
        self.count_windows = [
            "COUNT_WINDOW({count})",
            "COUNT_WINDOW({count}, {sliding})"
        ]
        
        self.period_windows = [
            "PERIOD({period}{unit})",  # 基本形式
            "PERIOD({period}{unit}, {offset}{offset_unit})"  # 带偏移的形式
        ]
        
        self.ts_functions = {
            'meters': {
            'col': 'cts',  # meters表的时间戳列名
            'common': [     # 通用函数,所有窗口类型都支持
                "last({col})",
                "first({col})", 
                "last_row({col})"
            ],
            'window': [     # 仅普通窗口支持的函数
                "_twstart",
                "_twend"
            ],
            'period': [     # period窗口特有的函数
                "cast(_tlocaltime/1000000 as timestamp)",
                "last({col})",
                "first({col})"
            ]
        },
        'stream_trigger': {
            'col': 'ts',   # stream_trigger表的时间戳列名
            'common': [     # 通用函数
                "last({col})",
                "first({col})",
                "last_row({col})"
            ],
            'window': [     # 仅普通窗口支持的函数
                "_twstart",
                "_twend"
            ],
            'period': [     # period窗口特有的函数
                "cast(_tlocaltime/1000000 as timestamp)",
                "last({col})",
                "first({col})"
            ]
        }
    }
        
        # 定义可用的聚合函数
        self.agg_functions = [
            "count(*)",
            "avg({col})",
            "sum({col})",
            "max({col})",
            "min({col})",
            "first({col})",
            "last({col})",
            "last_row({col})"
        ]
        
        # 定义源表(meters)的列名
        self.meters_columns = {
            "timestamp": ["cts"],
            "numeric": ["cint", "cuint"]
        }
        
        # 定义触发表(stream_trigger)的列名
        self.trigger_columns = {
            "timestamp": ["ts"],
            "numeric": ["c1", "c2"]
        }
    
    def _get_random_where_condition(self, source_table, window_type='window'):
        """根据源表和窗口类型生成随机的WHERE条件
        
        Args:
            source_table: 源表名称,可以是 'meters' 或 'stream_trigger'
            window_type: 窗口类型,可选值: 'window'(普通窗口), 'period'(周期窗口), 'session'(会话窗口)等
        Returns:
            str: 生成的WHERE条件
        """
        if source_table == 'stream_trigger' or source_table == '%%trows':
            # 根据窗口类型选择可用的WHERE条件
            if window_type in ['session', 'count']:  # 事件窗口可以使用 _twstart/_twend
                conditions = [
                    "",  # 不使用WHERE条件
                    "where ts >= _twstart and ts < _twend",  
                    f"where {random.choice(self.trigger_columns['numeric'])} > 0",
                ]
            else:  # period窗口等其他类型
                conditions = [
                    "",  # 不使用WHERE条件
                    f"where {random.choice(self.trigger_columns['numeric'])} > 0",
                    f"where {random.choice(self.trigger_columns['numeric'])} between 0 and 50",
                ]
        else:  # meters表
            if window_type in ['session', 'count']:  # 事件窗口
                conditions = [
                    "",  # 不使用WHERE条件
                    "where cts >= _twstart and cts < _twend",
                    f"where {random.choice(self.meters_columns['numeric'])} > 0",
                ]
            else:  # period窗口等其他类型
                conditions = [
                    "",  # 不使用WHERE条件
                    f"where {random.choice(self.meters_columns['numeric'])} > 0",
                    f"where {random.choice(self.meters_columns['numeric'])} between 0 and 50",
                ]
                
        return random.choice(conditions)
        
        
    def _get_ts_function(self, source_table, window_type='window'):
        """根据源表获取正确的时间戳处理函数
        
        Args:
            source_table: 源表名称
            window_type: 窗口类型,可选值: 'window'(普通窗口), 'period'(周期窗口)
        Returns:
            str: 格式化后的时间戳处理函数
        """
        table_type = 'meters' if source_table == 'meters' else 'stream_trigger'
        ts_info = self.ts_functions[table_type]
        
        # 根据窗口类型选择可用的函数列表
        if window_type == 'period':
            available_funcs = ts_info['common'] + ts_info['period']
        else:  # 普通窗口
            available_funcs = ts_info['common'] + ts_info['window']
        
        # 随机选择一个函数
        func = random.choice(available_funcs)
        
        # 如果函数中包含{col},则替换为对应的时间戳列名
        if '{col}' in func:
            return func.format(col=ts_info['col'])
        return func
    
    def generate_session_stream(self, stream_number, database):
        """专门生成session类型的stream"""
        # 随机选择session窗口类型
        window_type = random.choice(self.session_windows)
        
        # 为session窗口专门设置参数范围
        params = {
            "gap": random.randint(1, 60),  # session gap范围1-60
        }
        
        # 格式化窗口类型
        window = window_type.format(**params)
        
        # 随机选择源表
        source_table = random.choice(['meters', 'stream_trigger','%%trows'])
        
        # 根据源表选择列名
        columns = (
            self.meters_columns['numeric'] if source_table == 'meters' 
            else self.trigger_columns['numeric']
        )
        
        # 为session窗口选择合适的聚合函数
        aggs = random.sample(self.agg_functions, random.randint(1, 5))  # session通常使用1-5个聚合函数
        aggs = [agg.format(col=random.choice(columns)) for agg in aggs]
        
        # 根据源表选择合适的时间戳处理函数
        ts_function = self._get_ts_function(source_table)
        
        # 修改select子句,使用随机选择的时间戳处理函数
        select_clause = f"select {ts_function} ts, {', '.join(aggs)}"
        
        # session建议使用partition by
        partition_by = "partition by tbname" if random.choice([True, False]) else ""
                
        # 随机生成WHERE条件(基于选择的源表)
        where_clause = self._get_random_where_condition(source_table, 'session')
        
        
        # 构建完整的create stream语句
        stream_sql = f"""create stream {database}.s{stream_number} {window} 
            from {database}.stream_trigger {partition_by} 
            into {database}.st{stream_number} 
            as {select_clause} 
            from {source_table}  
            {where_clause};"""
            
        return stream_sql
    
    def generate_count_stream(self, stream_number, database):
        """专门生成count window类型的stream
        
        Args:
            stream_number: stream编号
            database: 数据库名
        Returns:
            str: 生成的stream SQL语句
        """
        # 随机选择count window类型
        window_type = random.choice(self.count_windows)
        
        # 为count window设置参数
        count_val = random.randint(2, 2147483647)  # 计数条数范围2-2147483647
        sliding_val = random.randint(1, min(1000, count_val))  # 滑动条数不超过count

        params = {
            "count": count_val,
            "sliding": sliding_val
        }
        
        # 随机选择源表
        source_table = random.choice(['meters', 'stream_trigger', '%%trows'])
        
        # 根据源表选择列名
        agg_columns = (
            self.meters_columns['numeric'] if source_table == 'meters' 
            else self.trigger_columns['numeric']
        )
        
        # 随机决定是否使用触发列
        use_trigger_cols = random.choice([True, False])
        if use_trigger_cols:
        # 从触发表的列中随机选择1-2个作为触发列
            trigger_cols = random.sample(self.trigger_columns['numeric'], 
                                    random.randint(1, len(self.trigger_columns['numeric'])))
            window = f"COUNT_WINDOW({params['count']}, {params['sliding']}, {', '.join(trigger_cols)})"
        else:
            # 不使用触发列
            window = window_type.format(**params)
        
        # 为count window选择合适的聚合函数
        aggs = random.sample(self.agg_functions, random.randint(1, 5))  # count window通常使用1-5个聚合函数
        aggs = [agg.format(col=random.choice(agg_columns)) for agg in aggs]
        
        
        # 根据源表选择合适的时间戳处理函数
        ts_function = self._get_ts_function(source_table)
        
        # 修改select子句,使用随机选择的时间戳处理函数
        select_clause = f"select {ts_function} ts, {', '.join(aggs)}"
        
        # 随机决定是否使用partition by
        partition_by = "partition by tbname" if source_table == 'meters' else ""
        
        # 随机生成WHERE条件
        where_clause = self._get_random_where_condition(source_table, 'count')
        
        # 构建完整的create stream语句
        stream_sql = f"""create stream {database}.s{stream_number} {window} 
            from {database}.stream_trigger {partition_by} 
            into {database}.st{stream_number} 
            as {select_clause} 
            from {source_table}  
            {where_clause};"""
            
        return stream_sql
    
    def generate_period_stream(self, stream_number, database):
        """专门生成period类型的stream
        
        Args:
            stream_number: stream编号
            database: 数据库名
        Returns:
            str: 生成的stream SQL语句
        """
        # 随机选择period window类型
        window_type = random.choice(self.period_windows)
        
        # 定义时间单位选项
        period_units = ['a', 's', 'm', 'h', 'd']
        offset_units = ['a', 's', 'm', 'h']
        
        # 为period window设置参数
        # period范围: 10毫秒到3650天,根据不同单位设置合理范围
        unit = random.choice(period_units)
        if unit == 'a':  # 毫秒
            period = random.randint(10, 1000000)
        elif unit == 's':  # 秒
            period = random.randint(1, 100000)
        elif unit == 'm':  # 分钟
            period = random.randint(1, 10000)
        elif unit == 'h':  # 小时
            period = random.randint(1, 1000)
        else:  # 天
            period = random.randint(1, 3650)
        
        params = {
            "period": period,
            "unit": unit
        }
        
        # 单位转换到毫秒的倍数
        unit_to_ms = {
            'a': 1,           # 毫秒
            's': 1000,        # 秒
            'm': 60 * 1000,   # 分钟
            'h': 3600 * 1000, # 小时
            'd': 86400 * 1000 # 天
        }
        
        # 计算period的毫秒值
        period_ms = period * unit_to_ms[unit]
        
        # 根据窗口类型设置offset参数
        if "offset" in window_type:  # 带offset的形式
            # 选择单位时确保offset可以小于period
            max_unit_idx = list(unit_to_ms.keys()).index(unit)
            valid_offset_units = list(unit_to_ms.keys())[:max_unit_idx + 1]
            offset_unit = random.choice(valid_offset_units)
            
            # 根据offset_unit选择合适的范围
            max_offset = period_ms // unit_to_ms[offset_unit]  # 确保转换为毫秒后不超过period
            if max_offset < 1:
                # 如果转换后太小,调整单位
                smaller_units = valid_offset_units[:valid_offset_units.index(offset_unit)]
                if smaller_units:
                    offset_unit = random.choice(smaller_units)
                    max_offset = period_ms // unit_to_ms[offset_unit]
                else:
                    offset_unit = 'a'
                    max_offset = period_ms
            
            # 生成不超过period的offset值
            if offset_unit == 'a':
                offset = random.randint(1, min(1000, max_offset))
            elif offset_unit == 's':
                offset = random.randint(1, min(60, max_offset))
            elif offset_unit == 'm':
                offset = random.randint(1, min(60, max_offset))
            else:  # 'h'
                offset = random.randint(1, min(24, max_offset))
                
            params.update({
                "offset": offset,
                "offset_unit": offset_unit
            })
            window = f"PERIOD({period}{unit}, {offset}{offset_unit})"
        else:
            # 基本形式,不带offset
            window = f"PERIOD({period}{unit})"
        
        # 格式化窗口类型
        window = window_type.format(**params)
            
        # 随机选择源表
        source_table = random.choice(['meters', 'stream_trigger', '%%trows'])
        
        # 根据源表选择列名
        columns = (
            self.meters_columns['numeric'] if source_table == 'meters' 
            else self.trigger_columns['numeric']
        )
        
        # 为period window选择合适的聚合函数
        aggs = random.sample(self.agg_functions, random.randint(1, 3))
        aggs = [agg.format(col=random.choice(columns)) for agg in aggs]
        
        # 根据源表选择合适的时间戳处理函数
        ts_function = self._get_ts_function(source_table, window_type='period')
        
        # 修改select子句,使用随机选择的时间戳处理函数
        select_clause = f"select {ts_function} ts, {', '.join(aggs)}"
        
        # 随机决定是否使用partition by
        partition_by = "partition by tbname" if source_table == 'meters' else ""
        
        # 随机生成WHERE条件
        where_clause = self._get_random_where_condition(source_table, 'period')
        
        # 构建完整的create stream语句
        stream_sql = f"""create stream {database}.s{stream_number} {window} 
            from {database}.stream_trigger {partition_by} 
            into {database}.st{stream_number} 
            as {select_clause} 
            from {source_table}  
            {where_clause};"""
            
        return stream_sql
    
    def generate_random_stream(self, stream_number, database):
        # """根据不同窗口类型调用对应的生成函数"""
        # # # session窗口的生成
        # # return self.generate_session_stream(stream_number, database)
        # # count窗口的生成
        # return self.generate_count_stream(stream_number, database)
        """根据不同窗口类型调用对应的生成函数
        
        Args:
            stream_number: stream编号
            database: 数据库名
        Returns:
            str: 生成的stream SQL语句
        """
        # 随机选择窗口类型
        stream_type = random.choice([
            'all',      # 混合模式
            # 'session',  # session窗口
            # 'count',    # count窗口
            # 'interval', # 时间间隔窗口 - 待实现
            # 'state',    # 状态窗口 - 待实现
            # 'period'    # 周期窗口 - 待实现
        ])
        
        if stream_type == 'all':
            implemented_types = [
                'session',
                'count',
                # 'interval',
                # 'state',
                'period'  #语法有问题
            ]
            stream_type = random.choice(implemented_types)
        
        # 根据选择的类型调用对应的生成函数
        if stream_type == 'session':
            return self.generate_session_stream(stream_number, database)
        elif stream_type == 'count':
            return self.generate_count_stream(stream_number, database)
        # 为其他类型预留位置
        # elif stream_type == 'interval':
        #     return self.generate_interval_stream(stream_number, database)
        # elif stream_type == 'state':
        #     return self.generate_state_stream(stream_number, database)
        elif stream_type == 'period':
            return self.generate_period_stream(stream_number, database)
        else:
            raise ValueError(f"未支持的窗口类型: {stream_type}")
        
        
    # def generate_random_stream(self, stream_number):
    #     # 随机选择窗口类型
    #     window_type = random.choice(self.window_types)
        
    #     # 随机生成参数
    #     params = {
    #         "interval": random.randint(1, 10),
    #         "sliding": random.randint(1, 5),
    #         "col": random.choice(self.columns),
    #         "duration": random.randint(1, 5),
    #         "count": random.randint(2, 5),
    #         "period": random.randint(5, 20)
    #     }
        
    #     # 格式化窗口类型
    #     window = window_type.format(**params)
        
    #     # 随机选择1-3个聚合函数
    #     aggs = random.sample(self.agg_functions, random.randint(1, 3))
    #     aggs = [agg.format(col=random.choice(self.columns)) for agg in aggs]
        
    #     # 生成select子句
    #     select_clause = f"select _twstart ts, {', '.join(aggs)}"
        
    #     # 随机决定是否使用partition by
    #     partition_by = "partition by tbname" if random.choice([True, False]) else ""
        
    #     # 构建完整的create stream语句
    #     stream_sql = f"""create stream s{stream_number} {window} 
    #         from stream_trigger {partition_by} 
    #         into st{stream_number} 
    #         as {select_clause} 
    #         from meters;"""
            
    #     return stream_sql

class TestStreamTriggerType1:
    def setup_class(cls):
        """测试类初始化"""
        cls.database = "qdb"
        cls.vgroups = 1
        # 添加默认的stream数量参数
        cls.stream_count = 20
        
        tdLog.debug(f"start to execute {__file__}")
        
    @staticmethod
    def generate_test_data(database, start_time='2025-01-01 00:00:00', rows=1000, batch_size=100):
        """生成测试数据
        
        Args:
            database: 数据库名
            start_time: 起始时间
            rows: 总行数
            batch_size: 每个批次的行数
        Returns:
            list: SQL语句列表
        """
        test_data_sqls = []
        timestamp = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        base_ts = time.mktime(timestamp)
        current_ts = base_ts
        
        for i in range(rows):
            ts = current_ts + random.uniform(0.001, 1000)
            # 计算毫秒部分
            ms = int((ts % 1) * 1000)
            t1_sql = f"insert into {database}.t1 using {database}.meters tags(1) values ('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}.{ms:03}', {i % 100}, {i % 50});"
            
            # t2的时间戳比t1随机晚
            ts2 = ts + random.uniform(0.001, 1000)
            ms2 = int((ts2 % 1) * 1000)
            t2_sql = f"insert into {database}.t2 using {database}.meters tags(2) values ('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts2))}.{ms2:03}', {(i + 1) % 100}, {(i + 1) % 50});"
            
            # 生成stream_trigger表数据
            # 在两个meters表的时间戳之间随机插入2-10条trigger数据
            trigger_count = random.randint(2, 10)
            for j in range(trigger_count):
                trigger_ts = ts + (ts2 - ts) * random.random()  # 在ts和ts2之间随机取时间
                trigger_ms = int((trigger_ts % 1) * 1000)
                
                # 生成随机的trigger值,保持一定相关性
                c1_val = random.randint(max(0, i % 100 - 10), min(100, i % 100 + 10))  # 在当前i的基础上随机偏移
                c2_val = random.randint(max(0, i % 50 - 5), min(50, i % 50 + 5))      # 在当前i的基础上随机偏移
                
                trigger_sql = f"insert into {database}.stream_trigger values ('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(trigger_ts))}.{trigger_ms:03}', {c1_val}, {c2_val});"
                test_data_sqls.append(trigger_sql)
            
            test_data_sqls.extend([t1_sql, t2_sql])
            current_ts = ts2  # 使用t2的时间作为下一轮的起始时间
        
        # 按批次分组
        return [test_data_sqls[i:i + batch_size] for i in range(0, len(test_data_sqls), batch_size)]

    
    def test_stream_trigger_type1(self):
        """
        测试stream trigger类型1
        可以通过环境变量STREAM_COUNT设置要生成的stream数量
        """
        # 从环境变量获取stream数量,如果没有设置则使用默认值
        # eg:STREAM_COUNT=10 pytest --clean cases/13-StreamProcessing/99-Others/random_stream.py  --skip_stop 
        import os
        stream_count = int(os.getenv('STREAM_COUNT', self.stream_count))
        tdLog.debug(f"使用的stream数量: {stream_count}")
        
        # 打开文件用于写入SQL
        with open('all.sql', 'w') as f, open('success.sql', 'w') as success_f, open('error.sql', 'w') as error_f:
            # 初始化环境
            tdStream.dropAllStreamsAndDbs()
            tdStream.createSnode()
            
            # 创建数据库和表
            create_db_sql = f"create database {self.database} vgroups {self.vgroups};"
            create_stable_sql = f"create stable {self.database}.meters (cts timestamp, cint int, cuint int unsigned) tags(tint int);"
            create_trigger_sql = f"create table {self.database}.stream_trigger (ts timestamp, c1 int, c2 int);"
            
            # 写入建表SQL
            f.write("-- 创建数据库和表\n")
            f.write(f"{create_db_sql}\n")
            f.write(f"{create_stable_sql}\n")
            f.write(f"{create_trigger_sql}\n\n")
            success_f.write("-- 初始化环境: 创建数据库和表\n")
            success_f.write(f"{create_db_sql}\n")
            success_f.write(f"{create_stable_sql}\n")
            success_f.write(f"{create_trigger_sql}\n\n")
            
            # 写入测试数据
            f.write("-- 插入测试数据\n")
            test_data_sqls = [
                f"insert into {self.database}.t1 using {self.database}.meters tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
                f"insert into {self.database}.t2 using {self.database}.meters tags(2) values ('2025-01-01 00:00:00.102', 1, 0);",
                f"insert into {self.database}.t1 using {self.database}.meters tags(1) values ('2025-01-01 00:00:01'    , 1, 1);", 
                f"insert into {self.database}.t2 using {self.database}.meters tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
                f"insert into {self.database}.t1 using {self.database}.meters tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
                f"insert into {self.database}.t2 using {self.database}.meters tags(2) values ('2025-01-01 00:00:02.600', 3, 2);"
            ]
            # 写入测试数据到both all.sql和success.sql
            f.write("-- 插入测试数据\n")
            success_f.write("-- 插入测试数据\n")
            for sql in test_data_sqls:
                f.write(f"{sql}\n")
                success_f.write(f"{sql}\n")
            f.write("\n")
            success_f.write("\n")
            
                       
            # 写入查询语句到both all.sql和success.sql
            query_sql = f"select _wstart, avg(cint) from {self.database}.meters interval(1s);"
            f.write("-- 查询数据\n")
            f.write(f"{query_sql}\n\n")
            success_f.write("-- 查询数据\n")
            success_f.write(f"{query_sql}\n\n")
            
            # 执行SQL
            tdSql.prepare(dbname=self.database, vgroups=self.vgroups)
            tdSql.execute(create_stable_sql)
            tdSql.execute(create_trigger_sql)
            tdSql.executes(test_data_sqls)
            
            # 生成更多测试数据
            batch_sqls = self.generate_test_data(self.database, rows=1000, batch_size=100)
            f.write("-- 插入测试数据\n")
            success_f.write("-- 插入测试数据\n")
            
            # 执行并记录SQL
            from concurrent.futures import ThreadPoolExecutor
            
            def execute_batch(batch):
                try:
                    tdSql.executes(batch)
                    # 写入文件
                    for sql in batch:
                        f.write(f"{sql}\n")
                        success_f.write(f"{sql}\n")
                    return True
                except Exception as e:
                    print(f"批量插入失败: {str(e)}")
                    return False
            
            print("开始写入测试数据...")
            batch_sqls = self.generate_test_data(self.database, rows=1000, batch_size=100)
        
            success_batches = 0
            total_batches = len(batch_sqls)
            
            for batch_id, batch in enumerate(batch_sqls, 1):
                try:
                    # 每个批次写入前暂停一小段时间
                    time.sleep(0.1)  
                    
                    # 执行当前批次
                    tdSql.executes(batch)
                    
                    # 写入文件
                    for sql in batch:
                        f.write(f"{sql}\n")
                        success_f.write(f"{sql}\n")
                        
                    success_batches += 1
                    print(f"完成批次 {batch_id}/{total_batches}")
                    
                except Exception as e:
                    print(f"批次 {batch_id} 写入失败: {str(e)}")
                    # 写入错误日志但继续执行
                    error_f.write(f"-- Batch {batch_id} 写入失败\n")
                    error_f.write(f"-- 错误信息: {str(e)}\n")
                    for sql in batch:
                        error_f.write(f"{sql}\n")
                    error_f.write("\n")
                    
            print(f"数据写入完成: 成功 {success_batches} 批, 失败 {total_batches - success_batches} 批")
            
            f.write("\n")
            success_f.write("\n")
            
            # 使用StreamGenerator生成随机stream
            generator = StreamGenerator()
            streams = []
            success_count = 0
            error_count = 0
            
            # 写入stream SQL
            f.write("-- 创建streams\n")
            print(f"\n开始生成 {stream_count} 个随机stream:")
            for i in range(stream_count):  
                stream_sql = generator.generate_random_stream(i+1,self.database)
                streams.append(self.StreamItem(stream_sql, lambda: None))
                # 写入文件
                f.write(f"-- Stream {i+1}\n")
                f.write(f"{stream_sql}\n\n")
                # 尝试执行stream并记录结果
                try:
                    tdSql.execute(stream_sql)
                    # 执行成功,写入success.sql
                    success_f.write(f"-- Stream {success_count + 1} 执行成功\n")
                    success_f.write(f"{stream_sql}\n\n")
                    success_count += 1
                    print(f"Successfully created stream: {stream_sql}")
                except Exception as e:
                    # 执行失败,写入error.sql
                    error_f.write(f"-- Stream {error_count + 1} 执行失败\n")
                    error_f.write(f"-- 错误信息: {str(e)}\n")
                    error_f.write(f"{stream_sql}\n\n")
                    error_count += 1
                    print(f"Failed to create stream: {stream_sql}")
                    print(f"Error: {str(e)}")
                    
            # 打印执行统计信息
            print(f"\n执行统计:")
            print(f"总共: {len(streams)} 个streams")
            print(f"成功: {success_count} 个")
            print(f"失败: {error_count} 个")
                    
            # 检查stream状态和结果
            if success_count > 0:
                tdStream.checkStreamStatus()
                print("\n开始检查stream结果:")
                
                # 存储所有成功创建的stream编号
                all_streams = list(range(1, success_count + 1))
                
                # 首次检查所有stream的结果
                stream_results = {}  # 用字典存储每个stream的结果数量
                valid_streams = []  # 存储有效的(表存在的)stream编号
                
                print("\n首次检查所有streams:")
                for stream_num in all_streams:
                    try:
                        query_sql = f"select count(*) from {self.database}.st{stream_num}"
                        tdSql.query(query_sql)
                        count = tdSql.getData(0, 0)
                        stream_results[stream_num] = count
                        valid_streams.append(stream_num)  # 记录有效的stream
                        print(f"Stream st{stream_num} 首次检查结果数量: {count}")
                    except Exception as e:
                        print(f"检查st{stream_num}失败(可能表不存在): {str(e)}")
                        stream_results[stream_num] = -1  # 用-1标记错误
                
                if valid_streams:  # 只有存在有效的stream才进行重试
                    print(f"\n发现 {len(valid_streams)} 个有效stream,开始重试检查:")
                    for attempt in range(5):  # 5次重试
                        print(f"\n第{attempt + 1}次重试:")
                        time.sleep(1)  # 等待1秒
                        
                        # 只检查有效的stream
                        for stream_num in valid_streams:
                            try:
                                query_sql = f"select count(*) from {self.database}.st{stream_num}"
                                tdSql.query(query_sql)
                                count = tdSql.getData(0, 0)
                                # 记录新的结果
                                stream_results[stream_num] = count
                                print(f"Stream st{stream_num} 结果数量: {count}")
                            except Exception as e:
                                print(f"检查st{stream_num}失败: {str(e)}")
                                stream_results[stream_num] = -1
                
                # 输出最终统计结果
                print("\n最终检查结果统计:")
                print("=" * 50)
                print("有数据的streams:")
                has_data = [num for num in valid_streams if stream_results[num] > 0]
                for num in has_data:
                    print(f"st{num}: {stream_results[num]}条数据")
                
                print("\n无数据的streams:")
                no_data = [num for num in valid_streams if stream_results[num] == 0]
                for num in no_data:
                    print(f"st{num}: 0条数据")
                
                print("\n表不存在的streams:")
                not_exists = set(all_streams) - set(valid_streams)
                for num in not_exists:
                    print(f"st{num}: 表不存在")
                
                print("\n检查失败的streams:")
                failed = [num for num in valid_streams if stream_results[num] == -1]
                for num in failed:
                    print(f"st{num}: 检查失败")
                
                print("\n统计信息:")
                print(f"总共streams: {len(all_streams)}个")
                print(f"表存在: {len(valid_streams)}个")
                print(f"表不存在: {len(not_exists)}个")
                print(f"有数据: {len(has_data)}个")
                print(f"无数据: {len(no_data)}个")
                print(f"检查失败: {len(failed)}个")
                print("=" * 50)

        
    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()