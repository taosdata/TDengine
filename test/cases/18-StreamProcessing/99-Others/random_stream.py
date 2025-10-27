import time
import math
import random
import re
import subprocess
import datetime 
from new_test_framework.utils import tdLog, tdSql, tdStream

class StreamGenerator:
    def __init__(self, tdSql):
        
        self.tdSql = tdSql
        # 定义窗口类型选项
        self.interval_windows = [
            "interval({interval}s) sliding({sliding}s)"
        ]
        
        
        self.count_windows = [
            "COUNT_WINDOW({count})",
            "COUNT_WINDOW({count}, {sliding})"
        ]
        
        self.period_windows = [
            "PERIOD({period}{unit})",  # 基本形式
            "PERIOD({period}{unit}, {offset}{offset_unit})"  # 带偏移的形式
        ]
        
        # 添加系统表配置
        self.system_tables = [
            'performance_schema.perf_connections',
            'performance_schema.perf_apps',
            'performance_schema.perf_queries',
            'performance_schema.perf_consumers',
            'performance_schema.perf_trans',
            'information_schema.ins_dnodes',
            'information_schema.ins_mnodes',
            'information_schema.ins_qnodes',
            'information_schema.ins_snodes',
            'information_schema.ins_cluster',
            'information_schema.ins_databases',
            'information_schema.ins_functions',
            'information_schema.ins_indexes',
            'information_schema.ins_stables',
            'information_schema.ins_tables',
            'information_schema.ins_tags',
            'information_schema.ins_columns',
            'information_schema.ins_virtual_child_columns',
            'information_schema.ins_users',
            'information_schema.ins_grants',
            'information_schema.ins_vgroups',
            'information_schema.ins_configs',
            'information_schema.ins_dnode_variables',
            'information_schema.ins_topics',
            'information_schema.ins_subscriptions',
            'information_schema.ins_streams',
            'information_schema.ins_stream_tasks',
            'information_schema.ins_vnodes',
            'information_schema.ins_user_privileges',
            'information_schema.ins_views',
            'information_schema.ins_compacts',
            'information_schema.ins_compact_details',
            'information_schema.ins_grants_full',
            'information_schema.ins_grants_logs',
            'information_schema.ins_machines',
            'information_schema.ins_arbgroups',
            'information_schema.ins_encryptions',
            'information_schema.ins_tsmas',
            'information_schema.ins_anodes',
            'information_schema.ins_anodes_full',
            'information_schema.ins_disk_usage',
            'information_schema.ins_filesets',
            'information_schema.ins_transaction_details',
        ]
        
        # 添加流选项配置
        self.stream_options = [
            "WATERMARK({duration})",           # duration: 1s, 1m, 1h 等
            "EXPIRED_TIME({exp_time})",        # exp_time: 1s, 1m, 1h 等
            "IGNORE_DISORDER",
            "DELETE_RECALC",
            "DELETE_OUTPUT_TABLE",
            "FILL_HISTORY",                    # 可选带时间参数
            "FILL_HISTORY_FIRST",              # 可选带时间参数
            "CALC_NOTIFY_ONLY",
            "LOW_LATENCY_CALC",
            "PRE_FILTER({expr})",              # expr: 过滤表达式
            "FORCE_OUTPUT",
            "MAX_DELAY({delay})",              # delay: 延迟时间
            "EVENT_TYPE({event_type})"         # event_type: WINDOW_OPEN|WINDOW_CLOSE
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
        },
        'test.meters': {    # 添加test.meters表的配置
            'col': 'ts',    # test.meters表的时间戳列名
            'common': [     # 通用函数
                "last({col})",
                "first({col})", 
                "last_row({col})"
            ],
            'window': [     # 窗口函数
                "_twstart",
                "_twend"
            ],
            'period': [     # period窗口函数
                "cast(_tlocaltime/1000000 as timestamp)",
                "last({col})",
                "first({col})"
            ]
        }
    }
        
        # 定义可用的聚合函数
        self.agg_functions = [
            "count(*)",
            "count({col}) as count_val",
            "avg({col}) as avg_val",
            "sum({col}) as sum_val",
            "max({col}) as max_val",
            "min({col}) as min_val",
            "mode({col}) as mode_val",
            "irate({col}) as irate_val",
            "sum({col}) as sum_val",
            "first({col}) as first_val",
            "last({col}) as last_val",
            "last_row({col}) as last_row_val",
            "stddev({col}) as std_val",
            "stddev_pop({col}) as std_pop_val",
            "var_pop({col}) as var_pop_val",
            "apercentile({col},95) as aper_val",
            "spread({col}) as spread_val"
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
            
        # 虚拟表的列定义
        self.virtual_columns = {
            'numeric': [
                't1_cint', 't1_cuint',  # t1表引用的列
                't2_cint', 't2_cuint',  # t2表引用的列
                'stream_trigger_c1',     # stream_trigger表引用的列
                'stream_trigger_c2'
            ],
            'timestamp': ['ts']          # 时间戳列
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
        elif source_table == 'v1' or source_table == 'sv1' or source_table == 'sv1_v1' or source_table == 'sv1_v2' : #virtable
            # 根据窗口类型选择可用的WHERE条件
            if window_type in ['session', 'count']:  # 事件窗口可以使用 _twstart/_twend
                conditions = [
                    "",  # 不使用WHERE条件
                    "where ts >= _twstart and ts < _twend",  
                    f"where {random.choice(self.virtual_columns['numeric'])} > 0",
                ]
            else:  # period窗口等其他类型
                conditions = [
                    "",  # 不使用WHERE条件
                    f"where {random.choice(self.virtual_columns['numeric'])} > 0",
                    f"where {random.choice(self.virtual_columns['numeric'])} between 0 and 50",
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
        # 根据表名确定使用哪个配置
        if source_table == 'test.meters':  #自定义的库
            table_type = 'test.meters'
        elif source_table == 'meters':    #默认的库和表
            table_type = 'meters'
        else:
            table_type = 'stream_trigger'
        
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
    
    def _parse_system_table_columns(self, table_name):
        """
        解析系统表的列结构
        
        参数:
            table_name: 完整的系统表名 (eg: performance_schema.perf_connections)
            
        返回:
            dict: 包含所有列和按类型分类的列信息的字典
            {
                'all': ['col1', 'col2', ...],           # 所有列
                'numeric': ['col1', 'col3', ...],       # 数值类型列
                'timestamp': ['ts', 'create_time', ...], # 时间戳类型列
                'bool': ['is_valid', ...],              # 布尔类型列
                'string': ['name', 'status', ...]       # 字符串类型列
            }
        """
        try:
            # 执行desc命令获取表结构
            #print(f"=============================={table_name}")
            self.tdSql.query(f"desc {table_name}")
            desc_results = self.tdSql.queryResult
            #print(desc_results)
            # 定义最大安全长度（50000字节）
            MAX_SAFE_LENGTH = 50000
            
            # 按类型分类列名
            columns = {
                #'all': [],
                'numeric': [],      # 数值类型列
                'string': [],       # 字符串类型列
                'bool': [],         # bool类型列
                'timestamp': []     # 时间戳类型列
            }
            
            for row in desc_results:
                col_name = str(row[0]).strip() # 列名
                col_type = str(row[1]).strip().upper()  # 类型
                col_length = int(row[2])    # 长度
                
                #print(f"列: {col_name}, 类型: {col_type}, 长度: {col_length}")
                
                # 跳过无效的列名（纯数字或空）
                if not col_name :
                    print(f"警告: 跳过无效的列名: {col_name}")
                    continue
                
                # 跳过长度超过限制的VARCHAR列
                if ('VARCHAR' in col_type or 'NCHAR' in col_type) and col_length > MAX_SAFE_LENGTH:
                    print(f"警告: 列 {col_name} 长度({col_length})超过限制({MAX_SAFE_LENGTH})，已跳过")
                    continue
                
                # 根据类型分类
                if any(t in col_type for t in ['INT', 'INT UNSIGNED', 'BIGINT', 'BIGINT UNSIGNED', 'SMALLINT', 'SMALLINT UNSIGNED', 'TINYINT', 'TINYINT UNSIGNED', 'FLOAT', 'DOUBLE']):
                    columns['numeric'].append(col_name)
                elif col_type == 'TIMESTAMP':
                    columns['timestamp'].append(col_name)
                elif col_type == 'BOOL':
                    columns['bool'].append(col_name)
                else:  
                    columns['string'].append(col_name)
                    
            return columns
            
        except Exception as e:
            print(f"解析系统表结构时出错: {str(e)}")
            return columns
        
    def _generate_random_timestamp(self):
        """生成随机的时间戳，范围是当前时间前一年内
        
        Returns:
            str: 格式化的时间戳字符串，如 '2025-01-01 00:00:00'
        """
        # 计算时间范围
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=365)
        
        # 生成随机时间
        time_delta = end_time - start_time
        days = time_delta.days
        random_days = random.randint(0, days)
        random_seconds = random.randint(0, 24*60*60)
        
        random_time = start_time + datetime.timedelta(days=random_days, seconds=random_seconds)
        
        # 格式化时间戳
        return random_time.strftime("'%Y-%m-%d %H:%M:%S'")

    def _get_trigger_table_columns(self, trigger_table):
        """获取触发表(第一个FROM子句)的列信息
        
        Args:
            trigger_table: 触发表名称('meters', 'stream_trigger', 'v1'或其他自定义表)
            
        Returns:
            dict: 按类型分类的列字典
        """
        if trigger_table == 'meters':
            return {
                'numeric': self.meters_columns['numeric'],
                'string': self.meters_columns.get('string', []),
                'timestamp': ['cts']
            }
        elif trigger_table == 'stream_trigger':
            return {
                'numeric': self.trigger_columns['numeric'],
                'string': self.trigger_columns.get('string', []),
                'timestamp': ['ts']
            }
        elif trigger_table == 'v1' or trigger_table == 'sv1' or trigger_table == 'sv1_v1' or trigger_table == 'sv1_v2':
            return {
                'numeric': self.virtual_columns['numeric'],
                'string': self.virtual_columns.get('string', []),
                'timestamp': ['ts']
            }
        else:
            # 如果是自定义表，解析其结构
            return self._parse_system_table_columns(trigger_table)
        
    def _get_source_table_columns(self, source_table):
        """获取数据源表(第二个FROM子句)的列信息
        
        Args:
            source_table: 源表名称(可以是系统表或其他表)
            
        Returns:
            dict: 按类型分类的列字典
        """
        if source_table.startswith(('information_schema.', 'performance_schema.')):
            return self._parse_system_table_columns(source_table)
        else:
            return self._get_trigger_table_columns(source_table)
    
    def _generate_stream_option(self, trigger_table, window_type='window'):
        """生成随机的流选项
        
        Returns:
            str: 生成的流选项，如果不生成则返回空字符串
        """
        # 20%的概率不使用流选项
        if random.random() < 0.2:
            return ""
        
        # 根据窗口类型筛选可用的选项
        if window_type == 'period':
            # PERIOD 窗口不支持 FILL_HISTORY 和 FILL_HISTORY_FIRST
            available_options = [
                "WATERMARK({duration})",           
                "EXPIRED_TIME({exp_time})",        
                "IGNORE_DISORDER",
                "DELETE_RECALC",
                "DELETE_OUTPUT_TABLE",
                "CALC_NOTIFY_ONLY",
                "LOW_LATENCY_CALC",
                "PRE_FILTER({expr})",              
                "FORCE_OUTPUT",
                "MAX_DELAY({delay})",              
                "EVENT_TYPE({event_type})"         
            ]
        else:
            # 其他窗口类型支持所有选项
            available_options = self.stream_options
            
        # 随机选择一个选项
        option = random.choice(available_options)
            
        # 随机选择一个选项
        option = random.choice(self.stream_options)
        
        # 根据不同选项类型处理参数
        if '{duration}' in option:
            # 生成随机时间duration: 1-60s, 1-60m, 1-24h, 1-7d
            value = random.randint(1, 60)
            unit = random.choice(['s', 'm', 'h', 'd'])
            if unit == 'h':
                value = random.randint(1, 24)
            elif unit == 'd':
                value = random.randint(1, 7)
            option_value = option.format(duration=f"{value}{unit}")
            
        elif '{exp_time}' in option:
            # 生成过期时间
            value = random.randint(1, 60)
            unit = random.choice(['s', 'm', 'h', 'd'])
            option_value = option.format(exp_time=f"{value}{unit}")
            
        elif '{delay}' in option:
            # 生成延迟时间
            value = random.randint(1, 30)
            unit = random.choice(['s', 'm'])
            option_value = option.format(delay=f"{value}{unit}")
            
        elif '{expr}' in option:
            # 生成简单的过滤表达式
            # 获取表的可用列
            columns_dict = self._get_trigger_table_columns(trigger_table)
            
            # 优先使用数值列进行过滤
            available_cols = columns_dict.get('numeric', [])
            if not available_cols:
                # 如果没有数值列，使用字符串列
                available_cols = columns_dict.get('string', [])
                
            if available_cols:
                col = random.choice(available_cols)
                
                # 根据列类型选择合适的操作符和值
                if col in columns_dict.get('numeric', []):
                    op = random.choice(['>', '<', '=', '>=', '<=', '<>'])
                    val = random.randint(1, 1000)
                else:
                    op = random.choice(['=', '<>'])
                    val = f"'{random.choice(['a', 'b', 'c', 'd', 'e'])}'"
                    
                col_name = f"`{col}`" if not col.startswith('`') else col
                option_value = option.format(expr=f"{col_name} {op} {val}")
            
            else:
                # 如果没有可用的列，返回空字符串
                return ""
            
        elif option == "EVENT_TYPE({event_type})":
            # 随机选择事件类型
            event_type = random.choice(['WINDOW_OPEN', 'WINDOW_CLOSE'])
            option_value = f"event_type({event_type})"
            
        elif option in ["FILL_HISTORY", "FILL_HISTORY_FIRST"]:
            # 50%的概率添加起始时间
            if random.random() < 0.5:
                start_time = self._generate_random_timestamp()
                option_value = f"{option}({start_time})"
            else:
                option_value = option
                
        else:
            option_value = option
        
        # 返回格式化的STREAM_OPTIONS子句
        return f"STREAM_OPTIONS({option_value})"

        
    def _build_stream_sql(self, stream_number, database, window, window_type='period', trigger_table=None):
        """
        构建流SQL的公共方法
        
        参数:
            stream_number: 流编号
            database: 数据库名
            window: 窗口子句
            window_type: 窗口类型(period/interval/state/event/count/session)
        """
        def add_unique_alias(agg_list):
            """为重复的聚合函数添加唯一别名"""
            seen = {}  # 记录已经出现的聚合函数
            result = []
            
            def clean_alias(agg_expr):
                """清理并规范化别名"""
                try:
                    # 处理特殊情况
                    if agg_expr == 'count(*)':
                        return 'count_all'
                        
                    # 解析常规聚合函数
                    if '(' not in agg_expr or ')' not in agg_expr:
                        return agg_expr.lower().replace(' ', '_')
                        
                    # 提取函数名和列名
                    func_name = agg_expr[:agg_expr.index('(')].lower()
                    col_name = agg_expr[agg_expr.index('(')+1:agg_expr.rindex(')')].strip()
                    
                    # 处理特殊字符
                    col_name = col_name.replace('*', 'all')
                    col_name = col_name.replace('.', '_')
                    
                    return f"{func_name}_{col_name}"
                    
                except Exception as e:
                    print(f"警告: 处理别名时出错 '{agg_expr}': {str(e)}")
                    # 返回一个安全的默认别名
                    return f"agg_{len(seen)}"
                
            #print(f"\n开始处理聚合函数列表: {agg_list}")
            
            for agg in agg_list:
                #print(f"\n处理表达式: {agg}")
                # 如果已有as子句，先移除
                base_expr = agg.split(' as ')[0].strip()
                #print(f"基础表达式: {base_expr}")
                
                # 生成基础别名
                try:
                    # 处理count(*)的特殊情况
                    if 'count(*)' in base_expr.lower():
                        base_alias = 'count_total_'
                    else:
                        # 处理带反引号的列名
                        if base_expr.startswith('`') and base_expr.endswith('`'):
                            # 直接使用列名作为别名，去掉反引号
                            col_name = base_expr.strip('`')
                            base_alias = col_name
                            #print(f"处理反引号列名: {col_name}")
                        else:
                            # 提取函数名和列名
                            left_paren = base_expr.find('(')
                            right_paren = base_expr.rfind(')')
                        
                            if left_paren != -1 and right_paren != -1:
                                func_name = base_expr[:left_paren].strip().lower()
                                params = base_expr[left_paren+1:right_paren].replace('`', '').strip()
                                
                                # 处理带参数的函数，如 apercentile(c1,95)
                                if ',' in params:
                                    parts = [p.strip() for p in params.split(',')]
                                    col_name = parts[0]
                                    extra_params = '_'.join(parts[1:])                                        
                                    base_alias = f"{func_name}_{col_name}_{extra_params}"
                                else:
                                    col_name = params
                                
                                base_alias = f"{func_name}_{col_name}"
                                #print(f"提取到 - 函数名: {func_name}, 原始列名: {col_name}")
                            else:
                                base_alias = base_expr.lower().replace('`', '').replace(' ', '_')
                                #print(f"无法解析函数和列名, 使用基础别名: {base_alias}")
                    
                    # 为基础别名添加_val后缀
                    alias = f"{base_alias}_val"
                    #print(f"构造的完整别名: {alias}")
                    
                    # 如果别名已存在，添加数字后缀
                    if alias in seen:
                        seen[alias] += 1
                        final_alias = f"{alias}_{seen[alias]}"
                        #print(f"别名已存在, 添加后缀: {final_alias}")
                    else:
                        seen[alias] = 1
                        final_alias = alias
                        #print(f"使用新别名: {final_alias}")   
                        
                    
                    # 添加到结果列表，保持原始表达式不变
                    result.append(f"{base_expr} as {final_alias}")                    
                    
                except Exception as e:
                    print(f"警告: 生成别名时出错 '{base_expr}': {str(e)}")
                    # 生成一个安全的默认别名
                    final_alias = f"col_{len(result)}"
                    result.append(f"{base_expr} as {final_alias}")
    
            # 在这里处理实际的列替换
            final_result = []
            for expr in result:
                if '{col}' in expr:
                    # 从原始SQL中获取实际列名
                    actual_col = 'c1'  # 这里需要传入或获取实际的列名
                    expr = expr.replace('{col}', actual_col)
                    #print(f"替换后的表达式: {expr}")
                final_result.append(expr)
            
            #print(f"\n最终生成的表达式列表: {final_result}\n")       
            return result
        
        basic_tables = ['meters', 'stream_trigger', '%%trows', 'v1' , 'sv1', 'sv1_v1', 'sv1_v2' ,'test.meters']
        selected_system_tables = random.sample(self.system_tables, random.randint(2, 3))
        available_tables = basic_tables + selected_system_tables 
        source_table = random.choice(available_tables)
        
        select_items = []
    
        def build_system_table_query():
            """处理系统表查询"""
            # 解析系统表结构
            columns_dict = self._parse_system_table_columns(source_table)
            if not columns_dict:
                raise Exception(f"无法解析系统表 {source_table} 的结构")
                
            # 获取时间戳列
            ts_columns = columns_dict.get('timestamp', [])
            ts_function = f"`{random.choice(ts_columns)}`" if ts_columns else 'now'
            #print(f"选择的时间戳: {ts_function}")
            
            # use_aggregation = random.choice([True, False])
            # # 系统表的聚合函数列表 - 只包含 count
            # system_aggs = [
            #     "count({col}) as {col}_count",
            #     "count(*) as count_all"
            # ]
            
            # 获取可用的数值列和字符串列
            numeric_cols = columns_dict.get('numeric', [])
            string_cols = columns_dict.get('string', [])
            bool_cols = columns_dict.get('bool', [])
            timestamp_cols = columns_dict.get('timestamp', [])
            all_cols = numeric_cols + string_cols + bool_cols + timestamp_cols
            
            select_items = []
            # if use_aggregation and all_cols:
            #     num_agg_cols = min(len(all_cols), random.randint(1, 3))
            #     for col in random.sample(all_cols, num_agg_cols):
            #         agg_func = random.choice(system_aggs)
            #         agg_expr = agg_func.format(col=f"`{col}`")
            #         select_items.append(agg_expr)
            #         print(f"添加聚合表达式: {agg_expr}")

            # else:            
            # 不使用聚合函数，直接选择2-4个列
            all_cols = numeric_cols + string_cols + bool_cols + timestamp_cols
            if not all_cols:
                raise Exception(f"表 {source_table} 没有可用的列")
            
            num_cols = min(len(all_cols), random.randint(2, 4))
            for i, col in enumerate(random.sample(all_cols, num_cols)):
                select_items.append(f"`{col}` as column{i}")
            
            return ts_function, select_items
        
        def build_random_table_query():
            """处理任意数据表查询"""
            # todo
            # 解析系统表结构
            columns_dict = self._parse_system_table_columns(source_table)
            if not columns_dict:
                raise Exception(f"无法解析数据表 {source_table} 的结构")
                
            # 获取时间戳列
            ts_function = self._get_ts_function(source_table, window_type)
            print(f"选择的时间戳: {ts_function}")
            
            use_aggregation = random.choice([True, False])
            
            # 获取可用的列
            numeric_cols = columns_dict.get('numeric', [])
            string_cols = columns_dict.get('string', [])
            bool_cols = columns_dict.get('bool', [])
            timestamp_cols = columns_dict.get('timestamp', [])
            all_cols = numeric_cols + string_cols + bool_cols + timestamp_cols
            
            select_items = []
            if use_aggregation and numeric_cols:
                # 选择1-2个数值列应用聚合函数
                num_agg_cols = min(len(numeric_cols), random.randint(1, 5))
                for col in random.sample(numeric_cols, num_agg_cols):
                    agg_func = random.choice(self.agg_functions)
                    agg_expr = agg_func.format(col=f"`{col}`")
                    select_items.append(agg_expr)
                    print(f"添加聚合表达式: {agg_expr}")

            else:
                # 不使用聚合函数，直接选择2-4个列
                all_cols = numeric_cols + string_cols + bool_cols + timestamp_cols
                if not all_cols:
                    raise Exception(f"表 {source_table} 没有可用的列")
                
                num_cols = min(len(all_cols), random.randint(2, 4))
                for i, col in enumerate(random.sample(all_cols, num_cols)):
                    select_items.append(f"`{col}` as column{i}")
            
            return ts_function, select_items
        
        def build_normal_table_query():
            """处理特定表查询"""
            if source_table == 'meters':
                columns = self.meters_columns['numeric']
            elif source_table == 'stream_trigger' or source_table == '%%trows':
                columns = self.trigger_columns['numeric']
            else:  # v1虚拟表
                columns = self.virtual_columns['numeric']
                
            # 获取时间戳处理函数
            ts_function = self._get_ts_function(source_table, window_type)
            
            # 随机决定是否使用聚合函数
            use_aggregation = random.choice([True, False])
            
            select_items = []
            if use_aggregation:
                # 选择1-3个聚合函数
                aggs = random.sample(self.agg_functions, random.randint(1, 3))
                select_items = [agg.format(col=random.choice(columns)) for agg in aggs]
                aggs = add_unique_alias(aggs)
            else:
                # 选择2-10个普通列
                num_cols = min(len(columns), random.randint(2, 10))
                selected_cols = random.sample(columns, num_cols)
                select_items = [f"{col} as column{i}" for i, col in enumerate(selected_cols)]
                
            return ts_function, select_items
        
        
        # 随机选择触发表
        trigger_tables = ['meters', 'stream_trigger', 'v1' , 'sv1', 'sv1_v1', 'sv1_v2']  # 可以扩展添加其他自定义表 todo eg:, 'test.meters'
        if trigger_table is None:
            trigger_table = random.choice(trigger_tables)
        
        # 生成流选项(使用触发表的列)
        stream_option = self._generate_stream_option(trigger_table)
        stream_option = f" {stream_option} " if stream_option else " "
        
        # 随机选择源表(可以是系统表或其他表)
        source_tables = (
            trigger_tables +  # 基本表
            ['information_schema.ins_tables', 'performance_schema.perf_apps']  # 系统表
        )
        source_table = random.choice(source_tables)
        
        # 处理partition by（只对meters表和虚拟超级表使用）
        partition_by = "partition by tbname" if (trigger_table == 'meters' or trigger_table == 'sv1') else ""
        
        
        # 根据表类型选择处理方式
        if source_table.startswith(('performance_schema.', 'information_schema.')):
            ts_function, select_items = build_system_table_query()
            where_clause = "" # TODO
        elif source_table == 'test.meters':
            ts_function, select_items = build_random_table_query()
            where_clause = "" # TODO
        else:
            ts_function, select_items = build_normal_table_query()
            where_clause = self._get_random_where_condition(source_table, window_type)
        
        # 确保至少有一个列被选择
        if not select_items:
            raise Exception(f"无法为表 {source_table} 生成有效的选择列")
        
        select_items_with_alias = add_unique_alias(select_items)
        
        # 拼接时间戳列和其他列
        select_clause = f"select {ts_function} ts"
        if select_items_with_alias:
            # 将所有表达式用逗号连接
            select_clause += ", " + ", ".join(select_items_with_alias)
        #print(f"最终的select子句: {select_clause}")
        
        # 构建完整的create stream语句
        stream_sql = f"""create stream {database}.s{stream_number} {window} 
            from {database}.{trigger_table} {partition_by} {stream_option}
            into {database}.st{stream_number} 
            as {select_clause} 
            from {source_table} 
            {where_clause};"""
            
        return stream_sql
        
        
        
    def _build_stream_sql_bak(self, stream_number, database, window, window_type='period'):
        """
        构建流SQL的公共方法
        
        参数:
            stream_number: 流编号
            database: 数据库名
            window: 窗口子句
            window_type: 窗口类型(period/interval/state/event/count/session)
        """
        def add_unique_alias(agg_list):
            """为重复的聚合函数添加唯一别名"""
            seen = {}  # 记录已经出现的聚合函数
            result = []
            
            def clean_alias(agg_expr):
                """清理并规范化别名"""
                try:
                    # 处理特殊情况
                    if agg_expr == 'count(*)':
                        return 'count_all'
                        
                    # 解析常规聚合函数
                    if '(' not in agg_expr or ')' not in agg_expr:
                        return agg_expr.lower().replace(' ', '_')
                        
                    # 提取函数名和列名
                    func_name = agg_expr[:agg_expr.index('(')].lower()
                    col_name = agg_expr[agg_expr.index('(')+1:agg_expr.rindex(')')].strip()
                    
                    # 处理特殊字符
                    col_name = col_name.replace('*', 'all')
                    col_name = col_name.replace('.', '_')
                    
                    return f"{func_name}_{col_name}"
                    
                except Exception as e:
                    print(f"警告: 处理别名时出错 '{agg_expr}': {str(e)}")
                    # 返回一个安全的默认别名
                    return f"agg_{len(seen)}"
            
            for agg in agg_list:
                # 如果已有as子句，先移除
                base_expr = agg.split(' as ')[0].strip()
                
                # 生成基础别名
                base_alias = clean_alias(base_expr)
                
                if base_alias in seen:
                    # 如果函数已存在，添加数字后缀
                    seen[base_alias] += 1
                    result.append(f"{base_expr} as {base_alias}_{seen[base_alias]}")
                else:
                    seen[base_alias] = 1
                    result.append(f"{base_expr} as {base_alias}")
                    
            return result
    
        # 随机选择源表
        #source_table = random.choice(['meters', 'stream_trigger', '%%trows', 'v1', 'performance_schema.perf_connections'])
        basic_tables = ['meters', 'stream_trigger', 'v1', 'sv1', 'sv1_v1', 'sv1_v2']
        selected_system_tables = random.sample(self.system_tables, random.randint(2, 3))
        available_tables = basic_tables + selected_system_tables
        source_table = random.choice(available_tables)
        
        # # 根据源表选择列名
        # if source_table.startswith('performance_schema.') or source_table.startswith('information_schema.'):
        #     # 解析系统表结构
        #     columns_dict = self._parse_system_table_columns(source_table)
        #     if not columns_dict:
        #         raise Exception(f"无法解析系统表 {source_table} 的结构")
        #     columns = columns_dict['numeric'] + columns_dict['string'] + columns_dict['bool'] # 合并可用列
        #     # 系统表使用实际的时间戳列
        #     ts_column = columns_dict['timestamp'][0] if columns_dict['timestamp'] else 'now'
        
        if source_table.startswith('performance_schema.') or source_table.startswith('information_schema.'):
            columns_dict = self._parse_system_table_columns(source_table)
            if not columns_dict:
                raise Exception(f"无法解析系统表 {source_table} 的结构")
                
            # 合并所有可用列
            columns = []
            columns.extend(columns_dict.get('numeric', []))
            columns.extend(columns_dict.get('string', []))
            columns.extend(columns_dict.get('bool', []))
            
            # 获取时间戳列
            ts_columns = columns_dict.get('timestamp', [])
            if ts_columns:
                # 如果有时间戳列，随机选择一个
                ts_function = f"`{random.choice(ts_columns)}`"
            else:
                # 如果没有时间戳列，使用now函数
                ts_function = 'now'
            print(f"选择的时间戳: {ts_function}") 
            
            # 随机决定是否使用聚合函数
            use_aggregation = random.choice([True, False])
            select_items = []
            
            if use_aggregation and columns_dict['numeric']:
                # 从数值类型列中选择1-2个应用聚合函数
                num_cols = min(len(columns_dict['numeric']), random.randint(1, 2))
                selected_numeric = random.sample(columns_dict['numeric'], num_cols)
                
                # 为每个选中的数值列随机选择聚合函数
                for col in selected_numeric:
                    agg_func = random.choice(self.agg_functions)
                    agg_expr = agg_func.format(col=f"`{col}`")
                    select_items.append(agg_expr)
                    print(f"添加聚合表达式: {agg_expr}")
                
            else:
                # 直接选择2-4列
                all_cols = columns_dict['numeric'] + columns_dict['string'] + columns_dict['bool']
                if all_cols:
                    num_cols = min(len(all_cols), random.randint(2, 4))
                    selected_cols = random.sample(all_cols, num_cols)
                    select_items = [f"`{col}` as column{i}" for i, col in enumerate(selected_cols)]
            
            # 确保至少有一个列被选择
            if not select_items:
                if columns_dict['numeric']:
                    col = random.choice(columns_dict['numeric'])
                    select_items.append(f"`{col}` as column0")
                elif columns_dict['string']:
                    col = random.choice(columns_dict['string'])
                    select_items.append(f"`{col}` as column0")
                else:
                    raise Exception(f"表 {source_table} 没有可用的列")
            
            # 构建完整的select子句
            select_clause = f"select {ts_function} ts, {', '.join(select_items)}"
        
        else:
            # 原有表的列处理逻辑
            if source_table == 'meters':
                columns = self.meters_columns['numeric']
            elif source_table == 'stream_trigger' or source_table == '%%trows':
                columns = self.trigger_columns['numeric']
            else:  # v1虚拟表
                columns = self.virtual_columns['numeric']
            
        
        # 随机决定是使用聚合函数还是直接选择列
        use_aggregation = random.choice([True, False])
        
        # 获取时间戳处理函数
        if source_table.startswith('performance_schema.') or source_table.startswith('information_schema.'):
            ts_function = ts_function  
        else:
            ts_function = self._get_ts_function(source_table, window_type)
        
        if use_aggregation and not source_table.startswith('performance_schema.') and not source_table.startswith('information_schema.'):
            # 选择合适的聚合函数
            aggs = random.sample(self.agg_functions, random.randint(1, 3))
            aggs = [agg.format(col=random.choice(columns)) for agg in aggs]
            aggs = add_unique_alias(aggs)
            select_items = aggs
        else:
            # 直接选择列模式
            selected_cols = random.sample(columns, min(random.randint(2, 10), len(columns)))
            if source_table.startswith('performance_schema.') or source_table.startswith('information_schema.'):
                # 系统表列使用反引号包裹
                select_items = [f"`{col}` as column{i}" for i, col in enumerate(selected_cols)]
            else:
                select_items = [f"{col} as col_{i}" for i, col in enumerate(selected_cols)]
        
        # 构建select子句
        select_clause = f"select {ts_function} ts, {', '.join(select_items)}"
        
        # 处理PARTITION BY (系统表不使用partition by)
        partition_by = "" if source_table.startswith('performance_schema.')  or source_table.startswith('information_schema.') else \
                    ("partition by tbname" if (source_table == 'meters' or source_table == 'sv1') else "")
        
        # 生成WHERE条件
        # 只有非系统表才需要where子句
        where_clause = ""
        if not any(source_table.startswith(prefix) for prefix in ['performance_schema.', 'information_schema.']):
            where_clause = self._get_random_where_condition(source_table, window_type)
        
        # 返回完整的create stream语句
        return f"""create stream {database}.s{stream_number} {window} 
            from {database}.stream_trigger {partition_by} 
            into {database}.st{stream_number} 
            as {select_clause} 
            from {source_table}  
            {where_clause};"""
            
    def _get_numeric_columns(self, table_name):
        """获取表的数值类型列
        
        Args:
            table_name: 表名
            
        Returns:
            list: 数值类型列名列表
        """
        if table_name == 'stream_trigger':
            return self.trigger_columns['numeric']
        elif table_name == 'meters':
            return self.meters_columns['numeric']
        elif table_name == 'v1' or table_name == 'sv1' or table_name == 'sv1_v1' or table_name == 'sv1_v2' :
            return self.virtual_columns['numeric']
        # elif table_name == 'test.meters':
            # 假设test.meters表有这些数值列
            #return ['current', 'voltage', 'phase', 'groupid']
            # todo
        else:
            columns_dict = self._parse_system_table_columns(table_name)
            return columns_dict.get('numeric', [])
    
    def generate_interval_stream(self, stream_number, database, 
                            interval_val=None, sliding_val=None,
                            interval_offset=None, offset_time=None):
        """
        生成带interval窗口的流计算
        
        参数:
            stream_number: 流编号
            database: 数据库名
            interval_val: 窗口时长(如'5m','1h')
            sliding_val: 滑动时长(如'1m','30s') 
            interval_offset: 窗口偏移(可选)
            offset_time: 触发时间偏移(可选)
        """
        
        period_units = ['m', 'h', 's']
        offset_units = ['s', 'm']
        
        interval_ranges = {
            's': (5, 60),    # 秒级: 5秒-60秒
            'm': (1, 60),    # 分钟级: 1分钟-60分钟
            'h': (1, 24)     # 小时级: 1小时-24小时
        }
        
        sliding_ranges = {
            's': (1, 30),    # 秒级滑动: 1秒-30秒
            'm': (1, 30)     # 分钟级滑动: 1分钟-30分钟
        }
        
        offset_ranges = {
            's': (0, 30),    # 秒级偏移: 0秒-30秒
            'm': (0, 5)      # 分钟级偏移: 0分钟-5分钟
        }
        
        def generate_random_time(ranges, units):
            """生成随机时间值"""
            unit = random.choice(units)
            value = random.randint(ranges[unit][0], ranges[unit][1])
            return f"{value}{unit}"
        
        def validate_time(time_value, valid_units):
            """验证时间值格式"""
            if not time_value:
                return False
            match = re.match(r'(\d+)([smh])', time_value)
            if not match:
                return False
            value, unit = match.groups()
            return unit in valid_units
        
        def parse_time_value(time_str):
            """解析时间值为秒数"""
            if not time_str:
                return 0
            match = re.match(r'(\d+)([smh])', time_str)
            if not match:
                return 0
            value, unit = match.groups()
            value = int(value)
            if unit == 'h':
                return value * 3600
            elif unit == 'm':
                return value * 60
            return value
        
        def validate_offsets(interval, interval_off, sliding, sliding_off):
            """验证偏移值是否合法"""
            # 解析所有时间值为秒
            interval_secs = parse_time_value(interval)
            interval_off_secs = parse_time_value(interval_off)
            sliding_secs = parse_time_value(sliding)
            sliding_off_secs = parse_time_value(sliding_off)
            
            # 验证窗口偏移是否小于窗口长度
            if interval_off_secs >= interval_secs:
                interval_off = f"{interval_secs//2}s"
                
            # 验证滑动偏移是否小于滑动长度
            if sliding_off_secs >= sliding_secs:
                sliding_off = f"{sliding_secs//2}s"
                
            return interval_off, sliding_off
        
        # 生成或验证时间值
        if not interval_val or not validate_time(interval_val, period_units):
            interval_val = generate_random_time(interval_ranges, period_units)
            
        if not sliding_val or not validate_time(sliding_val, period_units):
            sliding_val = generate_random_time(sliding_ranges, ['s', 'm'])
            
        # 随机决定是否添加偏移并验证大小关系 
        if interval_offset is None and random.choice([True, False]):
            interval_offset = generate_random_time(offset_ranges, offset_units)
            
        if offset_time is None and random.choice([True, False]):
            offset_time = generate_random_time(offset_ranges, offset_units)
            
        # 验证并调整偏移值
        if interval_offset or offset_time:
            interval_offset, offset_time = validate_offsets(
                interval_val, interval_offset,
                sliding_val, offset_time
            )
        
        # 构建窗口子句
        window = " "
        if interval_offset:
            window += f"INTERVAL({interval_val}, {interval_offset}) "
        else:
            window += f"INTERVAL({interval_val}) "
            
        if offset_time:
            window += f"SLIDING({sliding_val}, {offset_time}) "
        else:
            window += f"SLIDING({sliding_val}) "
            
        return self._build_stream_sql(stream_number, database, window, 'interval')
    
    def generate_event_window_stream(self, stream_number, database, 
                                start_condition=None, end_condition=None,
                                duration_time=None):
        """
        生成事件窗口流计算SQL
        
        参数:
            stream_number: 流编号
            database: 数据库名
            start_condition: 开始条件,为None则随机生成
            end_condition: 结束条件,为None则随机生成
            duration_time: 最小持续时长,为None则随机决定是否添加
        """
        trigger_table = random.choice(['meters', 'stream_trigger', 'v1', 'sv1', 'sv1_v1', 'sv1_v2']) #, 'test.meters'
        
        numeric_cols = self._get_numeric_columns(trigger_table)
        if not numeric_cols:
            raise Exception(f"表 {trigger_table} 没有可用的数值列")
    
        condition_templates = [
            "{col} = {val}",
            "{col} > {val}",
            "{col} < {val}",
            "{col} between {val1} and {val2}",
            "abs({col} - {val}) > {val2}"
        ]
        
        duration_units = ['s', 'm']
        duration_ranges = {
            's': (1, 30),     # 秒级: 1-30秒
            'm': (1, 5)       # 分钟级: 1-5分钟
        }
        
        start_col = random.choice(numeric_cols)
        end_col = random.choice(numeric_cols)
        
        start_template = random.choice(condition_templates)
        end_template = random.choice(condition_templates)
        
        def generate_condition(template, col):
            val1 = random.randint(1, 100)
            val2 = random.randint(1, 100)
            return template.format(col=col, val=val1, val1=min(val1, val2), val2=max(val1, val2))
        
        start_condition = generate_condition(start_template, start_col)
        end_condition = generate_condition(end_template, end_col)
        
        def generate_random_duration():
            """生成随机持续时长"""
            unit = random.choice(duration_units)
            value = random.randint(duration_ranges[unit][0], 
                                duration_ranges[unit][1])
            return f"{value}{unit}"
        
        # 随机决定是否添加持续时长
        if duration_time is None:
            duration_time = generate_random_duration() if random.choice([True, False]) else None
        
        # 构建事件窗口子句
        window = f"EVENT_WINDOW(START WITH {start_condition} END WITH {end_condition})"
        if duration_time:
            window += f" TRUE_FOR({duration_time})"
        #window += " IGNORE EXPIRED 0 IGNORE UPDATE 0"
        
        return self._build_stream_sql(stream_number, database, window, 'event', trigger_table)
 
    def generate_state_window_stream(self, stream_number, database, 
                                state_column=None, duration_time=None):
        """
        生成状态窗口流计算SQL
        
        参数:
            stream_number: 流编号
            database: 数据库名
            state_column: 状态列名,为None则随机选择
            duration_time: 最小持续时长,为None则随机决定是否添加
        """
        
        # 获取触发表
        trigger_table = random.choice(['meters', 'stream_trigger', 'v1', 'sv1', 'sv1_v1', 'sv1_v2']) #, 'test.meters'
        
        # 获取该表的数值列
        numeric_cols = self._get_numeric_columns(trigger_table)
        if not numeric_cols:
            raise Exception(f"表 {trigger_table} 没有可用的数值列")
        
        # 随机选择一个状态列
        state_column = random.choice(numeric_cols)
        
        duration_units = ['s', 'm']
        duration_ranges = {
            's': (1, 30),     # 秒级: 1-30秒
            'm': (1, 5)       # 分钟级: 1-5分钟
        }
        
        def generate_random_duration():
            """生成随机持续时长"""
            unit = random.choice(duration_units)
            value = random.randint(duration_ranges[unit][0], 
                                duration_ranges[unit][1])
            return f"{value}{unit}"
        
        # 随机决定是否添加持续时长
        if duration_time is None:
            duration_time = generate_random_duration() if random.choice([True, False]) else None
        
        # 构建状态窗口子句
        window = f"STATE_WINDOW({state_column})"
        if duration_time:
            window += f" TRUE_FOR({duration_time})"
        #window += " IGNORE EXPIRED 0 IGNORE UPDATE 0"
        
        return self._build_stream_sql(stream_number, database, window, 'state',trigger_table)
    
    def generate_session_stream(self, stream_number, database):
        """专门生成session类型的stream"""
        # 获取触发表
        trigger_table = random.choice(['meters', 'stream_trigger', 'v1', 'sv1', 'sv1_v1', 'sv1_v2']) #, 'test.meters'
        columns_dict = self._parse_system_table_columns(trigger_table)
        
        # 获取时间戳列
        timestamp_cols = columns_dict['timestamp']
        if not timestamp_cols:
            raise Exception(f"表 {trigger_table} 没有可用的时间戳列")
        
        timestamp_col = timestamp_cols[0]  # 使用第一个时间戳
        
        # 生成会话窗口参数
        interval = random.randint(1, 100)
        unit = random.choice(['s', 'm', 'h', 'd'])
        
        # 生成基本会话窗口
        window = f"session({timestamp_col}, {interval}{unit})"
            
        return self._build_stream_sql(stream_number, database, window, 'session', trigger_table)
    
    def generate_count_window_stream(self, stream_number, database):
        """专门生成count window类型的stream
        
        Args:
            stream_number: stream编号
            database: 数据库名
        Returns:
            str: 生成的stream SQL语句
        """
        # 获取触发表
        trigger_table = random.choice(['meters', 'stream_trigger', 'v1', 'sv1', 'sv1_v1', 'sv1_v2'])
        
        # 获取表的列信息
        columns_dict = self._parse_system_table_columns(trigger_table)
        
        # 随机选择count window类型
        window_type = random.choice(self.count_windows)
        
        # 为count window设置参数
        count_val = random.randint(2, 2147483647)  # 计数条数范围2-2147483647
        sliding_val = random.randint(1, min(1000, count_val))  # 滑动条数不超过count

        params = {
            "count": count_val,
            "sliding": sliding_val
        }
        
        # 随机决定是否使用触发列
        use_trigger_cols = random.choice([True, False])
        if use_trigger_cols:
        # 从触发表的列中随机选择1-2个作为触发列
            trigger_cols = random.sample(columns_dict['numeric'], 
                                    random.randint(1, len(columns_dict['numeric'])))
            window = f"COUNT_WINDOW({params['count']}, {params['sliding']}, {', '.join(trigger_cols)})"
        else:
            # 不使用触发列
            window = window_type.format(**params)
            
        return self._build_stream_sql(stream_number, database, window, 'count', trigger_table)
    
    def generate_period_stream(self, stream_number, database, 
                        period_val=None, offset_val=None):
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
        
        period_ranges = {
            'd': (1, 365),    # 1-365天
            'h': (1, 24),     # 1-24小时
            'm': (1, 60),     # 1-60分钟
            's': (1, 60),      # 1-60秒
            'a': (10, 1000)    # 1-1000毫秒
        }
        
        offset_ranges = {
            'h': (0, 12),     # 0-12小时
            'm': (0, 30),     # 0-30分钟
            's': (0, 30),     # 0-30秒
            'a': (0, 1000)    # 0-1000毫秒
        }
        
        def validate_time(time_value, valid_units):
            """验证时间值格式"""
            if not time_value:
                return False
            match = re.match(r'(\d+)([smhd])', time_value)
            if not match:
                return False
            value, unit = match.groups()
            return unit in valid_units
        
        def generate_random_time(ranges, units):
            """生成随机时间值"""
            unit = random.choice(units)
            value = random.randint(ranges[unit][0], ranges[unit][1])
            return f"{value}{unit}"
        
        # 生成或验证周期值 - 允许使用天单位
        if not period_val or not validate_time(period_val, period_units):
            period_val = generate_random_time(period_ranges, period_units)
        
        # 生成或验证偏移值 - 不允许使用天单位
        if offset_val is not None and not validate_time(offset_val, offset_units):
            # 如果提供了无效的偏移值，生成一个有效的
            offset_val = generate_random_time(offset_ranges, offset_units)
        elif offset_val is None and random.choice([True, False]):
            # 随机决定是否添加偏移
            offset_val = generate_random_time(offset_ranges, offset_units)
        
        # 解析时间值为秒数
        def parse_time_to_seconds(time_str):
            if not time_str:
                return 0
            match = re.match(r'(\d+)([smhd])', time_str)
            if not match:
                return 0
            value, unit = match.groups()
            value = int(value)
            if unit == 'd':
                return value * 86400    # 天转秒
            elif unit == 'h':
                return value * 3600     # 小时转秒
            elif unit == 'm':
                return value * 60       # 分钟转秒
            return value                # 秒

        # 验证并调整时间值
        def validate_and_adjust_time(period_seconds, offset_seconds):
            if offset_seconds >= period_seconds:
                # 如果偏移值大于周期值，将偏移值调整为周期值的一半
                new_offset_seconds = period_seconds // 2
                if new_offset_seconds > 3600:
                    return f"{new_offset_seconds//3600}h"
                elif new_offset_seconds > 60:
                    return f"{new_offset_seconds//60}m"
                else:
                    return f"{new_offset_seconds}s"
            return None  # 不需要调整

        # 如果提供了offset，验证大小关系
        if offset_val:
            period_seconds = parse_time_to_seconds(period_val)
            offset_seconds = parse_time_to_seconds(offset_val)
            
            # 如果offset大于period，进行调整
            adjusted_offset = validate_and_adjust_time(period_seconds, offset_seconds)
            if adjusted_offset:
                offset_val = adjusted_offset
                
        # 格式化窗口类型
        window = f"PERIOD({period_val}"
        if offset_val:
            window += f", {offset_val}"
        window += ")"
        #window += ") IGNORE EXPIRED 0 IGNORE UPDATE 0"
            
        return self._build_stream_sql(stream_number, database, window, 'period')
    
    def generate_period_stream_bak(self, stream_number, database):
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
            # 'interval', # 时间间隔窗口
            # 'state',    # 状态窗口 
            # 'event',    # 事件窗口 
            # 'period'    # 周期窗口 
        ])
        
        if stream_type == 'all':
            implemented_types = [
                'session',
                'count',
                'interval',
                'event',
                'state',
                'period'  
            ]
            stream_type = random.choice(implemented_types)
        
        # 根据选择的类型调用对应的生成函数
        if stream_type == 'session':
            return self.generate_session_stream(stream_number, database)
        elif stream_type == 'count':
            return self.generate_count_window_stream(stream_number, database)
        elif stream_type == 'interval':
            return self.generate_interval_stream(stream_number, database)
        elif stream_type == 'event':
            return self.generate_event_window_stream(stream_number, database)
        elif stream_type == 'state':
            return self.generate_state_window_stream(stream_number, database)
        elif stream_type == 'period':
            return self.generate_period_stream(stream_number, database)
        else:
            raise ValueError(f"未支持的窗口类型: {stream_type}")
        
    def run_test_cycle(self, database, num_streams, iterations=1, sleep_interval=1):
        """循环生成和删除指定数量的流
        
        Args:
            database: 数据库名称
            num_streams: 每次生成的流数量
            iterations: 循环次数，默认为1
            sleep_interval: 每次操作后的等待时间(秒)，默认为1
        """
        import time
        
        for i in range(iterations):
            print(f"\n=== 开始第 {i+1} 轮测试 ===")
            
            # 生成的流ID列表
            stream_numbers = []
            
            try:
                # 生成n个流
                print(f"\n正在生成 {num_streams} 个流...")
                for j in range(num_streams):
                    stream_number = j + 1
                    stream_numbers.append(stream_number)
                    
                    # 随机选择一种流类型生成
                    stream_type = random.choice(['normal', 'session', 'state', 'event', 'count'])
                    
                    try:
                        if stream_type == 'normal':
                            self.generate_normal_stream(stream_number, database)
                        elif stream_type == 'session':
                            self.generate_session_stream(stream_number, database)
                        elif stream_type == 'state':
                            self.generate_state_window_stream(stream_number, database)
                        elif stream_type == 'event':
                            self.generate_event_window_stream(stream_number, database)
                        else:
                            self.generate_count_window_stream(stream_number, database)
                            
                        print(f"成功创建流 {database}.s{stream_number}")
                        time.sleep(sleep_interval)  # 等待一段时间
                        
                    except Exception as e:
                        print(f"创建流 {database}.s{stream_number} 失败: {str(e)}")
                        continue
                
                # 删除所有生成的流
                print(f"\n正在删除 {len(stream_numbers)} 个流...")
                for stream_number in stream_numbers:
                    try:
                        self.tdSql.execute(f"drop stream if exists {database}.s{stream_number}")
                        print(f"成功删除流 {database}.s{stream_number}")
                        time.sleep(sleep_interval)
                    except Exception as e:
                        print(f"删除流 {database}.s{stream_number} 失败: {str(e)}")
                
            except Exception as e:
                print(f"\n第 {i+1} 轮测试出错: {str(e)}")
            
            print(f"\n=== 第 {i+1} 轮测试完成 ===")
            

class TestStreamTriggerType1:
        
    def setup_class(cls):
        """测试类初始化"""
        cls.database = "qdb"
        cls.vgroups = random.randint(1, 30)
        # 添加默认的stream数量参数
        cls.stream_count = 50
        
        # from random_stream import StreamGenerator  
        # stream_gen = StreamGenerator(tdSql)
        
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
        # eg:STREAM_COUNT=10 pytest --clean cases/18-StreamProcessing/99-Others/random_stream.py  --skip_stop 
        import os
        stream_count = int(os.getenv('STREAM_COUNT', self.stream_count))
        tdLog.debug(f"使用的stream数量: {stream_count}")
        
        # 打开文件用于写入SQL
        with open('all.sql', 'w') as f, open('success.sql', 'w') as success_f, open('error.sql', 'w') as error_f:
            # 初始化环境
            tdStream.dropAllStreamsAndDbs()
            tdStream.createSnode()
            #proc = subprocess.Popen('taosBenchmark -y',stdout=subprocess.PIPE, shell=True, text=True)
            # 获取当前脚本的目录路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # 首先回到 TDinternal 目录
            td_internal_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))))
            # 然后构建 cfg 目录路径
            cfg_dir = os.path.join(td_internal_dir, 'sim', 'dnode1', 'cfg')

            # 使用构建的路径执行 taosBenchmark 命令
            cmd = f'taosBenchmark -c {cfg_dir} -y'
            subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, text=True)
            
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
                
            tdLog.info(f"=============== create vtable")
            sql = """create vtable v1 (
                    ts timestamp,
                    t1_cint int from t1.cint,
                    t1_cuint INT UNSIGNED from t1.cuint,
                    t2_cint int from t2.cint,
                    t2_cuint INT UNSIGNED from t2.cuint,   
                    stream_trigger_c1 int from stream_trigger.c1,
                    stream_trigger_c2 int from stream_trigger.c2);"""
            tdSql.execute(sql)
            
            tdLog.info(f"=============== create stable")
            stable_sql = """create stable s1 (
                    ts timestamp,
                    t1_cint int ,
                    t1_cuint INT UNSIGNED ,
                    t2_cint int ,
                    t2_cuint INT UNSIGNED ,   
                    stream_trigger_c1 int ,
                    stream_trigger_c2 int )
                    TAGS (t0 INT, t1 VARCHAR(32)) VIRTUAL 0;"""
            tdSql.execute(stable_sql)
            tdSql.execute(f"create table sub_t1 using s1 tags(1,'t1')")
            tdSql.execute(f"create table sub_t2 using s1 tags(2,'t2')")
            i = random.randint(2, 500)
            while i > 0:
                tdSql.execute(f"insert into sub_t1 values (now,{i},{i},{i},{i},{i},{i})")
                tdSql.execute(f"insert into sub_t2 values (now,{i},{i},{i},{i},{i},{i})")
                i = i - 1
            
            tdLog.info(f"=============== create vstable")
            vstable_sql = """create stable sv1 (
                    ts timestamp,
                    t1_cint int ,
                    t1_cuint INT UNSIGNED ,
                    t2_cint int ,
                    t2_cuint INT UNSIGNED ,   
                    stream_trigger_c1 int ,
                    stream_trigger_c2 int )
                    TAGS (t0 INT, t1 VARCHAR(32)) VIRTUAL 1;"""
            tdSql.execute(vstable_sql)
            vtable_sql = """create vtable sv1_v1
                    (sub_t1.t1_cint, sub_t1.t1_cuint,
                    sub_t2.t2_cint, sub_t2.t2_cuint,
                    sub_t2.stream_trigger_c1, sub_t2.stream_trigger_c2)
                    using sv1
                    TAGS (1, 'vtable_1');"""
            tdSql.execute(vtable_sql)
            vtable_sql = """create vtable sv1_v2
                    (sub_t1.t1_cint, sub_t1.t1_cuint,
                    sub_t2.t2_cint, sub_t2.t2_cuint,
                    sub_t2.stream_trigger_c1, sub_t2.stream_trigger_c2)
                    using sv1
                    TAGS (2, 'vtable_2');"""
            tdSql.execute(vtable_sql)
            vtable_sql = """create vtable sv1_v3
                    using sv1
                    TAGS (2, 'vtable_3');"""
            tdSql.execute(vtable_sql)
            
            # 使用StreamGenerator生成随机stream
            generator = StreamGenerator(tdSql)
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

            # print("开始写入测试数据..第2批.")
            # batch_sqls = self.generate_test_data(self.database, rows=1000, batch_size=100)
            # success_batches = 0
            # total_batches = len(batch_sqls)
            
            # for batch_id, batch in enumerate(batch_sqls, 1):
            #     try:
            #         # 每个批次写入前暂停一小段时间
            #         time.sleep(0.1)  
                    
            #         # 执行当前批次
            #         tdSql.executes(batch)
                    
            #         # 写入文件
            #         for sql in batch:
            #             f.write(f"{sql}\n")
            #             success_f.write(f"{sql}\n")
                        
            #         success_batches += 1
            #         print(f"完成批次 {batch_id}/{total_batches}")
                    
            #     except Exception as e:
            #         print(f"批次 {batch_id} 写入失败: {str(e)}")
            #         # 写入错误日志但继续执行
            #         error_f.write(f"-- Batch {batch_id} 写入失败\n")
            #         error_f.write(f"-- 错误信息: {str(e)}\n")
            #         for sql in batch:
            #             error_f.write(f"{sql}\n")
            #         error_f.write("\n")
                    
            # print(f"数据写入完成: 成功 {success_batches} 批, 失败 {total_batches - success_batches} 批")
            
            # f.write("\n")
            # success_f.write("\n")
            # print("第2批写入测试数据结束...")
            
            # 随机决定是否执行循环测试
            if random.random() < 0.79:  # 70%的概率执行循环测试
                iterations = random.randint(3, 10)  # 随机执行3-10轮
                num_streams = random.randint(10, 20)  # 每轮随机生成10-20个流
                sleep_interval = random.randint(1, 3)  # 随机等待1-3秒
                
                print(f"\n=== 开始随机循环测试 ({iterations}轮) ===")
                
                for i in range(iterations):
                    print(f"\n--- 第 {i+1} 轮测试开始 ---")
                    stream_numbers = []
                    
                    try:
                        # 生成流
                        print(f"正在生成 {num_streams} 个流...")
                        for j in range(num_streams):
                            #stream_number = random.randint(1000, 9999)  # 使用随机ID避免冲突
                            stream_number = 1000 + j # 使用固定ID
                            stream_numbers.append(stream_number)
                            
                            # 随机选择流类型
                            stream_type = random.choice(['interval', 'session', 'state', 'event', 'period' , 'count'])
                            
                            try:
                                if stream_type == 'interval':
                                    stream_sql = generator.generate_interval_stream(stream_number, self.database)
                                elif stream_type == 'session':
                                    stream_sql = generator.generate_session_stream(stream_number, self.database)
                                elif stream_type == 'state':
                                    stream_sql = generator.generate_state_window_stream(stream_number, self.database)
                                elif stream_type == 'event':
                                    stream_sql = generator.generate_event_window_stream(stream_number, self.database)
                                elif stream_type == 'period':
                                    stream_sql = generator.generate_period_stream(stream_number, self.database)
                                else:
                                    stream_sql = generator.generate_count_window_stream(stream_number, self.database)

                                print(f"\n生成的建流语句 (stream_number: {stream_number}, type: {stream_type}):")
                                print(f"{stream_sql}")
                                                               
                                tdSql.execute(f"drop table if exists {self.database}.st{stream_number}")
                                
                                # 执行创建stream的SQL
                                tdSql.execute(stream_sql)
                                
                                # 查询当前系统中的流信息
                                try:
                                    tdSql.query("select stream_name from information_schema.ins_streams")
                                    streams_in_system = tdSql.queryResult
                                    # print(f"\n当前系统中的所有流:")
                                    # for row in streams_in_system:
                                    #     print(f"  - {row[0]}")
                                    print(f"总共 {len(streams_in_system)} 个流")
                                except Exception as query_e:
                                    print(f"查询流信息失败: {str(query_e)}")
                                                                       
                                success_f.write(f"成功创建流 {self.database}.s{stream_number}\n")
                                success_f.write(f"SQL: {stream_sql}\n")
                                print(f"成功创建流 {self.database}.s{stream_number}")
                                time.sleep(sleep_interval)
                                
                            except Exception as e:
                                error_f.write(f"创建流 {self.database}.s{stream_number} 失败: {str(e)}\n")
                                error_f.write(f"SQL: {stream_sql if 'stream_sql' in locals() else '建流SQL生成失败'}\n")
                                print(f"创建流 {self.database}.s{stream_number} 失败: {str(e)}")
                                print(f"失败的SQL: {stream_sql if 'stream_sql' in locals() else '建流SQL生成失败'}")

                                # 即使创建失败也查询一次流信息，看看系统状态
                                try:
                                    tdSql.query("select stream_name from information_schema.ins_streams")
                                    streams_in_system = tdSql.queryResult
                                    print(f"创建失败后，当前系统中的流数量: {len(streams_in_system)}")
                                except:
                                    print("查询流信息失败")
                                
                                continue
                        
                        # 删除生成的流
                        print(f"\n正在删除 {len(stream_numbers)} 个流...")
                        for stream_number in stream_numbers:
                            try:
                                tdSql.execute(f"drop stream if exists {self.database}.s{stream_number}")
                                success_f.write(f"成功删除流 {self.database}.s{stream_number}\n")
                                print(f"成功删除流 {self.database}.s{stream_number}")
                                time.sleep(sleep_interval)
                            except Exception as e:
                                f.write(f"删除流 {self.database}.s{stream_number} 失败: {str(e)}\n")
                                print(f"删除流 {self.database}.s{stream_number} 失败: {str(e)}")
                        
                    except Exception as e:
                        f.write(f"\n第 {i+1} 轮测试出错: {str(e)}\n")
                        print(f"\n第 {i+1} 轮测试出错: {str(e)}")
                    
                    print(f"--- 第 {i+1} 轮测试完成 ---")
                
                print("=== 随机循环测试完成 ===\n")
            
                    
            # 检查stream状态和结果
            if success_count > 0:
                #tdStream.checkStreamStatus() #todo,看看后续有没有更合适的方法
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
                
            tdStream.dropAllStreamsAndDbs()
            
            
        
    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()