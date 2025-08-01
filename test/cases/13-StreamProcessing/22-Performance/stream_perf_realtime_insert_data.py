import json
import subprocess
import threading
import time
import taos
import argparse
import os
import datetime

    
class StreamStarter:
    def __init__(self, runtime=None,  table_count=1000, insert_rows=1, next_insert_rows=250, 
                sql_type='insert', disorder_ratio=0, vgroups=4,cluster_root=None) -> None:
        # 设置集群根目录,默认使用/mnt/merged_disk/taos_stream_cluster2
        self.cluster_root = cluster_root if cluster_root else '/mnt/merged_disk/taos_stream_cluster2'        
        self.table_count = table_count      # 子表数量
        self.insert_rows = insert_rows      # 插入记录数
        self.next_insert_rows = next_insert_rows      # 后续每轮插入记录数
        self.disorder_ratio = disorder_ratio # 数据乱序率
        self.vgroups = vgroups # vgroups数量
        self.sql_type = sql_type 
        # 定义3个实例的配置
        self.instances = [
            {
                'name': 'dnode1',
                'host': 'localhost',
                'port': 6030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode1/data',
                'log_dir': f'{self.cluster_root}/dnode1/log',
            },
            {
                'name': 'dnode2',
                'host': 'localhost',
                'port': 7030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode2/data',
                'log_dir': f'{self.cluster_root}/dnode2/log',
            },
            {
                'name': 'dnode3',
                'host': 'localhost',
                'port': 8030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode3/data/',
                'log_dir': f'{self.cluster_root}/dnode3/log',
            }
        ]
        
        
        self.sql = None
        self.host = self.instances[0]['host']
        self.user = self.instances[0]['user']
        self.passwd = self.instances[0]['passwd']
        self.conf = f"{self.cluster_root}/dnode1/conf/taos.cfg"
        self.tz = 'Asia/Shanghai'
        # 设置运行时间和性能文件路径
        self.runtime = runtime if runtime else 600  # 默认运行10分钟
        
      

    def get_connection(self):
        """获取数据库连接
        
        Returns:
            tuple: (connection, cursor)
        """
        try:
            conn = taos.connect(
                host=self.host,
                user=self.user,
                password=self.passwd,
                config=self.conf,
                timezone=self.tz
            )
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            raise

        
    def create_database(self, db_name, vgroups=None, dnodes=None):
        """创建数据库
        
        Args:
            db_name: 数据库名称
            vgroups: vgroups数量,如果不指定则使用配置中的值
            dnodes: 指定数据库所在的dnode,如果不指定则使用配置中的值
        """
        try:
            # 获取数据库配置
            db_config = self.db_config.get(db_name, {})
            vgroups = vgroups or db_config.get('vgroups', self.vgroups)
            dnodes = dnodes or db_config.get('dnodes', '1')
            
            # 创建数据库
            conn, cursor = self.get_connection()
            create_db_sql = f"create database {db_name} vgroups {vgroups}"
            if dnodes:
                create_db_sql += f" dnodes '{dnodes}'"
                
            print(f"\n创建数据库: {create_db_sql}")
            cursor.execute(create_db_sql)
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            print(f"数据库 {db_name} 创建成功")
            return True
            
        except Exception as e:
            print(f"创建数据库 {db_name} 失败: {str(e)}")
            return False

    def insert_source_from_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": f"{self.cluster_root}/dnode1/conf",
            "host": "localhost",
            "port": 6030,
            "rest_port": 6041,
            "user": "root",
            "password": "taosdata",
            "thread_count": 50,
            "create_table_thread_count": 5,
            "result_file": "/tmp/taosBenchmark_result.log",
            "confirm_parameter_prompt": "no",
            "insert_interval": 1,
            "num_of_records_per_req": 1000,
            "max_sql_len": 102400,
            "databases": [
                {
                    "dbinfo": {
                        "name": "stream_from",
                        "drop": "no",
                        "replica": 1,
                        "duration": 10,
                        "precision": "ms",
                        "keep": 3650,
                        "minRows": 100,
                        "maxRows": 4096,
                        "comp": 2,
                        "dnodes": "1",
                        "vgroups": self.vgroups,
                        "stt_trigger": 2,
                        "WAL_RETENTION_PERIOD": 86400
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "yes",
                            "childtable_count": self.table_count,
                            "childtable_prefix": "ctb0_",
                            "escape_character": "no",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "interlace_rows": 1,
                            "tcp_transfer": "no",
                            "insert_rows": self.next_insert_rows,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": self.disorder_ratio,
                            "disorder_range": 1000,
                            "keep_trying": -1,
                            "timestamp_step": 50,
                            "trying_interval": 10,
                            "start_timestamp": "2025-06-01 00:00:00",
                            "sample_format": "csv",
                            "sample_file": "./sample.csv",
                            "tags_file": "",
                            "columns": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "BIGINT",
                                    "count": 1
                                },
                                {
                                    "type": "DOUBLE",
                                    "count": 1
                                },
                                {
                                    "type": "FLOAT",
                                    "count": 1
                                }
                            ],
                            "tags": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "VARCHAR",
                                    "count": 1,
                                    "len": 32
                                }
                            ]
                        }
                    ]
                }
            ],
            "prepare_rand": 10000,
            "chinese": "no",
            "test_log": "/tmp/testlog/"
        }

        with open('/tmp/stream_from_insertdata.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            
    def update_insert_config(self):
        """
        更新数据插入配置
        Args:
            next_insert_rows: 下一轮要插入的数据行数
        """
        try:
            print("\n=== 更新数据插入配置 ===")
            
            # 获取最新时间戳
            conn = taos.connect(
                host=self.host,
                user=self.user,
                password=self.passwd,
                config=self.conf
            )
            cursor = conn.cursor()
            
            try:
                # 根据 SQL 类型决定时间戳更新方式
                if self.sql_type == 's2_5' or self.sql_type == 's2_11':
                    next_start_time = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"流类型为{self.sql_type}，使用当前时间作为起始时间: {next_start_time}")
                
                else:
                    # 查询最新时间戳
                    cursor.execute("select last(ts) from stream_from.stb")
                    last_ts = cursor.fetchall()[0][0]
                
                    if not last_ts:
                        raise Exception("未能获取到最新时间戳")
                        
                    # 将时间戳转换为字符串格式，并加上1秒
                    next_start_time = (last_ts + datetime.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                    print(f"当前最新时间戳: {last_ts}")
                    print(f"更新起始时间为: {next_start_time}")
                
                # 读取现有配置
                config_file = '/tmp/stream_from_insertdata.json'
                with open(config_file, 'r') as f:
                    content = f.read()
                
                # 使用正则表达式替换时间戳
                import re
                new_content = re.sub(
                    r'"start_timestamp":\s*"[^"]*"',
                    f'"start_timestamp": "{next_start_time}"',
                    content,
                    count=1  # 只替换第一次出现的时间戳
                )
                
                # 格式化写入以确保 JSON 格式正确
                with open(config_file, 'w') as f:
                    f.write(new_content)
                
                print("配置时间戳已更新")
                return True
                
            except Exception as e:
                print(f"更新配置时出错: {str(e)}")
                return False
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            print(f"执行更新配置时出错: {str(e)}")
            return False

            
    def do_start(self):
        self.insert_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # 获取新连接执行流式查询
            conn, cursor = self.get_connection()
            
            print("开始连接数据库")
            cursor.execute('use stream_from')
          
            try:            
                # 循环执行写入和计算
                cycle_count = 0
                start_time = time.time()
                
                while True:
                    cycle_count += 1
                    print(f"\n=== 开始第 {cycle_count} 轮写入和计算 ===")
                    
                    if cycle_count > 1:
                        # 从第二轮开始，先写入新数据
                        print("\n写入新一批测试数据...")
                        if not self.update_insert_config():
                            raise Exception("写入新数据失败")
                        
                        cmd = "taosBenchmark -f /tmp/stream_from_insertdata.json"
                        if subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True).returncode != 0:
                            raise Exception("写入新数据失败")
                                           
                    # 检查是否达到运行时间限制
                    if time.time() - start_time >= self.runtime * 60:
                        print(f"\n\n已达到运行时间限制 ({self.runtime} 分钟)，停止执行")
                        break
                        
                    print(f"\n=== 第 {cycle_count} 轮处理完成 ===")
             
            except Exception as e:
                print(f"查询写入操作出错: {str(e)}")
            finally:
                cursor.close()
                conn.close()
                print("查询写入操作完成")               
                
        except Exception as e:
            print(f"执行错误: {str(e)}")            

                                    
    def format_timestamp(self, ts):
        """格式化时间戳为可读字符串
        Args:
            ts: 毫秒级时间戳
        Returns:
            str: 格式化后的时间字符串 (YYYY-MM-DD HH:mm:ss)
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts/1000))

    
def main():
    
    parser = argparse.ArgumentParser(description='TDengine Stream Test')
    parser.add_argument('-m', '--mode', type=int, default=1,
                        help='1: do_start')
    parser.add_argument('-t', '--time', type=int, default=10,
                        help='运行时间(分钟),默认10分钟')
    parser.add_argument('--table-count', type=int, default=1000,
                        help='子表数量,默认1000')
    parser.add_argument('--insert-rows', type=int, default=1,
                        help='初始插入记录数,默认1')
    parser.add_argument('--next-insert-rows', type=int, default=250,
                       help='后续每轮插入记录数')
    parser.add_argument('--disorder-ratio', type=int, default=0,
                        help='数据乱序率,默认0')
    parser.add_argument('--vgroups', type=int, default=4,
                        help='vgroups,默认4')
    parser.add_argument('--sql_type', type=str, default='insert',
                        help='sql_type,默认insert')
    parser.add_argument('--cluster-root', type=str, default='/mnt/merged_disk/taos_stream_cluster2',
                        help='集群根目录,默认/mnt/merged_disk/taos_stream_cluster2')
    
    args = parser.parse_args()
    
    # 打印运行参数
    print("运行参数:")
    print(f"运行模式: {args.mode}")
    print(f"运行时间: {args.time}分钟")
    print(f"子表数量: {args.table_count}")
    print(f"初始插入记录数: {args.insert_rows}")
    print(f"后续每轮插入记录数: {args.next_insert_rows}")
    print(f"数据乱序: {args.disorder_ratio}")
    print(f"vgroups数: {args.vgroups}")
    print(f"集群目录: {args.cluster_root}")
    
    starter = StreamStarter(
        runtime=args.time,
        table_count=args.table_count,
        insert_rows=args.insert_rows,
        next_insert_rows=args.next_insert_rows,
        disorder_ratio=args.disorder_ratio,
        sql_type=args.sql_type,
        vgroups=args.vgroups,
        cluster_root=args.cluster_root
    )  
    
    print("\n开始执行...")
    if args.mode == 1:
        print("执行模式: do_start")
        starter.do_start()
            

if __name__ == "__main__":
    main()
