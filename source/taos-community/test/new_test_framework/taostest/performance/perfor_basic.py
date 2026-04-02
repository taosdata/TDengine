"""
Author: your name
Date: 2022-02-09 14:54:09
LastEditTime: 2022-03-11 17:43:24
LastEditors: cpwu
Description:
FilePath: /taostest/taos-test-framework/taostest/performance/perfor_basic.py
"""

import collections
import datetime
import os
import re
from dataclasses import dataclass
from os.path import basename

from psutil import cpu_count

from ..util.benchmark import gen_benchmark_json
from ..util.file import dict2yaml


@dataclass
class PerforArgs:
    fqdn: int = None
    json_file: str = None
    monitor_type: str = "promethues"
    db_name: str = f"{os.path.splitext(basename(__file__))[0]}_db"
    stb_name: str = f"{os.path.splitext(basename(__file__))[0]}_stb"


class Env:
    """
    description:
        创建各类环境配置文件，包括各类环境配置生成与最终的yaml文件生成

    usage:
        0. 创建环境实例
            env = Env()
        1. taosd环境配置
            taosd1 = env.set_taosd_env()
            taosd_dnode1 = env.set_taosd_dnode(
                endpoint="test_serv:6030",
                config_dir=/tmp,
                **{"keepColumnName":1}
            )
            taosd_dnode2 = env.set_taosd_dnode()
            taosd2 = env.set_taosd_env(
                fqdn=["vm1","vm2],
                version="3.0.0.0",
                spec={"config":{firstEP': '127.0.0.1:6030'}},
                dnode=[taosd_dnode1,taosd_dnode2]
            )
        2. 类似的进行taospy、taosBenchmark和prometheus环境配置
            taospy = env.set_taospy_env()
            taosBenchmark = env.set_taosBenchmark_env()
            promethus = env.set_promethus_env()
        3. 创建环境配置的yaml文件：
            env.set_env_file(
                env_dict=[taosd1,taosd2,taospy,taosBenchmark,promethus],
                path="/tmp",
                filename="test.yaml"
            )
    """
    TAOSD            = "taosd"
    TAOSPY           = "taopy"
    TAOSBENCHMARK    = "taosBenchmark"
    PROMETHEUS       = "prometheus"

    @classmethod
    def key_in_keys(cls,key, dicts):
        Flag = False
        for k,v in dicts.items():
            if key == k:
                Flag = True
                return Flag
            if isinstance(v, dict):
                Flag = cls.key_in_keys(key, v)
        return Flag

    @classmethod
    def set_taosd_dnode(cls,
        endpoint :str = None,
        config_dir :str = None,
        cfg : dict = None,
        **kwargs
    ):
        """
        description:
        Author: cpwu
        param  :
            cfg, cfg为taosd配置文件内容字典，例如 'debugFlag': 135,意思为将debugFlag参数设置为135
        return 返回dnode的配置内容
        """
        endpoint = endpoint if endpoint else '127.0.0.1:6030'
        config_dir =  config_dir if config_dir else '/var/dnode1/config'
        if not cfg:
            cfg = {
                'dataDir': '/var/dnode1/data',
                'logDir': '/var/dnode1/log',
                'debugFlag': 135
            }
        for k,v in kwargs.items():
            cfg[k] = v
        dnode = {
           'endpoint': endpoint,
           'config_dir': config_dir,
           'config': cfg
        }
        return dnode

    @classmethod
    def set_taosd_env(cls,
        fqdn    : list      = None,
        version : str       = None,
        spec    : dict      = None,
        config  : dict      = None,
        dnode   : list      = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]

        if not spec :
            version = version if version else '2.4.0.0'
            config = config if config else {'firstEP': '127.0.0.1:6030'}
            dnode = dnode if dnode else [cls.set_taosd_dnode()]
            spec = {
                'version': version,
                'config': config,
                'dnodes': dnode
                }
        else:
            if not cls.key_in_keys("version",spec) or version:
                spec["version"] = version
            if not cls.key_in_keys("config",spec) or config:
                spec["config"] = config
            if not cls.key_in_keys("dnode",spec) or dnode:
                spec["dnode"] = dnode

        return {
            "name"  : cls.TAOSD ,
            "fqdn"  : fqdn ,
            "spec"  : spec
        }

    @classmethod
    def set_taospy_cfg(cls,
        firstEP : str = None,
        logDir : str = None,
        **kwargs
    ):
        firstEP = firstEP if firstEP else "127.0.0.1:6030"
        logDir = logDir if logDir else "/var/dnode1/log"
        cfg = {
            "firstEP" : firstEP,
            "logDir"  : logDir,
        }
        for k,v in kwargs.items():
            cfg[k] = v

        return cfg

    @classmethod
    def set_taospy_env(cls,
        fqdn : list = None,
        spec : dict = None,
        version : str = None,
        client_version : str = None,
        config : dict = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]
        config = config if config else cls.set_taospy_cfg()
        if not spec:
           version = version if version else "2.2.0"
           client_version = client_version if client_version else "2.4.0.0"
           spec = {
               'version' : version,
               'client_version' : client_version,
               'config': config
           }
        else:
            if not cls.key_in_keys("version",spec) or version:
                spec["version"] = version
            if not cls.key_in_keys("client_version",spec) or client_version:
                spec["client_version"] = client_version

        return {
            "name" : cls.TAOSPY,
            "fqdn" : fqdn,
            "spec" : spec
        }

    @classmethod
    def set_taosBenchmark_taoscfg(cls,
        firstEP : str = None,
        logDir : str = None,
        **kwargs
    ):
        firstEP = firstEP if firstEP else "127.0.0.1:6030"
        logDir = logDir if logDir else "/var/taosbenchmark1/log"
        cfg = {
            "firstEP" : firstEP,
            "logDir"  : logDir
        }
        for k,v in kwargs.items():
            cfg[k] = v

        return cfg

    @classmethod
    def set_taosBenchmark_env(cls,
        fqdn : list = None,
        spec : dict = None,
        version : str = None,
        taos_version : str = None,
        config_dir : str = None,
        taos_config : dict = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]
        if not spec:
            version = version if version else "1.0.0"
            taos_version = taos_version if taos_version else "2.4.0.0"
            config_dir = config_dir if config_dir else "/var/taosbenchmark1/config/"
            taos_config = taos_config if taos_config else cls.set_taosBenchmark_taoscfg()
            spec = {
                "version" : version,
                "taos_version" : taos_version,
                "config_dir" : config_dir,
                "taos_config" : taos_config
            }
        else:
            if not cls.key_in_keys("version",spec) or version:
                spec["version"] = version
            if not cls.key_in_keys("taos_version",spec) or taos_version:
                spec["taos_version"] = taos_version
            if not cls.key_in_keys("config_dir",spec) or config_dir:
                spec["config_dir"] = config_dir
            if not cls.key_in_keys("taos_config",spec) or taos_config:
                spec["taos_config"] = taos_config

        return {
            "name" : cls.TAOSBENCHMARK,
            "fqdn" : fqdn,
            "spec" : spec
        }

    @classmethod
    def set_promethus_serv(cls,
        run_dir : str = None,
        config : dict = None,
        evaluation_interval : int = None,
        scrape_interval     : int = None,
        scrape_timeout      : int = None,
        job_name            : str = None,
        targets             : list = None
    ):
        run_dir = run_dir if run_dir else "/opt/prometheus"
        evaluation_interval = evaluation_interval if evaluation_interval else 1
        scrape_interval = scrape_interval if scrape_interval else 3
        scrape_timeout = scrape_timeout if scrape_timeout else 3
        job_name = job_name if job_name else "prometheus"
        targets = targets if targets else ["localhost:9090"]

        if not config:
            config = {
                "global" : {
                    "evaluation_interval" : f"{evaluation_interval}s",
                    "scrape_interval"     : f"{scrape_interval}s",
                    "scrape_timeout"      : f"{scrape_timeout}s"
                },
                "scrape_configs" : {
                    "job_name" : job_name,
                    "targets"  : targets
                }
            }
        else:
            if not os.CLD_STOPPED.key_in_keys("global", config):
                global_dict = {
                    "evaluation_interval" : f"{evaluation_interval}s",
                    "scrape_interval"     : f"{scrape_interval}s",
                    "scrape_timeout"      : f"{scrape_timeout}s"
                }
                config["global"] = global_dict
            elif not cls.key_in_keys("scrape_configs", config):
                scrape_configs = {
                    "job_name" : job_name,
                    "targets"  : targets
                }
                config["scrape_configs"] = scrape_configs
            elif not cls.key_in_keys("evaluation_interval", config):
                config["global"]["evaluation_interval"] = evaluation_interval
            elif not cls.key_in_keys("scrape_interval", config):
                config["global"]["scrape_interval"] = scrape_interval
            elif not cls.key_in_keys("scrape_timeout", config):
                config["global"]["scrape_timeout"] = scrape_timeout
            elif not cls.key_in_keys("job_name", config):
                config["scrape_configs"]["job_name"] = job_name
            elif not cls.key_in_keys("targets", config):
                config["scrape_configs"]["targets"] = targets
            else:
                pass
        server = {
            "run_dir"   : run_dir,
            "config"    : config
        }

        return server

    @classmethod
    def set_promethus_node(cls,
        fqdn : list = None,
        run_dir : str = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]
        run_dir = run_dir if run_dir else "/opt/node_exporter"

        return {
            "fqdn" : fqdn,
            "run_dir" : run_dir
        }

    @classmethod
    def set_promethus_proc(cls,
        fqdn :list = None,
        run_dir : str = None,
        config : dict = None,
        common_process : list = None,
        custom_process : dict = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]
        run_dir = run_dir if run_dir else "/opt/process_exporter"
        common_process = common_process if common_process else ["taosd", "taosadapter", "taosBenchmark"]
        if not isinstance(fqdn, list):
            fqdn = [fqdn]
        if config and not isinstance(config, dict):
            raise "config must be a dict"
        if custom_process and not isinstance(custom_process, dict):
            raise "custom_process must be a dict"
        if not isinstance(common_process, list):
            common_process = [common_process]

        if not config :
            config = {
                "common_process" : common_process
            }
        if not cls.key_in_keys('common_process', config) and not cls.key_in_keys("custom_process", config):
            raise "There is at least one of common_process key and custom_process key"

        if custom_process:
            config["custom_process"] = custom_process

        if config["common_process"] is None and config["custom_process"] is None:
            raise "both config['common_process'] and config['custom_process'] can not none"

        if cls.key_in_keys("custom_process", config):
            if len(fqdn) < len(config["custom_process"].keys()):
                raise "length of fqdn must be greater than or equal to list of custom_process value"

        proc = {
            "fqdn" : fqdn,
            "run_dir" : run_dir,
            "config" : config
        }

        return proc

    @classmethod
    def set_promethus_env(cls,
        fqdn : list = None,
        spec : dict = None,
        server : dict = None,
        node_exporter : dict = None,
        process_exporter : dict = None
    ):
        fqdn = fqdn if fqdn else ["localhost"]
        if not spec:
            server = server if server else cls.set_promethus_serv()
            node_exporter = node_exporter if node_exporter else cls.set_promethus_node()
            process_exporter = process_exporter if process_exporter else cls.set_promethus_proc()
            spec = {
                "server" : server,
                "node_exporter" : node_exporter,
                "process_exporter" : process_exporter
            }
        else:
            if not cls.key_in_keys("server",spec) or server:
                spec["server"] = server
            if not cls.key_in_keys("node_exporter",spec) or node_exporter:
                spec["node_exporter"] = node_exporter
            if not cls.key_in_keys("process_exporter",spec) or process_exporter:
                spec["process_exporter"] = process_exporter

        return {
            "name" : cls.PROMETHEUS,
            "fqdn" : fqdn,
            "spec" : spec
        }

    @classmethod
    def set_env_file(cls,
        env_dict : list = None,
        path : str      = None,
        filename : str  = None
    ):
        env_dict = env_dict if env_dict else [cls.set_taosd_env()]
        file_dict = {"settings" : env_dict}
        path = path if path else "/tmp"
        filename = filename if filename else "testenv.yaml"
        dict2yaml(file_dict, path, filename)


class InsertFile():
    """
    description:
        可以自定义创建各种insert的json文件以及对taosBenchmark的insert结果文件进行处理

        本类将taosBenchmark的json文件分为主体部分和databases部分，其中
            databases包括dbinfo和stableinfo两部分，
            stbinfo包括column和tag两部分
        每一部分均可自定义设置。

    usage:
        0. 实例化:
            jfile = InsertFile()
        1. 自定义实例属性
            1.1 设置tag、col的格式:
                col = jfile.schemacfg(intcount=2, tscount=1)
                tag = jfile.schemacfg()
            1.2 设置dbinfo:
                db = jfile.setDBinfo(name = "db" , keep=36500)
            1.3 设置stbinfo
                stb1 = jfile.setStbinfo(name="stb1", insert_mode="rest", insert_rows=0)
                stb2 = jfile.setStbinfo()  // 默认情况下，col和tag均只有一个int列(col默认第一个为timestamp列)
                如果有多个stb，分别创建不同的对象接收setStbinfo方法结果即可
            1.4 设置databases,注意，一个 database 内只允许有有一个dbinfo，但可以有多个super_tables
                databases1 = jfile.setDatabases()
                databases2 = jfile.setDatabases(dbinfo=db,super_tables=[stb1, stb2])
            1.5 设置全部的json内容
                json_info = jfile.setJsoninfo()
                json_info = jfile.setJsoninfo(databases=[databases1, databases2])
        2. 生成json文件
            jfile.genBenchmarkJson()
            jfile.create_file(result_dir="/tmp", filename="./insert.json",basecfg=cfg)

        # 中间将json文件put至目标机器上后，可以远程调用命令来执行相关的程序，并将结果文件get至当前机器

        3. 获取运行结果内容，假设结果文件名称为f(单个taosBenchmark运行结果文件),结果文件组为f_list(分布式结果文件组成)
            3.1 提取f文件中的每个线程的执行过程情况:
            jfile.get_thread_process_info(f)
            return :
                {
                    'thread[0]': [3420000.0, 5790000.0...],     # list内为每隔30s线程插入的记录数
                    'thread[1]': [3540000.0, 5900000.0...]
                }
            3.2 提取f文件中每个线程的运行时间(次数)情况:
            jfile.get_thread_run_times(f)
            return :
                {
                    'thread[0]': (125000000, 74876.74),  # tuple内包含两项,线程插入记录数和线程插入速度
                    'thread[1]': (125000000, 74892.71)
                }
            3.3 提取f文件中每个线程汇总数据
            jfile.get_summary_result(f)
            return :
                {
                    'min': 34.1,
                    'avg': 133.44,
                    'p90': 342.05,
                    'p95': 442.48,
                    'p99': 570.66,
                    'max': 811.73,          # min～max 项，对应值的单位为 ms
                    'total_times': 1668.0,  # 单位为 s
                    'rate': 599520.38       # 单位为 records/sec
                }
            3.4 提取f文件中所有信息
            jfile.get_file_total_res(f)
            3.5 对应的存在一组方法来提取一组结果文件中的结果
                jfile.get_dt_thread_proc(f_list)
                jfile.get_dt_thread_times(f_list)
                jfile.get_dt_total_summary(f_list)
                jfile.get_all_res(f_list)
            均返回dict对象，且第一层key值为结果文件名称
            3.6 而且提供了一组获取建表信息的方法
                jfile.get_create_tb_proc_info(f)
                jfile.get_create_tb_summary(f)
                jfile.get_dt_create_proc_info(f_list)
                jfile.get_dt_create_tb_summary(f_list)
                jfile.get_all_create_res(f_list)

    """

    def __init__(self) -> None:
        self.create_json = PerforArgs()

        self._type = "insert"
        self.thread : int   = 1 ,
        self.tinfo  : dict  = collections.defaultdict(dict)
        self.tinfos  : dict  = collections.defaultdict(dict)
        self._resultfile : str = None
        self._res_list:list = []
        self.result_dir : str = None
        self.json_file : str = None

    def set_res_file(self, filename:str):
        self._resultfile = filename
        self._res_list.append(self._resultfile)

    def type_check(func):
        def wrapper(self, **kwargs):
            num_types = ["tinyint", "smallint", "int", "bigint", "utinyint", "usmallint", "uint", "ubigint", "float", "double", "bool", "timestamp"]
            str_types = ["binary", "varchar", "nchar", "json"]
            for k, v in kwargs.items():
                if k.lower() not in num_types and k.lower() not in str_types:
                    return f"args {k} type error, not allowed"
                elif not isinstance(v, (int, list, tuple)):
                    return f"value {v} type error, not allowed"
                elif k.lower() in num_types and not isinstance(v, int):
                    return f"arg {v} takes 1 positional argument must be type int "
                elif isinstance(v, (list, tuple)) and len(v) > 3:
                    return f"arg {v} takes from 1 to 2 positional arguments but more than 2 were given "
                # elif isinstance(v, (list, tuple)) and [False for _ in v if not isinstance(_, int)]:
                    # return f"arg {v} takes from 1 to 2 positional arguments must be type int "
                else:
                    pass
            return func(self, **kwargs)

        return wrapper

    @type_check
    def column_tag_count(self, **column_tag) -> list:
        init_column_tag = []
        for k, v in column_tag.items():
            if re.search(k, "tinyint, smallint, int, bigint, utinyint, usmallint, uint, ubigint, float, double, bool, timestamp", re.IGNORECASE):
                init_column_tag.append({"type": k, "count": v})
            elif re.search(k, "binary, varchar, nchar, json", re.IGNORECASE):
                if isinstance(v, int):
                    init_column_tag.append({"type": k, "count": v, "len": 8})
                elif len(v) == 1:
                    init_column_tag.append({"type": k, "count": v[0], "len": 8})
                else:
                    kv_dict = {"type": k, "count": v[0], "len": v[1]}
                    if len(v) > 2:
                        kv_dict["values"] = v[2]
                    init_column_tag.append(kv_dict)
        return init_column_tag

    def schemacfg(self,
                  tcount: int = 0,
                  scount: int = 0,
                  intcount: int = 0,
                  bcount: int = 0,
                  utcount: int = 0,
                  uscount: int = 0,
                  uintcount: int = 0,
                  ubcount: int = 0,
                  floatcount: int = 0,
                  doublecount: int = 0,
                  binarycount: int = 0,
                  varcharcount: int = 0,
                  ncharcount: int = 0,
                  boolcount: bool = 0,
                  tscount: int = 0,
                  jsoncount: int = 0,
                  specified_elem: str = "col"
                  ):
        """
        description:
            usage:
                schemacfg(intcount=1,scount=2)  --> return {"int":1, smallint=2, float=0 ,....}
                suggest use this function as a arg in column_tag_count() , like column_tag_count(**schemacfg(intcount=1,scount=2))
        param {
            intcount    : num of int type columns/tags ,
            floatcount  : num of float type columns/tags ,
            bcount      : num of bigint type columns/tags ,
            tcount      : num of tinyint type columns/tags ,
            scount      : num of smallint type columns/tags ,
            uintcount    : num of int unsigned type columns/tags ,
            ubcount      : num of bigint unsigned type columns/tags ,
            utcount      : num of tinyint unsigned type columns/tags ,
            uscount      : num of smallint unsigned type columns/tags ,
            doublecount : num of double type columns/tags ,
            binarycount : num of binary type columns/tags ,
            varcharcount : num of varchar type columns/tags ,
            ncharcount  : num of nachr type columns/tags ,
            boolcount     : num of bool type columns/tags
            tscount     : num of timestamp type columns/tags
            }
        return {
            a dict without type check
        }
        """
        if specified_elem == "col":
            col_tag = {
                "INT": intcount,
                "TINYINT": tcount,
                "DOUBLE": doublecount,
                "VARCHAR": varcharcount,
                "NCHAR": ncharcount,
                "SMALLINT": scount,
                "BIGINT": bcount,
                "UTINYINT": utcount,
                "USMALLINT": uscount,
                "UINT": uintcount,
                "UBIGINT": ubcount,
                "FLOAT": floatcount,
                "BINARY": binarycount,
                "BOOL": boolcount,
                "TIMESTAMP": tscount,
                "JSON": jsoncount
            }
        elif specified_elem == "tag":
            col_tag = {
                "TINYINT": tcount,
                "VARCHAR": varcharcount,
                "INT": intcount,
                "DOUBLE": doublecount,
                "NCHAR": ncharcount,
                "SMALLINT": scount,
                "BIGINT": bcount,
                "UTINYINT": utcount,
                "USMALLINT": uscount,
                "UINT": uintcount,
                "UBIGINT": ubcount,
                "FLOAT": floatcount,
                "BINARY": binarycount,
                "BOOL": boolcount,
                "TIMESTAMP": tscount,
                "JSON": jsoncount
            }
        # col_tag = {
        #     "TINYINT": tcount,
        #     "SMALLINT": scount,
        #     "INT": intcount,
        #     "BIGINT": bcount,
        #     "UTINYINT": utcount,
        #     "USMALLINT": uscount,
        #     "UINT": uintcount,
        #     "UBIGINT": ubcount,
        #     "FLOAT": floatcount,
        #     "DOUBLE": doublecount,
        #     "BINARY": binarycount,
        #     "VARCHAR": varcharcount,
        #     "NCHAR": ncharcount,
        #     "BOOL": boolcount,
        #     "TIMESTAMP": tscount,
        #     "JSON": jsoncount
        # }
        return self.column_tag_count(**col_tag)

    def setDBinfo(self,
                  name: str = None,
                  drop: str = None,
                  replica: int = 1,
                  duration: int = 10,
                  precision: str = "ms",
                  keep: int = 3650,
                  retentions: str = None,
                  minRows: int = 100,
                  maxRows: int = 4096,
                  comp: int = 2,
                  vgroups: int = 2,
                  stt_trigger: int = None
                  ):
        name = name if name else self.create_json.db_name
        drop = drop if drop else "no"
        res = {
            "name": name,
            "drop": drop,
            "replica": replica,
            "duration": duration,
            "precision": precision,
            "keep": keep,
            "minRows": minRows,
            "maxRows": maxRows,
            "comp": comp,
            "vgroups": vgroups
        }
        if retentions is not None:
            res["retentions"] = retentions
        if stt_trigger is not None:
            res["stt_trigger"] = stt_trigger
        return res

    def setStbinfo(self,
        name : str = None ,
        child_table_exists  : str = "no" ,
        childtable_count    : int = 0 ,
        childtable_prefix   : str = None ,
        auto_create_table   : str = "no",
        escape_character    : str = "no",
        batch_create_tbl_num: int = 10 ,
        data_source         : str = "rand" ,
        insert_mode         : str = "taosc" ,
        rollup              : str = None ,
        interlace_rows      : str = 0 ,
        line_protocol       : str = None,
        tcp_transfer        : str = "no",
        insert_rows         : int = 0 ,
        partial_col_num     : int = 0,
        childtable_limit    : int = 0 ,
        childtable_offset   : int = 0 ,
        rows_per_tbl        : int = 0 ,
        max_sql_len         : int = 1024000 ,
        disorder_ratio      : int = 0 ,
        disorder_range      : int = 1000 ,
        timestamp_step      : int = 10 ,
        keep_trying         : int = 0 ,
        trying_interval     : int = 0 ,
        start_timestamp     : str = f"{datetime.datetime.now():%F %X}" ,
        sample_format       : str = "csv" ,
        sample_file         : str = "./sample.csv" ,
        tags_file           : str = "" ,
        columns             : list = [] ,
        tags                : list = [] ,
        specified_elem      : str = "col"
    ):
        childtable_prefix = childtable_prefix if childtable_prefix else "t_"
        name = name if name else self.create_json.stb_name
        tags = tags if tags else self.schemacfg(specified_elem=specified_elem)
        columns = columns if columns else self.schemacfg()
        stb = {
            "name": name,
            "child_table_exists": child_table_exists,
            "childtable_count": childtable_count,
            "childtable_prefix": childtable_prefix,
            "escape_character": escape_character,
            "auto_create_table": auto_create_table,
            "batch_create_tbl_num": batch_create_tbl_num,
            "data_source": data_source,
            "insert_mode": insert_mode,
            "rollup": rollup,
            "interlace_rows": interlace_rows,
            "line_protocol":line_protocol,
            "tcp_transfer":tcp_transfer,
            "insert_rows": insert_rows,
            "partial_col_num": partial_col_num,
            "childtable_limit": childtable_limit,
            "childtable_offset": childtable_offset,
            "rows_per_tbl": rows_per_tbl,
            "max_sql_len": max_sql_len,
            "disorder_ratio": disorder_ratio,
            "disorder_range": disorder_range,
            "keep_trying": keep_trying,
            "timestamp_step": timestamp_step,
            "trying_interval": trying_interval,
            "start_timestamp": start_timestamp,
            "sample_format": sample_format,
            "sample_file": sample_file,
            "tags_file": tags_file,
            "columns": columns,
            "tags": tags,
        }
        return stb

    def setStreams(self,
        stream_name : str = None ,
        stream_stb  : str = None ,
        trigger_mode    : str = None ,
        watermark   : str = None ,
        source_sql    : str = None,
        drop: str = None
    ):
        stream_dict = {
            "stream_name": stream_name,
            "stream_stb": stream_stb,
            "trigger_mode": trigger_mode,
            "watermark": watermark,
            "source_sql": source_sql,
            "drop": drop
        }
        return stream_dict

    def setDatabases(self,
                     dbinfo: dict = {},
                     super_tables: list = []
                     ):
        dbinfo = dbinfo if dbinfo else self.setDBinfo()
        super_tables = super_tables if super_tables else [self.setStbinfo()]

        database_dict = {
            "dbinfo": dbinfo,
            "super_tables": super_tables
        }
        return database_dict

    def setJsoninfo(self,
       cfgdir                   : str           = "/etc/taos" ,
       host                     : str           = "127.0.0.1" ,
       port                     : int           = 6030 ,
       rest_port                : int           = 6041 ,
       user                     : str           = "root" ,
       password                 : str           = "taosdata" ,
       thread_count             : int           = cpu_count() ,
       create_table_thread_count  : int           = cpu_count() ,
       result_file              : str           = None ,
       confirm_parameter_prompt : str           = "no" ,
       insert_interval          : int           = 0 ,
       num_of_records_per_req   : int           = 100 ,
       prepare_rand             : int           = 10000,
       max_sql_len              : int           = 1024000 ,
       databases                : list          = [],
       streams                  : list          = None,
       chinese                  : str           = "no"
    ):

        """
        description:
            This function provides a way to create insert json file that does not involve database
        param :
            { **kargs : cau use instance.setJsoninfo(attribute=value)}
        return
            a dict that be used to generate json-file
        """

        self.cfgdir = cfgdir if cfgdir else "/etc/taos"
        self.host = host if host else "127.0.0.1"
        self.user = user if user else "root"
        self.port = port if port else 6030
        self.rest_port = rest_port if rest_port else 6041
        self.password = password if password else "taosdata"
        self.thread = thread_count if thread_count else cpu_count()
        self.create_table_thread_count = create_table_thread_count if create_table_thread_count else cpu_count()
        self.result_file = result_file if result_file else ""
        self.confirm_parameter_prompt = confirm_parameter_prompt if confirm_parameter_prompt else "no"
        self.insert_interval = insert_interval if insert_interval else 0
        self.num_of_records_per_req = num_of_records_per_req if num_of_records_per_req else 100
        self.max_sql_len = max_sql_len if max_sql_len else 1024000

        databases = databases if databases else [self.setDatabases()]
        json_info = {
            "filetype": self._type ,
            "cfgdir": self.cfgdir ,
            "host": self.host ,
            "port": self.port ,
            "rest_port": self.rest_port ,
            "user": self.user ,
            "password": self.password ,
            "thread_count": self.thread ,
            "create_table_thread_count": self.create_table_thread_count ,
            "result_file": self.result_file,
            "confirm_parameter_prompt": self.confirm_parameter_prompt,
            "insert_interval": self.insert_interval,
            "num_of_records_per_req": self.num_of_records_per_req,
            "max_sql_len": self.max_sql_len,
            "databases": databases ,
            "insert_interval": insert_interval,
            "prepare_rand": prepare_rand,
            "chinese":chinese
        }
        json_info["streams"] = [streams] if streams is not None else False
        return json_info

    def genBenchmarkJson(self, result_dir:str=None, json_name: str=None, base_config=None,  **kwargs):
        result_dir = result_dir if result_dir else self.result_dir
        json_name = json_name if json_name else self.json_file
        base_config = base_config if  base_config else self.setJsoninfo()
        return gen_benchmark_json(result_dir, json_name, base_config, **kwargs)

    def key_in_keys(self,key, dicts):
        Flag = False
        for k,v in dicts.items():
            if key == k:
                Flag = True
                return Flag
            if isinstance(v, dict):
                Flag = self.key_in_keys(key, v)
        return Flag

    def _get_all_thread_processing_info(self,filename:str) :
        """
        description:
        param  {*}
        return : a str like
            thread[1] 10000
            thread[0] 10000
            thread[2] 10000
            thread[3] 10000
        """

        with open(filename, 'r') as f:
            pat = r"thread.*currently.*"
            all_info = re.findall(pat, f.read())
        sorted(all_info, key=lambda x: (x.split()[0], int(x.split()[8])))

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        if not self.key_in_keys("process", self.tinfos[f_name]):
            for line in all_info:
                thread_name , rows = line.split()[0], int(line.split()[8])
                if "process" in self.tinfo[thread_name]:
                    self.tinfo[thread_name]["process"].append(rows)
                else:
                    self.tinfo[thread_name]["process"] = [rows]
        else:
            for key in self.tinfo.keys():
                if  self.key_in_keys("process", self.tinfo[key]):
                    self.tinfo[key]["process"].clear()
            for line in all_info:
                thread_name , rows = line.split()[0], int(line.split()[8])
                self.tinfo[thread_name]["process"].append(rows)

        self.tinfos[f_name].update(self.tinfo)

    def get_thread_process_info(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_all_thread_processing_info(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "process" in v.keys():
                pro_dict[k] = v.get("process")
        return pro_dict

    def _get_thread_run_times(self,filename:str):
        with open(filename,"r") as f:
            pat = r"thread.*completed total.*"
            times_list = re.findall(pat, f.read())
        sorted(times_list, key=lambda x: x.split()[0])

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        for times in times_list:
            thread_name = times.split()[-9]
            rate = float(times.split()[-2])
            rows = int(times.split()[-3][:-1])
            self.tinfo[thread_name]["times"] = (rows, rate)

        self.tinfos[f_name].update(self.tinfo)

    def get_thread_run_times(self, filename:str=None):
        """
        description:
        param  :
            filename is taosBenchmark resultfile or None,
            when filename is None, auto use self.resultfile
        return :
            return a dict which value is tuple ,like :
                    'thread[num]': (rows, rate)
        """
        file = filename if filename else self._resultfile
        self._get_thread_run_times(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "times" in v.keys():
                pro_dict[k] = v.get("times")
        return pro_dict

    def _get_summary_result(self, filename:str, version=None):
        """
        description:
            different from QueryFile._get_thread_result_info,
            resultfile of insert action has no per thread rate calculation ,
            only total threads launch time summary
        Author: cpwu
        param  {*}
        return {*}
        """
        with open(filename,"r") as f:
            files = f.read()
            pat = r"insert delay,.*|Spent.*insert.*"
            if version == "3.0":
                batch_pat = r'"num_of_records_per_req":.*'
            else:
                batch_pat = r"number of records per req:.*"
            times_list = re.findall(pat, files)
            batch_res = re.findall(batch_pat, files)[0].split()[-1]
        del files

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        res = [re.sub("ms$|ns$|,|ms,|ns,|us$|us,", " ", r) for r in times_list]
        rates, delays = res[0], res[1]
        rows = int(rates.split()[6]) if version == "3.0" else int(rates.split()[9])
        if filename==self._resultfile:
            thread = self.thread
            batch = self.num_of_records_per_req
        else:
            thread = int(rates.split()[8]) if version == "3.0" else int(rates.split()[11])
            if version == "3.0":
                batch = int(batch_res.strip().replace(",", ""))
            else:
                batch = int(batch_res.replace('\x1b[33m', "").replace('\x1b[0m', ""))

        avg_time = float(delays.split()[5])

        time = rows/thread/batch * avg_time/1000
        record_rate = round(thread * batch * 1000 / avg_time, 2)

        self.tinfo["total"]["min"] = float(delays.split()[3])
        self.tinfo["total"]["avg"] = avg_time
        self.tinfo["total"]["p90"] = float(delays.split()[7])
        self.tinfo["total"]["p95"] = float(delays.split()[9])
        self.tinfo["total"]["p99"] = float(delays.split()[11])
        self.tinfo["total"]["max"] = float(delays.split()[13])
        self.tinfo["total"]["total_times"] = time
        self.tinfo["total"]["rate"] = record_rate
        if version == "3.0":
            self.tinfo["total"]["rate"] = float(rates.split()[12])
        self.tinfos[f_name].update(self.tinfo)

    def get_summary_result(self, filename:str=None, version=None):
        file = filename if filename else self._resultfile
        self._get_summary_result(filename=file, version=version)

        return self.tinfo.get("total")

    def get_file_total_res(self, filename:str):
        file = filename if filename else self._resultfile

        self.get_thread_process_info(file)
        self.get_thread_run_times(file)
        self.get_summary_result(file)

        return self.tinfo

    def get_dt_thread_proc(self, file_list:list=None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_thread_process_info(f)

        return pro_dict

    def get_dt_thread_times(self, file_list:list=None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_thread_run_times(f)

        return pro_dict

    def get_dt_total_summary(self, file_list:list = None, version=None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_summary_result(f, version=version)

        return pro_dict

    def get_all_res(self,file_list:list = None):
        f_list = file_list if file_list else self._res_list
        for f in f_list:
            self.get_file_total_res(f)

        return self.tinfos

    def _get_create_table_proc_info(self, filename:str=None):
        with open(filename,"r") as f:
            pat = r"thread.*already created.*"
            all_info = re.findall(pat, f.read())
        sorted(all_info, key=lambda x: (x.split()[0], int(x.split()[3])))

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        if not self.key_in_keys("process", self.tinfos[f_name]):
            for line in all_info:
                thread_name , tables = line.split()[0], int(line.split()[3])
                if "process" in self.tinfo[thread_name]:
                    self.tinfo[thread_name]["process"].append(tables)
                else:
                    self.tinfo[thread_name]["process"] = [tables]
        else:
            for key in self.tinfo.keys():
                if  self.key_in_keys("process", self.tinfo[key]):
                    self.tinfo[key]["process"].clear()
            for line in all_info:
                thread_name , tables = line.split()[0], int(line.split()[3])
                self.tinfo[thread_name]["process"].append(tables)

        self.tinfos[f_name].update(self.tinfo)

    def get_create_tb_proc_info(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_create_table_proc_info(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "process" in v.keys():
                pro_dict[k] = v.get("process")
        return pro_dict

    def _get_create_table_info(self, filename:str=None):
        with open(filename,"r") as f:
            pat = r"Spent.* create.*created"
            total_info = re.findall(pat, f.read())[0]

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        time , tables, threads = total_info.split()[1], total_info.split()[5], total_info.split()[8]
        actual_create = int(total_info.split()[-6]) + int(total_info.split()[-10])
        self.tinfo["total"]["times"]            = float(time)
        self.tinfo["total"]["tables"]           = int(tables)
        self.tinfo["total"]["threads"]          = int(threads)
        self.tinfo["total"]["actual_create"]    = actual_create

        self.tinfos[f_name].update(self.tinfo)

    def get_create_tb_summary(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_create_table_info(filename=file)

        return self.tinfo["total"]

    def get_file_create_res(self, filename:str=None):
        file = filename if filename else self._resultfile

        self.get_create_tb_proc_info(filename=file)
        self.get_create_tb_summary(filename=file)

        return self.tinfo

    def get_dt_create_proc_info(self, file_list:list=None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_create_tb_proc_info(f)

        return pro_dict

    def get_dt_create_tb_summary(self, file_list:list=None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_create_tb_summary(f)

        return pro_dict

    def get_all_create_res(self, file_list:list=None):
        f_list = file_list if file_list else self._res_list
        for f in f_list:
            self.get_file_create_res(f)

        return self.tinfos


class QueryFile():
    """
        generate query-type taosBenchmark file and process result ,
        the method is similar to class InsertFile
    """

    def __init__(self):
        self.create_json = PerforArgs()
        self._type = "query"
        self.thread : int   = 1
        self.tinfo  : dict  = collections.defaultdict(dict)
        self.tinfos  : dict  = collections.defaultdict(dict)
        self._resultfile : str = None
        self._res_list : list = []
        self.result_dir :str = None
        self.json_file :str = None

    def set_res_file(self, filename:str):
        self._resultfile = filename
        self._res_list.append(self._resultfile)

    def setJsoninfo(self,
        cfgdir                   : str           = "/etc/taos" ,
        host                     : str           = "127.0.0.1" ,
        port                     : int           = 6030 ,
        rest_port                : int           = 6041 ,
        user                     : str           = "root" ,
        password                 : str           = "taosdata" ,
        confirm_parameter_prompt : str           = "yes" ,
        databases                : str           = None ,
        query_times              : int           = 1 ,
        query_mode               : str           = "taosc",
        specified_table_query    : dict          = {} ,
        super_table_query        : dict          = {} ,
    ):
        self.cfgdir = cfgdir if cfgdir else "/etc/taos"
        self.host = host if host else self.create_json.fqdn
        self.port = port if port else 6030
        self.rest_port = rest_port if rest_port else 6041
        self.user = user if user else "root"
        self.password = password if password else "taosdata"
        self.confirm_parameter_prompt = confirm_parameter_prompt if confirm_parameter_prompt else "yes"
        self.query_times = query_times if query_times else 1
        self.query_mode = query_mode if query_mode else "taosc"

        databases = databases if databases else self.create_json.db_name
        specified_table_query = specified_table_query if specified_table_query else self.setSpecifyQuery()
        # super_table_query = super_table_query if super_table_query else self.setSupQuery()
        json_info = {
            "filetype"                  : self._type ,
            "cfgdir"                    : self.cfgdir ,
            "host"                      : self.host ,
            "port"                      : self.port ,
            "rest_port"                 : self.rest_port ,
            "user"                      : self.user ,
            "password"                  : self.password ,
            "confirm_parameter_prompt"  : self.confirm_parameter_prompt ,
            "databases"                 : databases ,
            "query_times"               : self.query_times ,
            "query_mode"                : self.query_mode,
            "specified_table_query"     : specified_table_query ,
            "super_table_query"         : super_table_query ,
        }
        return json_info

    def setSpecifyQuery(self,
        query_interval  : int   = 1 ,
        concurrent      : int   = None,
        sqls            : list  = []
    ):
        concurrent =  concurrent if concurrent else self.thread
        sqls = sqls if sqls else [self.setSqlInfo()]
        specified_table_query = {
            "query_interval": query_interval,
            "concurrent": concurrent,
            "sqls": sqls
        }
        return specified_table_query

    def setSqlInfo(self,
        sql     : str   = None,
        result  : str   = None
    ):
        result = result if result else ""

        sql = sql if sql else ""

        sql_info = {
            "sql": sql,
            "result": result,
        }
        return sql_info

    def setSupQuery(self,
                    stblname: str = None,
                    query_interval: int = 1,
                    threads: int = 1,
                    sqls: list = []
                    ):
        stblname = stblname if stblname else self.create_json.stb_name
        sqls = sqls if sqls else [self.setSqlInfo()]
        sup_info = {
            "stblname": stblname,
            "query_interval": query_interval,
            "threads": threads,
            "sqls": sqls
        }
        return sup_info

    def genBenchmarkJson(self, result_dir:str=None,json_name: str=None, base_config=None,  **kwargs):
        result_dir = result_dir if result_dir else self.result_dir
        json_name = json_name if json_name else self.json_file
        base_config = base_config if  base_config else self.setJsoninfo()
        return gen_benchmark_json(result_dir, json_name, base_config, **kwargs)

    def key_in_keys(self,key, dicts):
        Flag = False
        for k,v in dicts.items():
            if key == k:
                Flag = True
                break
            if isinstance(v, dict):
                Flag = self.key_in_keys(key, v)
                if Flag:
                    break
        return Flag

    def _get_all_thread_processing_info(self,filename:str) :
        """_summary_

        Args:
            filename (str): result file name

        Returns:
            dict: a defaultdict(dict,{})  whick values is {thread: qps_list} like:
                defaultdict(list,
                {'thread[1]': [72.695154, 74.648005, 74.486758, 75.575292],
                'thread[0]': [72.761816, 74.68133, 74.520085, 75.608618]})
        """

        with open(filename, 'r') as f:
            pat = r"thread.*completed queries.*"
            all_info = re.findall(pat, f.read())
        sorted(all_info, key=lambda x: (x.split()[0], int(x.split()[5][:-1])))

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        if not self.key_in_keys("process", self.tinfos[f_name]):
            for line in all_info:
                thread_name , qps = line.split()[0], float(line.split()[-1])
                if "process" in self.tinfo[thread_name]:
                    self.tinfo[thread_name]["process"].append(qps)
                else:
                    self.tinfo[thread_name]["process"] = [qps]
        else:
            for key in self.tinfo.keys():
                if  self.key_in_keys("process", self.tinfo[key]):
                    self.tinfo[key]["process"].clear()
            for line in all_info:
                thread_name , qps = line.split()[0], float(line.split()[-1])
                self.tinfo[thread_name]["process"].append(qps)

        self.tinfos[f_name].update(self.tinfo)

    def get_thread_process_info(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_all_thread_processing_info(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "process" in v.keys():
                pro_dict[k] = v.get("process")
        return pro_dict

    def _get_thread_run_times(self,filename:str):
        with open(filename,"r") as f:
            pat = r"thread.*query <.*"
            times_list = re.findall(pat, f.read())

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        for times in times_list:
            thread_name, query_times = times.split()[0], int(times.split()[8])
            self.tinfo[thread_name]["times"] = query_times

        self.tinfos[f_name].update(self.tinfo)

    def get_thread_run_times(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_thread_run_times(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "times" in v.keys():
                pro_dict[k] = v.get("times")
        return pro_dict

    def _get_thread_result_info(self, filename:str):
        with open(filename,"r") as f:
            pat = r"thread.*query <.*"
            times_list = re.findall(pat, f.read())
        sorted(times_list, key=lambda x: x.split()[0])

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        res = [re.sub("ms$|ns$|,|ms,|ns,|us$|us,", " ", r) for r in times_list]
        for r in res:
            thread_name = r.split()[0]
            self.tinfo[thread_name]["min"] = float(r.split()[-11])
            self.tinfo[thread_name]["avg"] = float(r.split()[-9])
            self.tinfo[thread_name]["p90"] = float(r.split()[-7])
            self.tinfo[thread_name]["p95"] = float(r.split()[-5])
            self.tinfo[thread_name]["p99"] = float(r.split()[-3])
            self.tinfo[thread_name]["max"] = float(r.split()[-1])

        self.tinfos[f_name].update(self.tinfo)

    def get_thread_result_info(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_thread_result_info(filename=file)

        pro_dict = {}
        for k,v in self.tinfo.items():
            if "min" in v.keys():
                temp = {}
                temp["min"] = v.get("min")
                temp["avg"] = v.get("avg")
                temp["p90"] = v.get("p90")
                temp["p95"] = v.get("p95")
                temp["p99"] = v.get("p99")
                temp["max"] = v.get("max")
                pro_dict[k] = temp

        return pro_dict

    def _get_summary_result(self, filename:str):
        with open(filename,"r") as f:
            pat = r"completed total queries.*"
            total_info = re.findall(pat, f.read())[0]

        f_name = os.path.splitext(basename(filename))[0]
        if f_name not in self.tinfos.keys():
            self.tinfo = collections.defaultdict(dict)
            self.tinfos[f_name] = self.tinfo

        total_times,total_QPS = int(total_info.split()[3][:-1]), float(total_info.split()[-1])
        self.tinfo["total"]["total_times"] = total_times
        self.tinfo["total"]["QPS"] = total_QPS

        self.tinfos[f_name].update(self.tinfo)

    def get_summary_result(self, filename:str=None):
        file = filename if filename else self._resultfile
        self._get_summary_result(filename=file)

        return self.tinfo.get("total")

    def get_file_total_res(self, filename:str):
        file = filename if filename else self._resultfile

        self.get_thread_process_info(file)
        self.get_thread_run_times(file)
        self.get_thread_result_info(file)
        self.get_summary_result(file)

        return self.tinfo

    def get_dt_thread_proc(self, file_list:list=None):
        """_summary_

        Args:
            file_list (list, optional): a list which consist of a series filename,
            the file corresponding to the filename is the result of the taosBenchmark-query

        Returns:
            _type_: a dict of distribute taosBenchmark result
        """
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_thread_process_info(f)

        return pro_dict

    def get_dt_thread_times(self, file_list:list = None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_thread_run_times(f)

        return pro_dict

    def get_dt_thread_summary(self, file_list:list = None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_thread_result_info(f)

        return pro_dict

    def get_dt_total_summary(self, file_list:list = None):
        f_list = file_list if file_list else self._res_list
        pro_dict = {}

        for f in f_list:
            f_name = os.path.splitext(basename(f))[0]
            pro_dict[f_name] = self.get_summary_result(f)

        return pro_dict

    def get_all_res(self,file_list:list = None):
        f_list = file_list if file_list else self._res_list
        for f in f_list:
            self.get_file_total_res(f)

        return self.tinfos


class Press:
    """_Press_
        生成相关的环境压力

    Returns:

    """



class Report:

    """
    description: 生成测试报告
    param {*}
    return {*}
    """


if __name__ == '__main__':
    pass
