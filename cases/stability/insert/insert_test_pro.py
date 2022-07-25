###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
import sys,getopt
import socket
import os
import random
import datetime
import re
import time
import json
import threading
import copy
from Query.queryutil.createdata import *

class TestInsertPro(TDCase):
    host_field_name = "HOST"
    port_field_name = "PORT"
    dbname_field_name = "DBNAME"
    resultfile_field_name = "RESULTFILE"
    childtable_prefix_field_name = "CHILDTABLEPREFIX"
    insert_cfg_file_param = "insert-cfg-file"
    insert_tmpl_file_param = "insert-tmpl-file"
    check_result_enabled_param = "check-result"
    check_performance_param = "check-performance"
    pre_create_db_param = "pre-create-db"
    stable_field_name = "STABLENAME"
    concurrency_param = "concurrency"
    key_param = "key"
    enable_second_round_param = "enable-second-round"

    def init(self):
        self.insert_cfg_file = None
        self.insert_tmpl_file = None
        self.replace_keys = []
        self.check_result_enabled = False
        self.check_performance = False
        self.pre_create_db = False
        self.concurrency = 1
        self.enable_second_round = False
        self.json_config_files = []
        self.result_files = []
        self.threads = []
        self.ret = True
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        self.tdCommon = TDCom(self.tdSql)

    def help(self):
        print("case parameters:")
        print(f"\t--{TestInsertPro.insert_cfg_file_param}")
        print(f"\t--{TestInsertPro.insert_tmpl_file_param}")
        print(f"\t--{TestInsertPro.key_param}")
        print(f"\t--{TestInsertPro.check_result_enabled_param}")
        print(f"\t--{TestInsertPro.check_performance_param}")
        print(f"\t--{TestInsertPro.pre_create_db_param}")
        print(f"\t--{TestInsertPro.concurrency_param}")
        print(f"\t--{TestInsertPro.enable_second_round_param}")

    # parse case parameters
    def parse_case_param(self):
        try:
            if self.case_param is None:
                self.set_error_msg("no case parameter specified")
                return False
            self.logger.debug("case parameters: [{}]".format(self.case_param))
            param_array = self.case_param.split(" ")
            # parse parameters
            opts, args = getopt.getopt(param_array, "h", ["help", f"{TestInsertPro.insert_cfg_file_param}=", f"{TestInsertPro.insert_tmpl_file_param}=", f"{TestInsertPro.key_param}=", f"{TestInsertPro.check_result_enabled_param}", f"{TestInsertPro.check_performance_param}", f"{TestInsertPro.pre_create_db_param}", f"{TestInsertPro.enable_second_round_param}", f"{TestInsertPro.concurrency_param}="])
            self.logger.debug(str(opts))
            for key, val in opts:
                self.logger.debug("key: {} value: {}".format(key, val))
                if key in (f"--{TestInsertPro.insert_cfg_file_param}"):
                    self.insert_cfg_file = val
                elif key in (f"--{TestInsertPro.insert_tmpl_file_param}"):
                    self.insert_tmpl_file = val
                elif key in (f"--{TestInsertPro.key_param}"):
                    self.replace_keys.append(val)
                elif key in (f"--{TestInsertPro.check_result_enabled_param}"):
                    self.check_result_enabled = True
                elif key in (f"--{TestInsertPro.check_performance_param}"):
                    self.check_performance = True
                elif key in (f"--{TestInsertPro.pre_create_db_param}"):
                    self.pre_create_db = True
                elif key in (f"--{TestInsertPro.enable_second_round_param}"):
                    self.enable_second_round = True
                elif key in (f"--{TestInsertPro.concurrency_param}"):
                    self.concurrency = int(val)
                else:
                    self.logger.error("invalid case parameter: {}".format(key))
                    self.set_error_msg("invalid case parameter: {}".format(key))
                    return False
            # check parameters
            if self.insert_tmpl_file is None:
                self.logger.error(f"case parameter {self.insert_tmpl_file_param} not specified")
                self.set_error_msg(f"case parameter {self.insert_tmpl_file_param} not specified")
                return False
            # get full path
            self.insert_tmpl_file = os.path.join(os.environ["TEST_ROOT"], self.insert_tmpl_file)
            # check file existance
            if not os.path.isfile(self.insert_tmpl_file):
                self.logger.error("{} not exist".format(self.insert_tmpl_file))
                self.set_error_msg("{} not exist".format(self.insert_tmpl_file))
                return False
            if not self.insert_cfg_file is None:
                # get full path
                self.insert_cfg_file = os.path.join(os.environ["TEST_ROOT"], self.insert_cfg_file)
                # check file existance
                if not os.path.isfile(self.insert_cfg_file):
                    self.logger.error("{} not exist".format(self.insert_cfg_file))
                    self.set_error_msg("{} not exist".format(self.insert_cfg_file))
                    return False
        except getopt.GetoptError:
            self.logger.error("parameter parse error [{}]".format(self.case_param))
            self.set_error_msg("parameter parse error [{}]".format(self.case_param))
            return False
        return True

    # parse config file
    def parse_config_file(self, config_file, replace_keys):
        # read config file and generate config dict
        config_dict = dict()
        lines = []
        if not config_file is None:
            with open(config_file, 'r') as file: 
                lines = file.readlines()
        if not replace_keys is None:
            for k in replace_keys:
                lines.append(k)
        for line in lines:
            line_stripped = line.strip()
            self.logger.debug(" {}".format(line_stripped))
            if line_stripped == "":
                continue
            if line_stripped.startswith("#"):
                continue
            pos = line_stripped.find("=")
            if pos <= 0:
                continue
            key = line_stripped[0:pos]
            value = line_stripped[pos+1:]
            config_dict[key] = value
        self.logger.debug(str(config_dict))
        return config_dict

    # create tmp dir
    def make_tmp_dir(self):
        self.logger.debug("log dir: {}".format(self.run_log_dir))
        tmp_dir = os.path.join(self.run_log_dir, "tmp")
        os.system("mkdir -p {}".format(tmp_dir))
        return tmp_dir

    # replace configuration
    def replace_config(self, filename, config):
        for key, value in config.items():
            os.system("sed -i \"s/\<{}\>/{}/g\" {}".format(key, value, filename))

    def run_benchmark(self, node, cmd):
        self.logger.debug(f"thread: {node}, {cmd}")
        result = {}
        try:
            result = self.envMgr._remote.cmd2(node, cmd)
            if result.failed:
                # self.logger.error(str(result))
                self.logger.error("cmd [{}] failed on [{}]".format(cmd, node))
                self.logger.error("cmd [{}] exit code: [{}]".format(cmd, result.exited))
                self.set_error_msg("cmd [{}] failed on [{}]".format(cmd, node))
                self.ret = False
            else:
                self.logger.info("cmd [{}] succeed on [{}]".format(cmd, node))
        except Exception as e:
            self.logger.error("cmd [{}] exception on [{}]".format(cmd, node))
            self.set_error_msg("cmd [{}] exception on [{}]".format(cmd, node))
            self.ret = False

    def run(self) -> bool:
        ret = self.parse_case_param()
        if ret == False:
            print("error in case paramters")
            self.help()
            return False
        self.logger.info("CONFIG FILE: %s", self.config_file)
        ret = self.run_with_param(False)
        if ret == False:
            self.logger.error("first round test failed")
            return False
        else:
            self.logger.info("first round test OK")
        if self.enable_second_round:
            ret = self.run_with_param(True)
            if ret == False:
                self.logger.error("second round test failed")
                return False
            else:
                self.logger.info("second round test OK")
        return True

    def run_with_param(self, second_round) -> bool:
        # get taosd host and port
        taosd_nodes = self.get_component_by_name("taosd")
        self.logger.debug(str(taosd_nodes))
        taosd_fqdn = []
        rkeys = copy.deepcopy(self.replace_keys)
        rkeys.insert(0, f"CHILDTABLEPREFIX=stb_")
        rkeys.insert(0, f"STABLENAME=stb")
        rkeys.insert(0, f"DROPENABLE=yes")
        rkeys.insert(0, f"REPLICA=1")
        child_table_flag = "no"
        if second_round:
            child_table_flag = "yes"
        rkeys.insert(0, f"CHILDTABLEEXISTS={child_table_flag}")
        host = ""
        port = ""
        for node in taosd_nodes:
            if (not node["spec"] is None) and (not node["spec"]["dnodes"] is None):
                taosd_fqdn = node["spec"]["dnodes"]
            if (not node["spec"] is None) and (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                host = node["spec"]["config"]["firstEP"].split(":")[0]
                port = node["spec"]["config"]["firstEP"].split(":")[1]
                self.logger.debug("{} : {}".format(host, port))
                rkeys.insert(0, f"{TestInsertPro.host_field_name}={host}")
                rkeys.insert(1, f"{TestInsertPro.port_field_name}={port}")
                break

        self.json_config_files = []
        self.result_files = []
        # read config file and generate config dict
        insert_config_dict = self.parse_config_file(self.insert_cfg_file, rkeys)

        # for second round, set DROPENABLE to "no"
        if second_round:
            insert_config_dict["DROPENABLE"] = "no"

        if (not TestInsertPro.host_field_name in insert_config_dict) or (not TestInsertPro.port_field_name in insert_config_dict):
            self.logger.error("firstEP not specified in env file")

        # create tmp dir
        tmp_dir = self.make_tmp_dir()

        for i in range (self.concurrency):
            insert_json_template_filename = os.path.basename(self.insert_tmpl_file) + f".{i}.{second_round}"
            self.logger.debug("insert json template basename: {}".format(insert_json_template_filename))
            insert_json_file = os.path.join(tmp_dir, insert_json_template_filename)
            self.logger.debug("insert json file: {}".format(insert_json_file))
            # copy query json template to a tmp directory
            os.system("cp -f {} {}".format(self.insert_tmpl_file, insert_json_file))

            insert_config_dict[TestInsertPro.dbname_field_name] = f"db{i}"
            insert_config_dict[TestInsertPro.stable_field_name] = f"stb{i}"
            insert_config_dict[TestInsertPro.childtable_prefix_field_name] = f"stb{i}_"
            insert_config_dict[TestInsertPro.resultfile_field_name] = f"\/tmp\/result_{i}.{second_round}.txt"
            self.envMgr._remote.cmd(insert_config_dict[TestInsertPro.host_field_name], f"rm -rf /tmp/result_{i}.{second_round}.txt")
            # os.system(f"rm -rf /tmp/result_{i}.txt")
            # replace config settings in json template
            self.replace_config(insert_json_file, insert_config_dict)
            os.system("cat {}".format(insert_json_file))

            # put insert json file to host
            self.envMgr._remote.put(insert_config_dict[TestInsertPro.host_field_name], insert_json_file, "/tmp")

            self.result_files.append(insert_config_dict[TestInsertPro.resultfile_field_name])
            self.json_config_files.append(insert_json_file)

        if (not second_round) and self.pre_create_db:
            db_names = []
            for insert_json_file in self.json_config_files:
                # load taosBenchmark json
                benchmark_config = dict()
                with open(insert_json_file, 'r') as file: 
                    benchmark_config = json.load(file)
                for db in benchmark_config["databases"]:
                    vgroups = 0
                    vgroups = db["dbinfo"]["vgroups"]
                    db_name = ""
                    replica = 1
                    if "name" in db["dbinfo"]:
                        db_name = db["dbinfo"]["name"]
                    if "replica" in db["dbinfo"]:
                        replica = db["dbinfo"]["replica"]
                    if "DATABASE_REPLICAS" in os.environ:
                        replica = int(os.environ["DATABASE_REPLICAS"])
                    if not db_name in db_names:
                        db_names.append(db_name)
                        self.tdCommon.createDb(db_name, True, replica=replica)
                        #ret = os.system(f"taos -h {host} -P {port} -s \"drop database if exists {db_name};\"")
                        #if ret != 0:
                        #    self.logger.error(f"drop database {db_name} failed")
                        #    self.set_error_msg(f"drop database {db_name} failed")
                        #    return False
                        #ret = os.system(f"taos -h {host} -P {port} -s \"create database if not exists {db_name} replica {replica} vgroups {vgroups};\"")
                        #if ret != 0:
                        #    self.logger.error(f"create database {db_name} failed")
                        #    self.set_error_msg(f"create database {db_name} failed")
                        #    return False

        # run benchmark insert data
        self.threads = []
        thread_interval = 0.25
        stime = float(self.concurrency) * thread_interval
        for i in range (self.concurrency):
            self.logger.debug(f"command delay {stime}")
            config_file = os.path.join("/tmp", os.path.basename(self.json_config_files[i]))
            cmd = ["ulimit -n 1048576", f"sleep {stime}", f"taosBenchmark -f {config_file}"]
            t = threading.Thread(target=self.run_benchmark, args=(insert_config_dict[TestInsertPro.host_field_name], cmd))
            self.threads.append(t)
            stime = stime - thread_interval
        for t in self.threads:
            time.sleep(thread_interval)
            t.start()
        for t in self.threads:
            t.join()
        if self.ret != True:
            return self.ret

        if self.check_result_enabled:
            for insert_json_file in self.json_config_files:
                self.logger.info("check result: {}".format(insert_json_file))
                # check result
                # load taosBenchmark json
                benchmark_config = dict()
                with open(insert_json_file, 'r') as file: 
                    benchmark_config = json.load(file)
                for db in benchmark_config["databases"]:
                    db_name = db["dbinfo"]["name"]
                    self.logger.debug("db_name: {}".format(db_name))
                    for stb in db["super_tables"]:
                        stb_name = stb["name"]
                        childtable_count = stb["childtable_count"]
                        insert_rows = stb["insert_rows"]
                        self.logger.debug("stb_name: {}".format(stb_name))
                        self.logger.debug("childtable_count: {}".format(childtable_count))
                        self.logger.debug("insert_rows: {}".format(insert_rows))
                        if childtable_count > 0:
                            self.tdSql.query("select count(*) from information_schema.user_tables where db_name = '{}' and stable_name = '{}';".format(db_name, stb_name))
                            self.tdSql.checkData(0, 0, childtable_count)
                        if childtable_count * insert_rows > 0:
                            self.tdSql.query("select count(*) from {}.{};".format(db_name, stb_name))
                            self.tdSql.checkData(0, 0, childtable_count * insert_rows)

                        # Query data check
                        self.tdSql.query("show {}.tables;".format(db_name))
                        stb_child_name = ""
                        i = 0
                        while 1:
                            if self.tdSql.getData(i, 4) == stb_name:
                                stb_child_name = self.tdSql.getData(i, 0)
                                break
                            i = i + 1
                            
                        self.tdSql.query("describe {}.{};".format(db_name,stb_child_name))
                        c1 = self.tdSql.getData(1, 0)
                        c11 = self.tdSql.getData(1, 1)
                        if (insert_rows > 0) and (c11 != "NCHAR") and (c11 != "BINARY") and (c11 != "VARCHAR"):
                            #sum check
                            sum_sql1 = "select sum({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            sum_sql2 = "select sum({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %sum_sql1 ,1,1,'%s' %sum_sql2 ,1,1)
                            #max check
                            max_sql1 = "select max({}) from {}.{};".format(c1 ,db_name, stb_child_name);
                            max_sql2 = "select max({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %max_sql1 ,1,1,'%s' %max_sql2 ,1,1)
                            #min check
                            min_sql1 = "select min({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            min_sql2 = "select min({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %min_sql1 ,1,1,'%s' %min_sql2 ,1,1)
                            #avg check
                            avg_sql1 = "select avg({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            avg_sql2 = "select avg({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %avg_sql1 ,1,1,'%s' %avg_sql2 ,1,1)
                            #first check
                            first_sql1 = "select first({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            first_sql2 = "select first({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %first_sql1 ,1,1,'%s' %first_sql2 ,1,1)
                            #last check
                            last_sql1 = "select last({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            last_sql2 = "select last({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %last_sql1 ,1,1,'%s' %last_sql2 ,1,1)
                            #last_row check
                            last_row_sql1 = "select last_row({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            last_row_sql2 = "select last_row({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %last_row_sql1 ,1,1,'%s' %last_row_sql2 ,1,1)
                            #top_2 check
                            top_sql1 = "select top({},2) from {}.{};".format(c1 , db_name, stb_child_name);
                            top_sql2 = "select top({},2) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %top_sql1 ,1,1,'%s' %top_sql2 ,1,1)
                            #bottom_2 check
                            bottom_sql1 = "select bottom({},2) from {}.{};".format(c1 , db_name, stb_child_name);
                            bottom_sql2 = "select bottom({},2) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %bottom_sql1 ,1,1,'%s' %bottom_sql2 ,1,1)
                            #csum check
                            csum_sql1 = "select csum({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            csum_sql2 = "select csum({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %csum_sql1 ,1,1,'%s' %csum_sql2 ,1,1)
                            #mavg check
                            mavg_sql1 = "select mavg({},2) from {}.{};".format(c1 , db_name, stb_child_name);
                            mavg_sql2 = "select mavg({},2) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %mavg_sql1 ,1,1,'%s' %mavg_sql2 ,1,1)
                            #spread check
                            spread_sql1 = "select spread({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            spread_sql2 = "select spread({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %spread_sql1 ,1,1,'%s' %spread_sql2 ,1,1)
                            #stddev check
                            stddev_sql1 = "select stddev({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            stddev_sql2 = "select stddev({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %stddev_sql1 ,1,1,'%s' %stddev_sql2 ,1,1)
                            #twa check
                            twa_sql1 = "select twa({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            twa_sql2 = "select twa({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %twa_sql1 ,1,1,'%s' %twa_sql2 ,1,1)
                            #abs check
                            abs_sql1 = "select abs({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            abs_sql2 = "select abs({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %abs_sql1 ,1,1,'%s' %abs_sql2 ,1,1)
                            #sin check
                            sin_sql1 = "select sin({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            sin_sql2 = "select sin({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %sin_sql1 ,1,1,'%s' %sin_sql2 ,1,1)
                            #acos check
                            acos_sql1 = "select acos({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            acos_sql2 = "select acos({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %acos_sql1 ,1,1,'%s' %acos_sql2 ,1,1)
                            #ceil check
                            ceil_sql1 = "select ceil({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            ceil_sql2 = "select ceil({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %ceil_sql1 ,1,1,'%s' %ceil_sql2 ,1,1)
                            #log check
                            log_sql1 = "select log({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            log_sql2 = "select log({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %log_sql1 ,1,1,'%s' %log_sql2 ,1,1)
                            #cast check
                            cast_sql1 = "select cast({} as bigint) from {}.{};".format(c1 , db_name, stb_child_name);
                            cast_sql2 = "select cast({} as bigint) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %cast_sql1 ,1,1,'%s' %cast_sql2 ,1,1)
                        elif (insert_rows > 0) and ((c11 != "NCHAR") or (c11 != "BINARY") or (c11 != "VARCHAR")):
                            #char_length check
                            char_length_sql1 = "select char_length({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            char_length_sql2 = "select char_length({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %char_length_sql1 ,1,1,'%s' %char_length_sql2 ,1,1)
                            #length check
                            length_sql1 = "select length({}) from {}.{};".format(c1 ,db_name, stb_child_name);
                            length_sql2 = "select length({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %length_sql1 ,1,1,'%s' %length_sql2 ,1,1)
                            #lower check
                            lower_sql1 = "select lower({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            lower_sql2 = "select lower({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %lower_sql1 ,1,1,'%s' %lower_sql2 ,1,1)
                            #lower check
                            lower_sql1 = "select lower({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            lower_sql2 = "select lower({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %lower_sql1 ,1,1,'%s' %lower_sql2 ,1,1)
                            #ltrim check
                            ltrim_sql1 = "select ltrim({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            ltrim_sql2 = "select ltrim({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %ltrim_sql1 ,1,1,'%s' %ltrim_sql2 ,1,1)
                            #rtrim check
                            rtrim_sql1 = "select rtrim({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            rtrim_sql2 = "select rtrim({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %rtrim_sql1 ,1,1,'%s' %rtrim_sql2 ,1,1)
                            #upper check
                            upper_sql1 = "select upper({}) from {}.{};".format(c1 , db_name, stb_child_name);
                            upper_sql2 = "select upper({}) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %upper_sql1 ,1,1,'%s' %upper_sql2 ,1,1)
                            #substr check
                            substr_sql1 = "select substr({},2) from {}.{};".format(c1 , db_name, stb_child_name);
                            substr_sql2 = "select substr({},2) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %substr_sql1 ,1,1,'%s' %substr_sql2 ,1,1)
                            #concat check
                            concat_sql1 = "select concat({},{}) from {}.{};".format(c1 , c1 ,db_name, stb_child_name);
                            concat_sql2 = "select concat({},{}) from {}.{} where tbname = '{}';".format(c1 , c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %concat_sql1 ,1,1,'%s' %concat_sql2 ,1,1)
                            #concat_ws check
                            concat_ws_sql1 = "select concat_ws('{}',{},{}) from {}.{};".format(c1 , c1 ,c1 ,db_name, stb_child_name);
                            concat_ws_sql2 = "select concat_ws('{}',{},{}) from {}.{} where tbname = '{}';".format(c1 , c1 , c1 ,db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %concat_ws_sql1 ,1,1,'%s' %concat_ws_sql2 ,1,1)
                            #cast check
                            cast_sql1 = "select cast({} as NCHAR(1000)) from {}.{};".format(c1 , db_name, stb_child_name);
                            cast_sql2 = "select cast({} as NCHAR(1000)) from {}.{} where tbname = '{}';".format(c1 , db_name, stb_name, stb_child_name)
                            self.tdCreateData.dataequal('%s' %cast_sql1 ,1,1,'%s' %cast_sql2 ,1,1)

                        if (not self.enable_second_round) or second_round:
                            self.logger.debug("check delete")
                            # BUG: delete with replica 3 never quit
                            self.tdSql.execute("delete from {}.{};".format(db_name, stb_name))
                            self.tdSql.execute("flush database {};".format(db_name))
                            self.tdSql.execute("reset query cache;")
                            self.tdSql.query("select * from {}.{};".format(db_name, stb_name))
                            self.tdSql.checkRow(0)

        # get performance result
        if not second_round:
            os.system("echo @#@#@#@#@#@#@#@#@#@#")
            for i in range (self.concurrency):
                result_file = f"/tmp/result_{i}.{second_round}.txt"
                local_result_file = f"{self.run_log_dir}/tmp/result_{i}.{second_round}.txt"
                self.envMgr._remote.get(insert_config_dict[TestInsertPro.host_field_name], result_file, f"{self.run_log_dir}/tmp")
                # check file
                if os.path.exists(local_result_file):
                    os.system(f"cat {local_result_file}")
                else:
                    self.logger.error(f"result file {local_result_file} not exist")
                    self.set_error_msg(f"result file {local_result_file} not exist")
                    self.ret = False
            if self.ret != True:
                return self.ret

            # analyze result
            if self.check_performance:
                time_elapsed = 0.0
                insert_rows = 0
                total_threads = 0
                insert_speed = 0.0
                for i in range (self.concurrency):
                    result_file = f"{self.run_log_dir}/tmp/result_{i}.{second_round}.txt"
                    with open(result_file, 'r') as file:
                        insert_rows_found = False
                        while 1:
                            line = file.readline()
                            if not line:
                                break
                            # self.logger.debug(line)
                            if line.find("insert rows:") >= 0:
                                insert_rows_found = True
                                a = self.get_number_after(line, "insert rows:")
                                self.logger.debug(f"insert rows: {a}")
                                if a == "":
                                    self.logger.error(f"insert rows: {a}")
                                    self.set_error_msg(f"error insert rows: {a}")
                                    self.ret = False
                                    break
                                a_int = int(a)
                                insert_rows += a_int
                                b = self.get_number_before(line, "thread(s)")
                                if b == "":
                                    self.logger.error(f"threads: {b}")
                                    self.set_error_msg(f"error threeds: {b}")
                                    self.ret = False
                                    break
                                b_int = int(b)
                                total_threads += b_int
                                self.logger.debug(f"threads: {b}")
                                c = self.get_number_before(line, "records\/second")
                                self.logger.debug(f"speed: {c}")
                                if c == "":
                                    self.logger.error(f"speed: {c}")
                                    self.set_error_msg(f"error speed: {c}")
                                    self.ret = False
                                    break
                                c_float = float(c)
                                insert_speed += c_float
                                d = self.get_number_after(line, "Spent")
                                self.logger.debug(f"Spent: {d}")
                                if d == "":
                                    self.logger.error(f"Spent: {d}")
                                    self.set_error_msg(f"error Spent: {d}")
                                    self.ret = False
                                    break
                                d_float = float(d)
                                time_elapsed = max(time_elapsed, d_float)
                        if insert_rows_found == False:
                            self.logger.error(f"key word insert row not found in {local_result_file}")
                            self.set_error_msg(f"key word insert row not found in {local_result_file}")
                            self.ret = False
                if self.ret:
                    taosd_count = len(taosd_fqdn)
                    vgroups = 0
                    insert_mode = ""
                    insert_json_file = self.json_config_files[0]
                    # get VGROUPS, INSERT MODE
                    # load taosBenchmark json
                    benchmark_config = dict()
                    with open(insert_json_file, 'r') as file: 
                        benchmark_config = json.load(file)
                    db = benchmark_config["databases"][0]
                    vgroups = db["dbinfo"]["vgroups"]
                    stb = db["super_tables"][0]
                    insert_mode = stb["insert_mode"]
                    self.logger.debug("vgroups: {}, insert mode:".format(vgroups, insert_mode))
                    os.system("echo @@##@@##  time spent: {:.1f}, insert rows: {}, total threads: {}, insert speed: {:.0f}, taosd count: {}, vgroups: {}, insert mode: {}".format(time_elapsed, insert_rows, total_threads, insert_speed, taosd_count, vgroups, insert_mode))
        return self.ret

    def get_number_after(self, line, keyword):
        cmd = "echo \"{}\"|grep -o \"{}.*\"|sed \"s/^{}//\"|awk '{{print $1}}'|grep -o \"[0-9.]*\"".format(line, keyword, keyword)
        result = os.popen(cmd)
        return result.readline().strip()

    def get_number_before(self, line, keyword):
        cmd = "echo \"{}\"|grep -o \".*{}\"|sed \"s/{}$//\"|awk '{{print $NF}}'|grep -o \"[0-9.]*\"".format(line, keyword, keyword)
        result = os.popen(cmd)
        return result.readline().strip()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            3mnodes insert
        """
        return case_description

    def author(self) -> str:
        return "tangfz"

    def tags(self):
        return T.Write.Stable

