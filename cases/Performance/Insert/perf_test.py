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
import sys,getopt
import socket
import os
import random
import datetime
import re
import time
import json
import threading

class TestPerf(TDCase):
    host_field_name = "HOST"
    port_field_name = "PORT"
    dbname_field_name = "DBNAME"
    resultfile_field_name = "RESULTFILE"
    insert_cfg_file_param = "insert-cfg-file"
    insert_tmpl_file_param = "insert-tmpl-file"
    check_result_enabled_param = "check-result"
    concurrency_param = "concurrency"
    key_param = "key"

    def init(self):
        self.insert_cfg_file = None
        self.insert_tmpl_file = None
        self.replace_keys = []
        self.check_result_enabled = False
        self.concurrency = 1
        self.json_config_files = []
        self.result_files = []
        self.threads = []
        self.ret = True

    def help(self):
        print("case parameters:")
        print(f"\t--{TestPerf.insert_cfg_file_param}")
        print(f"\t--{TestPerf.insert_tmpl_file_param}")
        print(f"\t--{TestPerf.key_param}")
        print(f"\t--{TestPerf.check_result_enabled_param}")
        print(f"\t--{TestPerf.concurrency_param}")

    # parse case parameters
    def parse_case_param(self):
        try:
            if self.case_param is None:
                self.set_error_msg("no case parameter specified")
                return False
            self.logger.debug("case parameters: [{}]".format(self.case_param))
            param_array = self.case_param.split(" ")
            # parse parameters
            opts, args = getopt.getopt(param_array, "h", ["help", f"{TestPerf.insert_cfg_file_param}=", f"{TestPerf.insert_tmpl_file_param}=", f"{TestPerf.key_param}=", f"{TestPerf.check_result_enabled_param}", f"{TestPerf.concurrency_param}="])
            self.logger.debug(str(opts))
            for key, val in opts:
                self.logger.debug("key: {} value: {}".format(key, val))
                if key in (f"--{TestPerf.insert_cfg_file_param}"):
                    self.insert_cfg_file = val
                elif key in (f"--{TestPerf.insert_tmpl_file_param}"):
                    self.insert_tmpl_file = val
                elif key in (f"--{TestPerf.key_param}"):
                    self.replace_keys.append(val)
                elif key in (f"--{TestPerf.check_result_enabled_param}"):
                    self.check_result_enabled = True
                elif key in (f"--{TestPerf.concurrency_param}"):
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

    def run_benchmark(self, node, config_file):
        self.logger.debug(f"thread: {node}, {config_file}")
        cmd = ["ulimit -n 1048576", "sleep 5", "taosBenchmark -f " + config_file]
        result = self.envMgr._remote.cmd2(node, cmd)
        if result.failed:
            # self.logger.error(str(result))
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, node))
            self.logger.error("cmd [{}] exit code: [{}]".format(cmd, result.exited))
            self.set_error_msg("cmd [{}] failed on [{}]".format(cmd, node))
            self.ret = False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, node))

    def run(self) -> bool:
        ret = self.parse_case_param()
        if ret == False:
            print("error in case paramters")
            self.help()
            return False
        self.logger.info("CONFIG FILE: %s", self.config_file)

        # get taosd host and port
        taosd_nodes = self.get_component_by_name("taosd")
        self.logger.debug(str(taosd_nodes))
        taosd_fqdn = []
        self.replace_keys.insert(0, f"CHILDTABLEPREFIX=stb_")
        self.replace_keys.insert(0, f"STABLENAME=stb")
        self.replace_keys.insert(0, f"DROPENABLE=yes")
        for node in taosd_nodes:
            if (not node["spec"] is None) and (not node["spec"]["dnodes"] is None):
                taosd_fqdn = node["spec"]["dnodes"]
            if (not node["spec"] is None) and (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                host = node["spec"]["config"]["firstEP"].split(":")[0]
                port = node["spec"]["config"]["firstEP"].split(":")[1]
                self.logger.debug("{} : {}".format(host, port))
                self.replace_keys.insert(0, f"{TestPerf.host_field_name}={host}")
                self.replace_keys.insert(1, f"{TestPerf.port_field_name}={port}")
                break

        # read config file and generate config dict
        insert_config_dict = self.parse_config_file(self.insert_cfg_file, self.replace_keys)

        if (not TestPerf.host_field_name in insert_config_dict) or (not TestPerf.port_field_name in insert_config_dict):
            self.logger.error("firstEP not specified in env file")

        # create tmp dir
        tmp_dir = self.make_tmp_dir()

        for i in range (self.concurrency):
            insert_json_template_filename = os.path.basename(self.insert_tmpl_file) + f".{i}"
            self.logger.debug("insert json template basename: {}".format(insert_json_template_filename))
            insert_json_file = os.path.join(tmp_dir, insert_json_template_filename)
            self.logger.debug("insert json file: {}".format(insert_json_file))
            # copy query json template to a tmp directory
            os.system("cp -f {} {}".format(self.insert_tmpl_file, insert_json_file))

            insert_config_dict[TestPerf.dbname_field_name] = f"db{i}"
            insert_config_dict[TestPerf.resultfile_field_name] = f"\/tmp\/result_{i}.txt"
            self.envMgr._remote.cmd(insert_config_dict[TestPerf.host_field_name], f"rm -rf /tmp/result_{i}.txt")
            # os.system(f"rm -rf /tmp/result_{i}.txt")
            # replace config settings in json template
            self.replace_config(insert_json_file, insert_config_dict)
            os.system("cat {}".format(insert_json_file))

            # put insert json file to host
            self.envMgr._remote.put(insert_config_dict[TestPerf.host_field_name], insert_json_file, "/tmp")

            self.result_files.append(insert_config_dict[TestPerf.resultfile_field_name])
            self.json_config_files.append(insert_json_file)

        # run benchmark insert data
        for i in range (self.concurrency):
            t = threading.Thread(target=self.run_benchmark, args=(insert_config_dict[TestPerf.host_field_name], os.path.join("/tmp", os.path.basename(self.json_config_files[i]))))
            self.threads.append(t)
        for t in self.threads:
            t.start()
        for t in self.threads:
            t.join()

        if self.check_result_enabled:
            for insert_json_file in self.json_config_files:
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

        # get performance result
        os.system("echo @#@#@#@#@#@#@#@#@#@#")
        for i in range (self.concurrency):
            result_file = f"/tmp/result_{i}.txt"
            local_result_file = f"{self.run_log_dir}/tmp/result_{i}.txt"
            self.envMgr._remote.get(insert_config_dict[TestPerf.host_field_name], result_file, f"{self.run_log_dir}/tmp")
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
        time_elapsed = 0.0
        insert_rows = 0
        total_threads = 0
        insert_speed = 0.0
        for i in range (self.concurrency):
            result_file = f"{self.run_log_dir}/tmp/result_{i}.txt"
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
            performance insert
        """
        return case_description

    def author(self) -> str:
        return "tangfz"

    def tags(self):
        return T.Write.Stable

