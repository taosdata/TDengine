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

class TestQuery(TDCase):
    host_field_name="HOST"
    port_field_name="PORT"
    insert_cfg_file_param="insert-cfg-file"
    insert_tmpl_file_param="insert-tmpl-file"
    query_cfg_file_param="query-cfg-file"
    query_tmpl_file_param="query-tmpl-file"

    def init(self):
        self.insert_cfg_file = None
        self.insert_tmpl_file = None
        self.query_cfg_file = None
        self.query_tmpl_file = None

    def help(self):
        print("case parameters:")
        print(f"\t--{TestQuery.insert_cfg_file_param}")
        print(f"\t--{TestQuery.insert_tmpl_file_param}")
        print(f"\t--{TestQuery.query_cfg_file_param}")
        print(f"\t--{TestQuery.query_tmpl_file_param}")

    # parse case parameters
    def parse_case_param(self):
        try:
            if self.case_param is None:
                return False
            self.logger.debug("case parameters: [{}]".format(self.case_param))
            param_array = self.case_param.split(" ")
            # parse parameters
            opts, args = getopt.getopt(param_array, "h", ["help", f"{TestQuery.insert_cfg_file_param}=", f"{TestQuery.insert_tmpl_file_param}=", f"{TestQuery.query_cfg_file_param}=", f"{TestQuery.query_tmpl_file_param}="])
            self.logger.debug(str(opts))
            for key, val in opts:
                self.logger.debug("key: {} value: {}".format(key, val))
                if key in (f"--{TestQuery.insert_cfg_file_param}"):
                    self.insert_cfg_file = val
                elif key in (f"--{TestQuery.insert_tmpl_file_param}"):
                    self.insert_tmpl_file = val
                elif key in (f"--{TestQuery.query_cfg_file_param}"):
                    self.query_cfg_file = val
                elif key in (f"--{TestQuery.query_tmpl_file_param}"):
                    self.query_tmpl_file = val
                else:
                    self.logger.error("invalid case parameter: {}".format(key))
                    return False
            # check parameters
            if self.insert_cfg_file is None:
                self.logger.error(f"case parameter {self.insert_cfg_file_param} not specified")
                return False
            if self.insert_tmpl_file is None:
                self.logger.error(f"case parameter {self.insert_tmpl_file_param} not specified")
                return False
            if self.query_cfg_file is None:
                self.logger.error(f"case parameter {self.query_cfg_file_param} not specified")
                return False
            if self.query_tmpl_file is None:
                self.logger.error(f"case parameter {self.query_tmpl_file_param} not specified")
                return False
            # get full path
            self.insert_cfg_file = os.path.join(os.environ["TEST_ROOT"], self.insert_cfg_file)
            self.insert_tmpl_file = os.path.join(os.environ["TEST_ROOT"], self.insert_tmpl_file)
            self.query_cfg_file = os.path.join(os.environ["TEST_ROOT"], self.query_cfg_file)
            self.query_tmpl_file = os.path.join(os.environ["TEST_ROOT"], self.query_tmpl_file)
            # check file existance
            if not os.path.isfile(self.insert_cfg_file):
                self.logger.error("{} not exist".format(self.insert_cfg_file))
                return False
            if not os.path.isfile(self.insert_tmpl_file):
                self.logger.error("{} not exist".format(self.insert_tmpl_file))
                return False
            if not os.path.isfile(self.query_cfg_file):
                self.logger.error("{} not exist".format(self.query_cfg_file))
                return False
            if not os.path.isfile(self.query_tmpl_file):
                self.logger.error("{} not exist".format(self.query_tmpl_file))
                return False
        except getopt.GetoptError:
            self.logger.error("parameter parse error [{}]".format(self.case_param))
            return False
        return True

    # parse config file
    def parse_config_file(self, config_file):
        # read config file and generate config dict
        config_dict = dict()
        lines = []
        with open(config_file, 'r') as file: 
            lines = file.readlines()
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

    def run(self) -> bool:
        ret = self.parse_case_param()
        if ret == False:
            print("error in case paramters")
            self.help()
            return False
        self.logger.info("CONFIG FILE: %s", self.config_file)
        # read config file and generate config dict
        insert_config_dict = self.parse_config_file(self.insert_cfg_file)
        query_config_dict = self.parse_config_file(self.query_cfg_file)

        # get taosd host and port
        taosd_nodes = self.get_component_by_name("taosd")
        self.logger.debug(str(taosd_nodes))
        for node in taosd_nodes:
            if (not node["spec"] is None) and (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                host = node["spec"]["config"]["firstEP"].split(":")[0]
                port = node["spec"]["config"]["firstEP"].split(":")[1]
                self.logger.debug("{} : {}".format(host, port))
                insert_config_dict[TestQuery.host_field_name] = host
                insert_config_dict[TestQuery.port_field_name] = port
                query_config_dict[TestQuery.host_field_name] = host
                query_config_dict[TestQuery.port_field_name] = port
                break
        if (not TestQuery.host_field_name in insert_config_dict) or (not TestQuery.port_field_name in insert_config_dict):
            self.logger.error("firstEP not specified in env file")

        # create tmp dir
        tmp_dir = self.make_tmp_dir()

        insert_json_template_filename = os.path.basename(self.insert_tmpl_file)
        self.logger.debug("insert json template basename: {}".format(insert_json_template_filename))
        insert_json_file = os.path.join(tmp_dir, insert_json_template_filename)
        self.logger.debug("insert json file: {}".format(insert_json_file))
        # copy query json template to a tmp directory
        os.system("cp -f {} {}".format(self.insert_tmpl_file, insert_json_file))

        query_json_template_filename = os.path.basename(self.query_tmpl_file)
        self.logger.debug("query json template basename: {}".format(query_json_template_filename))
        query_json_file = os.path.join(tmp_dir, query_json_template_filename)
        self.logger.debug("query json file: {}".format(query_json_file))
        # copy query json template to a tmp directory
        os.system("cp -f {} {}".format(self.query_tmpl_file, query_json_file))

        # replace config settings in json template
        self.replace_config(insert_json_file, insert_config_dict)
        os.system("cat {}".format(insert_json_file))
        self.replace_config(query_json_file, query_config_dict)
        os.system("cat {}".format(query_json_file))

        # put insert json file to host
        self.envMgr._remote.put(insert_config_dict[TestQuery.host_field_name], insert_json_file, "/tmp")

        # run benchmark insert data
        cmd = ["ulimit -n 1048576", "taosBenchmark -f /tmp/" + insert_json_template_filename]
        result = self.envMgr._remote.cmd2(insert_config_dict[TestQuery.host_field_name], cmd)
        if result.failed:
            # self.logger.error(str(result))
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, insert_config_dict[TestQuery.host_field_name]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, insert_config_dict[TestQuery.host_field_name]))

        # check result
        # load taosBenchmark json
        ret = True
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
                    self.tdSql.query("select count(tbname) from {}.{};".format(db_name, stb_name))
                    ret = self.tdSql.checkData(0, 0, childtable_count)
                if childtable_count * insert_rows > 0:
                    self.tdSql.query("select count(*) from {}.{};".format(db_name, stb_name))
                    ret = self.tdSql.checkData(0, 0, childtable_count * insert_rows)

        # put query json file to host
        self.envMgr._remote.put(query_config_dict[TestQuery.host_field_name], query_json_file, "/tmp")
        # run benchmark query data
        cmd = ["ulimit -n 1048576", "taosBenchmark -f /tmp/" + query_json_template_filename]
        result = self.envMgr._remote.cmd2(query_config_dict[TestQuery.host_field_name], cmd)
        if result.failed:
            # self.logger.error(str(result))
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, query_config_dict[TestQuery.host_field_name]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, query_config_dict[TestQuery.host_field_name]))

        return True

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            stability insert
        """
        return case_description

    def author(self) -> str:
        return "tangfz"

    def tags(self):
        return T.Write.Stable

