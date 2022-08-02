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
import socket
import os
import random
import datetime
import re
import time
import json
from Query.queryutil.createdata import *

class TestInsert(TDCase):
    key_json_template="JSON_TEMPLATE"

    def init(self):
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        #pass

    # parse config file
    def parse_config_file(self):
        # read config file and generate config dict
        config_dict = dict()
        lines = []
        with open(self.config_file, 'r') as file: 
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
            if key != TestInsert.key_json_template:
                os.system("sed -i \"s/\<{}\>/{}/g\" {}".format(key, value, filename))

    def run(self) -> bool:
        self.logger.info("CONFIG FILE: %s", self.config_file)
        # read config file and generate config dict
        config_dict = self.parse_config_file()

        # get taosbenchmark json template
        if not self.case_param is None:
            self.logger.debug("case parameter: {}".format(self.case_param))
            # update json template with case parameter
            config_dict[TestInsert.key_json_template] = self.case_param
        if not TestInsert.key_json_template in config_dict:
            self.logger.error("json template file not specified")
            self.set_error_msg("json template file not specified")
            return False

        # get full path of json template
        if not config_dict[TestInsert.key_json_template].startswith("/"):
            config_dict[TestInsert.key_json_template] = os.path.join(os.environ["TEST_ROOT"], config_dict[TestInsert.key_json_template])
        self.logger.debug("json template: {}".format(config_dict[TestInsert.key_json_template]))
        if not os.path.isfile(config_dict[TestInsert.key_json_template]):
            self.logger.error("{} not exist".format(config_dict[TestInsert.key_json_template]))
            self.set_error_msg("{} not exist".format(config_dict[TestInsert.key_json_template]))
            return False

        # create tmp dir
        tmp_dir = self.make_tmp_dir()
        json_template_filename = os.path.basename(config_dict[TestInsert.key_json_template])
        self.logger.debug("json template basename: {}".format(json_template_filename))
        json_file = os.path.join(tmp_dir, json_template_filename)
        self.logger.debug("json file: {}".format(json_file))
        # copy json template to a tmp directory
        os.system("cp -f {} {}".format(config_dict[TestInsert.key_json_template], json_file))

        # get taosd host and port
        taosd_nodes = self.get_component_by_name("taosd")
        self.logger.debug(str(taosd_nodes))
        for node in taosd_nodes:
            if (not node["spec"] is None) and (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                host = node["spec"]["config"]["firstEP"].split(":")[0]
                port = node["spec"]["config"]["firstEP"].split(":")[1]
                self.logger.debug("{} : {}".format(host, port))
                config_dict["HOST"] = host
                config_dict["PORT"] = port
                break
        if (not "HOST" in config_dict) or (not "PORT" in config_dict):
            self.logger.error("firstEP not specified in env file")

        # replace config settings in json template
        self.replace_config(json_file, config_dict)
        os.system("cat {}".format(json_file))

        # put json file to host
        self.envMgr._remote.put(config_dict["HOST"], json_file, "/tmp")

        # run benchmark
        cmd = ["ulimit -n 1048576", "taosBenchmark -f /tmp/" + json_template_filename]
        result = self.envMgr._remote.cmd2(config_dict["HOST"], cmd)
        if result.failed:
            # self.logger.error(str(result))
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, config_dict["HOST"]))
            self.logger.error("cmd [{}] exit code: [{}]".format(cmd, result.exited))
            self.set_error_msg("cmd [{}] failed on [{}]".format(cmd, config_dict["HOST"]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, config_dict["HOST"]))

        # check result
        # load taosBenchmark json
        ret = True
        benchmark_config = dict()
        with open(json_file, 'r') as file: 
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
                    ret = self.tdSql.checkData(0, 0, childtable_count)
                if childtable_count * insert_rows > 0:
                    self.tdSql.query("select count(*) from {}.{};".format(db_name, stb_name))
                    ret = self.tdSql.checkData(0, 0, childtable_count * insert_rows)
                
                # Query data check
                self.tdSql.query("show {}.tables;".format(db_name))
                stb_child_name = self.tdSql.getData(0,0)             
                if insert_rows > 0:
                    #sum check
                    sum_sql1 = "select sum(c1) from {}.{};".format(db_name, stb_child_name);
                    sum_sql2 = "select sum(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %sum_sql1 ,1,1,'%s' %sum_sql2 ,1,1)
                    #max check
                    max_sql1 = "select max(c1) from {}.{};".format(db_name, stb_child_name);
                    max_sql2 = "select max(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %max_sql1 ,1,1,'%s' %max_sql2 ,1,1)
                    #min check
                    min_sql1 = "select min(c1) from {}.{};".format(db_name, stb_child_name);
                    min_sql2 = "select min(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %min_sql1 ,1,1,'%s' %min_sql2 ,1,1)
                    #avg check
                    avg_sql1 = "select avg(c1) from {}.{};".format(db_name, stb_child_name);
                    avg_sql2 = "select avg(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %avg_sql1 ,1,1,'%s' %avg_sql2 ,1,1)
                    #first check
                    first_sql1 = "select first(c1) from {}.{};".format(db_name, stb_child_name);
                    first_sql2 = "select first(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %first_sql1 ,1,1,'%s' %first_sql2 ,1,1)
                    #last check
                    last_sql1 = "select last(c1) from {}.{};".format(db_name, stb_child_name);
                    last_sql2 = "select last(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %last_sql1 ,1,1,'%s' %last_sql2 ,1,1)
                    #last_row check
                    last_row_sql1 = "select last_row(c1) from {}.{};".format(db_name, stb_child_name);
                    last_row_sql2 = "select last_row(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %last_row_sql1 ,1,1,'%s' %last_row_sql2 ,1,1)
                    #top_2 check
                    top_sql1 = "select top(c1,2) from {}.{};".format(db_name, stb_child_name);
                    top_sql2 = "select top(c1,2) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %top_sql1 ,1,1,'%s' %top_sql2 ,1,1)
                    #bottom_2 check
                    bottom_sql1 = "select bottom(c1,2) from {}.{};".format(db_name, stb_child_name);
                    bottom_sql2 = "select bottom(c1,2) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %bottom_sql1 ,1,1,'%s' %bottom_sql2 ,1,1)
                    #csum check
                    csum_sql1 = "select csum(c1) from {}.{};".format(db_name, stb_child_name);
                    csum_sql2 = "select csum(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %csum_sql1 ,1,1,'%s' %csum_sql2 ,1,1)
                    #mavg check
                    mavg_sql1 = "select mavg(c1,2) from {}.{};".format(db_name, stb_child_name);
                    mavg_sql2 = "select mavg(c1,2) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %mavg_sql1 ,1,1,'%s' %mavg_sql2 ,1,1)
                    #spread check
                    spread_sql1 = "select spread(c1) from {}.{};".format(db_name, stb_child_name);
                    spread_sql2 = "select spread(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %spread_sql1 ,1,1,'%s' %spread_sql2 ,1,1)
                    #stddev check
                    stddev_sql1 = "select stddev(c1) from {}.{};".format(db_name, stb_child_name);
                    stddev_sql2 = "select stddev(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %stddev_sql1 ,1,1,'%s' %stddev_sql2 ,1,1)
                    #unique check TD-17597
                    # unique_sql1 = "select unique(c1) from {}.{};".format(db_name, stb_child_name);
                    # unique_sql2 = "select unique(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    # self.tdCreateData.dataequal('%s' %unique_sql1 ,1,1,'%s' %unique_sql2 ,1,1)
                    #twa check
                    twa_sql1 = "select twa(c1) from {}.{};".format(db_name, stb_child_name);
                    twa_sql2 = "select twa(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %twa_sql1 ,1,1,'%s' %twa_sql2 ,1,1)
                    #abs check
                    abs_sql1 = "select abs(c1) from {}.{};".format(db_name, stb_child_name);
                    abs_sql2 = "select abs(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %abs_sql1 ,1,1,'%s' %abs_sql2 ,1,1)
                    #sin check
                    sin_sql1 = "select sin(c1) from {}.{};".format(db_name, stb_child_name);
                    sin_sql2 = "select sin(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %sin_sql1 ,1,1,'%s' %sin_sql2 ,1,1)
                    #acos check
                    acos_sql1 = "select acos(c1) from {}.{};".format(db_name, stb_child_name);
                    acos_sql2 = "select acos(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %acos_sql1 ,1,1,'%s' %acos_sql2 ,1,1)
                    #ceil check
                    ceil_sql1 = "select ceil(c1) from {}.{};".format(db_name, stb_child_name);
                    ceil_sql2 = "select ceil(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %ceil_sql1 ,1,1,'%s' %ceil_sql2 ,1,1)
                    #log check
                    log_sql1 = "select log(c1) from {}.{};".format(db_name, stb_child_name);
                    log_sql2 = "select log(c1) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %log_sql1 ,1,1,'%s' %log_sql2 ,1,1)
                    #cast check
                    cast_sql1 = "select cast(c1 as bigint) from {}.{};".format(db_name, stb_child_name);
                    cast_sql2 = "select cast(c1 as bigint) from {}.{} where tbname = '{}';".format(db_name, stb_name, stb_child_name)
                    self.tdCreateData.dataequal('%s' %cast_sql1 ,1,1,'%s' %cast_sql2 ,1,1)

                self.tdSql.execute("delete from {}.{};".format(db_name, stb_name))
                self.tdSql.execute("flush database {};".format(db_name))
                self.tdSql.execute("reset query cache;")
                self.tdSql.query("select * from {}.{};".format(db_name, stb_name))
                self.tdSql.checkRow(0)
                    
        return ret

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

