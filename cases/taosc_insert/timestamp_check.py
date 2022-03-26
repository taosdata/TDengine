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
import copy

class TestTimestamp(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def ms_us_ns_db_check(self):
        """
        precision = ["ms", "us", "ns"]
        """
        for ts in ["ms", "us", "ns"]:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            self.tdSql.query('show databases')
            res = self.tdSql.getOneRow(0, dbname)
            self.tdSql.checkEqual(res[0][16], ts)
            self.tdSql.execute(f'create table if not exists {dbname}.{dbname} (ts timestamp, c1 int)')
            timestamp, dt = self.tdCom.genTs(ts)
            self.tdSql.execute(f'insert into {dbname}.{dbname} values ({timestamp}, 1)')
            self.tdSql.query(f'select ts from {dbname}.{dbname}')
            self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), str(dt))
            self.tdSql.execute(f'drop database if exists {dbname}')

    def h_m_s_check(self):
        """
        check hh:mm:ss
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
        self.tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:01", 1)')
        self.tdSql.query(f'select * from {dbname}.tb where c1 = 1')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2022-01-16 21:17:01")
        self.tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:61", 2)')
        self.tdSql.query(f'select * from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2022-01-16 21:18:01")
        self.tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:121", 3)')
        self.tdSql.query(f'select * from {dbname}.tb where c1 = 3')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2022-01-16 21:17:12")
        # TODO confirm
        self.tdSql.error(f'insert into {dbname}.tb values ("2022-01-16 21:17:62", 2)')
        self.tdSql.execute(f'drop database if exists {dbname}')

    # ! bug
    def human_date_check(self):
        """
        human date check
        """
        for ts in ["ms"]:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = self.tdCom.genTs(ts)[1]
            self.tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for ts_unit in self.tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                # tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_add using {dbname}.stb tags ("{timestamp}+1000{ts_unit}", 1)')
                # tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_sub using {dbname}.stb tags ("{timestamp}-1{ts_unit}", 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values ("{timestamp}+1{ts_unit}", 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values ("{timestamp}-1{ts_unit}", 1)')
            self.tdSql.query(f'select count(*) from {dbname}.tb')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], 16)
            self.tdSql.query(f'show {dbname}.stables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][4], 17)
            self.tdSql.execute(f'drop database if exists {dbname}')

    def now_check(self):
        """
        now check
        """
        for ts in ["ms", "us", "ns"]:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            dbname = dbname + '_' + ts
            self.tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for ts_unit in self.tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                self.tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_add using {dbname}.stb tags (now+{step}{ts_unit}, 1)')
                self.tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_sub using {dbname}.stb tags (now-{step}{ts_unit}, 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values (now+{step}{ts_unit}, 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values (now-{step}{ts_unit}, 1)')
            self.tdSql.query(f'select count(*) from {dbname}.tb')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], 16)
            self.tdSql.query(f'show {dbname}.stables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][4], 17)
            self.tdSql.execute(f'drop database if exists {dbname}')

    def epoch_check(self):
        """
        epoch check
        """
        for ts in ["ms", "us", "ns"]:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = self.tdCom.genTs(ts)[0]
            self.tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({timestamp}, 1)')
            for ts_unit in self.tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags ({timestamp}+1{ts_unit}, 1)')
                self.tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags ({timestamp}-1{ts_unit}, 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values ({timestamp}+{step}{ts_unit}, 1)')
                self.tdSql.execute(f'insert into {dbname}.tb values ({timestamp}-{step}{ts_unit}, 1)')
            self.tdSql.query(f'select count(*) from {dbname}.tb')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], 16)
            self.tdSql.execute(f'drop database if exists {dbname}')

    def error_check(self):
        """
        ts error check
        """
        # inconsistent precision
        pricision_list = ["ms", "us", "ns"]
        for ts in pricision_list:
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = self.tdCom.genTs(ts)[0]
            self.tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({timestamp}, 1)')
            pricision_list_tmp = copy.deepcopy(pricision_list)
            pricision_list_tmp.remove(ts)
            for illegal_ts in pricision_list_tmp:
                # TODO confirm
                # tdSql.error(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({tdCom.genTs(illegal_ts)[0]}, 1)')
                self.tdSql.error(f'insert into {dbname}.tb values ({self.tdCom.genTs(illegal_ts)[0]}, 1)')

            # * The second level can exceed 60
            for error_sql in [
                f'insert into {dbname}.tb values ("2022-01-143 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14# 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 00:05:55.*_*", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 0 0:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-1 4 00:05:55", 1)',
                f'insert into {dbname}.tb values ("9999-01-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-00-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-13-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-00 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-32 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-02-31 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-04-31 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 25:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 00:61:55", 1)',
                f'insert into {dbname}.tb values (now + 1n, 1)',
                f'insert into {dbname}.tb values (now - 1n, 1)',
                f'insert into {dbname}.tb values (now + 1y, 1)',
                f'insert into {dbname}.tb values (now - 1y, 1)'
                ]:
                self.tdSql.error(error_sql)

    def run(self) -> bool:
        self.ms_us_ns_db_check()
        self.h_m_s_check()
        #! bug
        # self.human_date_check()
        self.now_check()
        self.epoch_check()
        self.error_check()


    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            ms_us_ns_db_check <jayden>: [TD-13419] : check db ms/us/ns precision;\n
            h_m_s_check <jayden>: [TD-13419] : check ts second-level >= 60;\n
            human_date_check <jayden>: [TD-13419] : human date check;\n
            now_check <jayden>: [TD-13419] : now check;\n
            epoch_check <jayden>: [TD-13419] : epoch check;\n
            error_check <jayden>: [TD-13419] : erro check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.TimestampTest

