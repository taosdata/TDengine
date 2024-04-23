import sys
import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *

class TDTestCase:
    updatecfgDict = {'debugFlag':0, 'vdebugFlag': 143, 'qdebugflag':135, 'tqdebugflag':135, 'udebugflag':135, 'rpcdebugflag':135,
                     'asynclog': 0, 'stdebugflag':143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom

    def scalar_function(self, partition="tbname", fill_history_value=None):
        tdLog.info(f"*** testing stream scalar funtion partition: {partition}, fill_history_value: {fill_history_value} ***")
        self.tdCom.case_name = sys._getframe().f_code.co_name
        tdLog.info("preparing data ...")
        self.tdCom.prepare_data(fill_history_value=fill_history_value)
        # return
        tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
        tdSql.execute('create table if not exists scalar_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20));')
        if fill_history_value is None:
            fill_history = ""
        else:
            tdLog.info("inserting fill_history data ...")
            fill_history = f'fill_history {fill_history_value}'
            for i in range(self.tdCom.range_count):
                tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}-{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}-{i}s, 100, -100.1, "heBei", Null, "Bigdata");')

        # self.tdCom.write_latency(self.case_name)
        math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
        string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
        for math_function in math_function_list:
            tdLog.info(f"testing function {math_function} ...")
            tdLog.info(f"creating stream for function {math_function} ...")
            if math_function in ["log", "pow"]:
                tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0 {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_ct1;')
                tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_tb;')
            else:
                tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_ct1;')
                tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_ctb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_tb", math_function)
            for tbname in ["scalar_ct1", "scalar_tb"]:
                tdLog.info(f"function {math_function}: inserting data for tb --- {tbname} ...")
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')
            for i in range(self.tdCom.range_count):
                tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}+{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}+{i}s, 100, -100.1, "heBei", Null, "Bigdata");')
                if i%2 == 0:
                    tdLog.info(f"function {math_function}: update testing ...")
                    tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                else:
                    tdLog.info(f"function {math_function}: delete testing ...")
                    dt = f'cast({self.tdCom.date_time-1} as timestamp)'
                    tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    tdSql.execute(f'delete from scalar_tb where ts = {dt};')

                if fill_history_value:
                    tdLog.info(f"function {math_function}: disorder testing ...")
                    tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}-{self.tdCom.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}-{self.tdCom.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    dt = f'cast({self.tdCom.date_time-(self.tdCom.range_count-1)} as timestamp)'
                    tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    tdSql.execute(f'delete from scalar_tb where ts = {dt};')
            if math_function == "log" or math_function == "pow":
                tdLog.info(f"function {math_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb  partition by {partition} order by ts;')
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_ctb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_tb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_tb;')
            else:
                tdLog.info(f"function {math_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb  partition by {partition} order by ts;')
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_ctb;', f'select {math_function}(c1), {math_function}(c2) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_tb;', f'select {math_function}(c1), {math_function}(c2) from scalar_tb;')
            tdSql.execute(f'drop stream if exists stb_{math_function}_stream')
            tdSql.execute(f'drop stream if exists ctb_{math_function}_stream')
            tdSql.execute(f'drop stream if exists tb_{math_function}_stream')

        for string_function in string_function_list:
            tdLog.info(f"testing function {string_function} ...")
            tdLog.info(f"creating stream for function {string_function} ...")
            if string_function == "concat":
                tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
                tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
            elif string_function == "concat_ws":
                tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
                tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
            elif string_function == "substr":
                tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
                tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
            else:
                tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb partition by {partition};')
                tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
                tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_stb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_ctb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_tb", string_function)
            for tbname in ["scalar_ct1", "scalar_tb"]:
                tdLog.info(f"function {string_function}: inserting data for tb --- {tbname} ...")
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
                tdSql.execute(f'insert into {tbname} values ({self.tdCom.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')


            for i in range(self.tdCom.range_count):
                tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}+{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}+{i}s, 100, -100.1, "heBei", Null, "Bigdata");')
                if i%2 == 0:
                    tdLog.info(f"function {string_function}: update testing...")
                    tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                else:
                    tdLog.info(f"function {string_function}: delete testing ...")
                    dt = f'cast({self.tdCom.date_time-1} as timestamp)'
                    tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    tdSql.execute(f'delete from scalar_tb where ts = {dt};')

                if fill_history_value:
                    tdLog.info(f"function {string_function}: disorder testing ...")
                    tdSql.execute(f'insert into scalar_ct1 values ({self.tdCom.date_time}-{self.tdCom.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    tdSql.execute(f'insert into scalar_tb values ({self.tdCom.date_time}-{self.tdCom.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    dt = f'cast({self.tdCom.date_time-(self.tdCom.range_count-1)} as timestamp)'
                    tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    tdSql.execute(f'delete from scalar_tb where ts = {dt};')


            if string_function == "concat":
                tdLog.info(f"function {string_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
            elif string_function == "concat_ws":
                tdLog.info(f"function {string_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
            elif string_function == "substr":
                tdLog.info(f"function {string_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_ctb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_tb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
            else:
                tdLog.info(f"function {string_function}: confirming query result ...")
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_ctb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_tb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')

            tdSql.execute(f'drop stream if exists stb_{string_function}_stream')
            tdSql.execute(f'drop stream if exists ctb_{string_function}_stream')
            tdSql.execute(f'drop stream if exists tb_{string_function}_stream')

    def run(self):
        self.scalar_function(partition="tbname", fill_history_value=1)
        self.scalar_function(partition="tbname,c1,t1", fill_history_value=1)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())