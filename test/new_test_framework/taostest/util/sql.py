import datetime
import inspect
import os.path
import time
from typing import Tuple, List, Any
import threading
try:
    import taos
except:
    pass

from ..errors import TestException

def _parse_datetime(time_str):
    try:
        return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        pass
    try:
        return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass


class TDSql:
    # taostest enable sql recording variable
    taostest_enable_sql_recording_variable = "TAOSTEST_SQL_RECORDING_ENABLED"
    taostest_sql_file_name = "test.sql"

    def __init__(self, logger, run_log_dir, set_error_msg):
        self._logger = logger
        self._run_log_dir = run_log_dir
        self._set_error_msg = set_error_msg
        self.error_msg = ""
        self._conn: taos.TaosConnection = None
        self.query_row: int = None
        self.query_cols: int = None
        self.query_data: List[Tuple] = None
        self.query_result: taos.TaosResult = None
        self.affected_row: int = None
        self.vnode_status_index = 4
        self.time_out = 600
        self.white_list = ["statsd", "node_exporter", "collectd", "icinga2", "tcollector", "information_schema", "performance_schema"]

    @property
    def connection(self):
        """
        如果连接不存在则创建一个新的，
        如果存在则返回。
        注意：这个连接只能在主线程中使用。
        """
        if self._conn is None:
            self._conn = taos.connect(config=self._run_log_dir)
        return self._conn

    def recordSql(self, sql):
        if TDSql.taostest_enable_sql_recording_variable in os.environ:
            sql_file = os.path.join(self._run_log_dir, TDSql.taostest_sql_file_name)
            with open(sql_file, 'a') as f:
                if "select cast" not in sql:
                    if sql.endswith(";"):
                        f.write(f'{sql}\n')
                    else:
                        f.write(f'{sql};\n')

    def getConnection(self, config_dir: str = None):
        """
        返回一个新创建的数据库连接。
        """
        if config_dir is None:
            self._logger.info("getConnection %s", self._run_log_dir)
            return taos.connect(config=self._run_log_dir)
        else:
            self._logger.info("getConnection %s", config_dir)
            return taos.connect(config=config_dir)

    def get_connection(self, config, endpoint: str = None):
        if endpoint == None:
            endpoint = config["spec"]["config"]["firstEP"]
        host, port = endpoint.split(":")
        config_dir = self._run_log_dir
        if not config is None and "config_dir" in config["spec"]:
            config_dir = os.path.join(self._run_log_dir, config["spec"]["config_dir"])
        return taos.connect(host=host, port=int(port), config=config_dir)

    def query(self, sql, queryTimes=10, count_expected_res=None, record=True):
        """
         用途：主要用于执行正常语法输入的 select 语句
         更新: self._query_row, self._query_cols, self._query_data
         返回: TaosResult对象
        """
        i = 1
        if record:
            self._logger.debug("query: " + sql)
            self.recordSql(sql)
        else:
            self._logger.debug("query retrying...")
        while i <= queryTimes:
            try:
                result = self.connection.query(sql)
                self.query_data = result.fetch_all()
                self.query_row = result.row_count
                self.query_cols = result.field_count
                self.query_result = result
                if count_expected_res is not None:
                    counter = 0
                    while count_expected_res != self.query_data[0][0]:
                        result = self.connection.query(sql)
                        self.query_data = result.fetch_all()
                        if counter < self.stream_timeout:
                            counter += 0.5
                            time.sleep(0.5)
                        else:
                            return False
                return result
            except Exception as e:
                self._logger.debug("Try to query again, query times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    self._logger.debug("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def no_fetch_query(self, sql,queryTimes=10, expected_rows=100):
        """
         用途：主要用于执行正常语法输入的 select 语句,但不会fetch所有数据
        """
        i=1
        self._logger.debug("query: " + sql)
        self.recordSql(sql)
        while i <= queryTimes:
            try:
                result = self.connection.query(sql)
                for i, _ in enumerate(result):
                    if i == expected_rows - 1:
                        return result
            except Exception as e:
                self._logger.debug("Try to query again, query times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    self._logger.debug("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def execute(self, sql,queryTimes=10):
        """
        用途：用于执行 sql
        输入：sql语句
        更新： self._affected_row
        """
        i=1
        self._logger.debug("execute: " + sql)
        while i <= queryTimes:
            try:
                self.recordSql(sql)
                self.affected_row = self.connection.execute(sql)
                return self.affected_row
            except Exception as e:
                self._logger.debug("Try to execute sql again, query times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    self._logger.debug("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def error(self, sql, throw=True) -> bool:
        """
        @param throw: 失败是否抛异常
        @param sql: 期望执行错误的sql
        @return: 如果报错返回True, 如果没报错返回False
        """
        # self._logger.debug("debug error query: " + sql)
        # self.recordSql(sql)
        try:
            self._logger.debug("execute: " + sql)
            self.affected_row = self.connection.execute(sql)
            # self.execute(sql)
        except BaseException as e:
            self._logger.info(f"error occur as expected: {e}")
            self.error_msg = e
            return True
        if throw:
            raise AssertionError("expect error not occur:" + sql)
        else:
            self._set_error_msg("expect error not occur:" + sql)
            return False

    def checkData(self, row, col, data, throw=True) -> bool:
        """
        用途：用于在调用 query(select ...)后对返回结果数据进行检查
        输入：行号，列号，期望数值
        返回：正常，失败
        """
        if 0 <= row < self.query_row and 0 <= col < self.query_cols:
            real_data = self.query_data[row][col]
            # 直接比较，相等则返回
            if real_data == data:
                self._logger.debug(f"checkData success, row={row} col={col} expect={data} real={real_data}")
                return True
            # 直接比较不成功，尝试转换后再比较
            field: taos.TaosField = self.query_result.fields[col]
            if field.type == taos.FieldType.C_TIMESTAMP:
                if isinstance(data, int) or isinstance(data, float) or len(data) >= 28:
                    import pandas as pd
                    if pd.to_datetime(real_data) == pd.to_datetime(data):
                        self._logger.debug(f"checkData success, row={row} col={col} expect={data} real={real_data}")
                        return True
                elif real_data == _parse_datetime(data):
                    self._logger.debug(f"checkData success, row={row} col={col} expect={data} real={real_data}")
                    return True
            elif (field.type == taos.FieldType.C_FLOAT or field.type == taos.FieldType.C_DOUBLE) and isinstance(data, float):
                if abs(data - real_data) <= 0.000001:
                    self._logger.debug(f"checkData success, row={row} col={col} expect={data} real={real_data}")
                    return True
            if throw:
                raise AssertionError(f"checkData failed: row={row} col={col} expect={data} real={real_data}")
            else:
                self._set_error_msg(f"checkData failed: row={row} col={col} expect={data} real={real_data}")
                return False
        else:
            if throw:
                raise AssertionError(f"checkData param error, row:{row} col:{col} data:{data}")
            else:
                self._set_error_msg(f"checkData param error, row:{row} col:{col} data:{data}")
                return False

    def checkEqual(self, elm, expect_elm, throw=True) -> bool:
        """
        用途：用于在比较元素相等
        输入：两元素
        返回：正常，失败
        """
        if elm == expect_elm:
            self._logger.debug(f"checkEqual success, elm={elm} expect_elm={expect_elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
            else:
                self._set_error_msg(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
                return False

    def checkNotEqual(self, elm, expect_elm, throw=True) -> bool:
        """
        用途：用于在比较元素不相等
        输入：两元素
        返回：正常，失败
        """
        if elm != expect_elm:
            self._logger.debug(f"checkNotEqual success, elm!={elm} expect_elm={expect_elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkNotEqual error, elm={elm} expect_elm={expect_elm}")
            else:
                self._set_error_msg(f"checkNotEqual error, elm={elm} expect_elm={expect_elm}")
                return False

    def checkIn(self, contain_elm, elm, throw=True) -> bool:
        """
        用途：用于在比较元素包含
        输入：两元素
        返回：正常，失败
        """
        if contain_elm in elm:
            self._logger.debug(f"check in success, {contain_elm} in {elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkIn error, {contain_elm} not in {elm}")
            else:
                self._set_error_msg(f"checkIn error, {contain_elm} not in {elm}")
                return False

    def checkNotIn(self, contain_elm, elm, throw=True) -> bool:
        """
        用途：用于在比较元素不包含
        输入：两元素
        返回：正常，失败
        """
        if contain_elm not in elm:
            self._logger.debug(f"check not in success, {contain_elm} in {elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkNotIn error, {contain_elm} in {elm}")
            else:
                self._set_error_msg(f"checkNotIn error, {contain_elm} in {elm}")
                return False

    def getData(self, row, col):
        """
        用途：在调用 query(select ...)后可返回结果数据
        输入：行号，列号
        返回：该行号或者列号下的数据
        """
        if 0 <= row < self.query_row and 0 <= col < self.query_cols:
            return self.query_data[row][col]
        else:
            raise TestException(f"getData out of range: row={row} col={col}")

    def get_variable(self, search_attr):
        """
        get variable of search_attr access "show variables"
        """
        try:
            self.query("show variables")
            param_list = self.query_data
            for param in param_list:
                if param[0] == search_attr:
                    return param[1], param_list
        except Exception as e:
            raise Exception(repr(e))

    def getColNameList(self, col_tag=None):
        """
        get col name/type list
        """
        try:
            col_name_list = []
            col_type_list = []
            length_list = list()
            note_list = list()
            for query_col in self.query_data:
                col_name_list.append(query_col[0])
                col_type_list.append(query_col[1])
                length_list.append(query_col[2])
                note_list.append(query_col[3])
            if col_tag:
                return col_name_list, col_type_list, length_list, note_list
            return col_name_list
        except Exception as e:
            raise Exception(repr(e))

    def checkRow(self, expectQueryRows, throw=True):
        """
        这个方法和query方法是一组
        用途：检查查询语句返回的结果数是否符合预期值
        """
        if self.query_row == expectQueryRows:
            self._logger.debug("checkRow success")
            return True
        else:
            if throw:
                raise AssertionError(f"checkRow failed: expect={expectQueryRows} actual={self.query_row}")
            else:
                self._set_error_msg(f"checkRow failed: expect={expectQueryRows} actual={self.query_row}")
                return False

    def checkAffectedRows(self, expectAffectedRows, throw=True) -> bool:
        """
        这个方法合execute方法是一组
        用途：检查更新操作受影响的行数是否符合预期
        输入：sql期望的行数
        返回：正常，失败或退出
        """
        if self.affected_row == expectAffectedRows:
            self._logger.debug("checkAffectedRows success")
            return True
        else:
            if throw:
                raise AssertionError(f"checkAffectedRows failed: expect={expectAffectedRows} actual={self.affected_row}")
            else:
                self._set_error_msg(f"checkAffectedRows failed: expect={expectAffectedRows} actual={self.affected_row}")
                return False

    def checkOneRow(self, checkList: List[Any], throw=True) -> bool:
        """
        用途：用于query单条结果输出list进行校验
        输入：期望的checkList
        返回：正常，失败
        """
        if self.query_data == checkList:
            self._logger.debug("checkRow success")
            return True
        else:
            if throw:
                raise AssertionError(f"checkRow failed: expect={checkList} actual={self.query_data}")
            else:
                self._set_error_msg(f"checkRow failed: expect={checkList} actual={self.query_data}")
                return False

    def drop_all_db(self):
        self.query("show databases")
        if len(self.query_data) > 0:
            for db_info_list in self.query_data:
                if db_info_list[0] not in self.white_list or "telegraf" not in db_info_list[0]:
                    self.execute(f"drop database if exists `{db_info_list[0]}`")
                    if not db_info_list[0][0].isdigit():
                        self.execute(f"drop database if exists {db_info_list[0]}")

    def drop_db(self,database):
        #delete table and flushd db before delete database:
        self.query(f" show {database}.tables ")
        if len(self.query_data) > 0:
            for table_info_list in self.query_data:
                self.execute("delete from `{}`.`{}`;".format(database, table_info_list[0]))
                self.execute("flush database {};".format(database)) 
                self.execute("reset query cache;")
                self.query("select * from {}.`{}`;".format(database, table_info_list[0]))
                self.checkRow(0)
        self.query(f" show {database}.stables ")
        if len(self.query_data) > 0:
            for stable_info_list in self.query_data:
                self.execute("delete from {}.`{}`;".format(database, stable_info_list[0]))
                self.execute("flush database {};".format(database)) 
                self.execute("reset query cache;")
                self.query("select * from {}.`{}`;".format(database, stable_info_list[0]))
                self.checkRow(0)
        self.execute("drop database if exists {} ;".format(database))

    def multiThreadFunction(self, func_name, param_list):
        tlist = list()
        for param in param_list:
            t = threading.Thread(target=func_name, args=(param,))
            tlist.append(t)
        return tlist

    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self.execute, args=(insert_sql,))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def getOneRow(self, location, containElm) -> list:
        """
        用途：用于query并筛选出指定location含有containElm的tuple/list并将结果加入新列表
        输入：期望指定location包含的元素
        返回：正常，失败
        """
        res_list = list()
        if 0 <= location < self.query_row:
            for row in self.query_data:
                if row[location] == containElm:
                    res_list.append(row)
            return res_list
        else:
            raise TestException(f"getOneRow out of range: row_index={location} row_count={self.query_row}")

    def get_db_field_kv(self, location, containElm) -> list:
        res_list = self.getOneRow(location, containElm)
        field_list = list(map(lambda x: x["name"], self.query_result.fields))
        if len(res_list) > 0:
            return dict(zip(field_list, res_list[0]))
        else:
            raise TestException(f"get db field kv filed, dbname = {containElm}")


    def checkMultiRows(self, keywords, checkoutList: List[Any], throw=True) -> bool:
        """
        用途：用于query返回多条结果，选择包含关键字的行
        输入：keyword:关键字，checkoutList:需要对比的list
        返回：正常，失败
        """
        # 优先级不高，用的时候再实现
        raise NotImplementedError

    def get_replica(self, dbname):
        self.query('select * from information_schema.ins_databases')
        db_field_kv_dict = self.get_db_field_kv(0, dbname)
        return db_field_kv_dict["replica"]

    def get_dnodes(self):
        self.query('show dnodes;')
        dnode_id_list = list(map(lambda x:x[0], self.query_data))
        dnode_endpoint_list = list(map(lambda x:x[1], self.query_data))
        dnode_status_list = list(map(lambda x:x[4], self.query_data))
        self._logger.debug(f"get dnode_id_list, dnode_endpoint_list, dnode_status_list-----[{dnode_id_list}, {dnode_endpoint_list}, {dnode_status_list}]")
        return dnode_id_list, dnode_endpoint_list, dnode_status_list

    def get_dnodes_out_mnodes(self):
        self.query('show mnodes;')
        mnode_id_list = list(map(lambda x:x[0], self.query_data))
        new_index = list()
        dnodes_out_mnodes_list = self.get_dnodes()
        for i, j  in enumerate(dnodes_out_mnodes_list[0]):
            if j in mnode_id_list:
                new_index.append(i)
        new_index.reverse()
        for m in dnodes_out_mnodes_list:
            for n in new_index:
                m.pop(n)
        self._logger.debug(f"get dnodes_out_mnodes_list-----{dnodes_out_mnodes_list}")
        return dnodes_out_mnodes_list

    def get_vnode_status_list(self, dbname):
        db_replica = self.get_replica(dbname)
        if db_replica == 1:
            _db_replica = 3
        else:
            _db_replica = db_replica
        self.query(f'show {dbname}.vgroups;')

        vnode_status_list = list()


        for query_data in self.query_data:
            sub_vnode_status_list = list()
            status_start_index = self.vnode_status_index
            for i in range(_db_replica):
                sub_vnode_status_list.append(query_data[status_start_index])
                status_start_index += 2
            vnode_status_list.append(sorted(list(map(lambda x:str(x), sub_vnode_status_list))))
        self._logger.debug(f"get vnode_status_list-----{vnode_status_list}")
        return vnode_status_list

    def wait_select_leader(self, dbname):
        offline_ready_flag = False
        select_leader_time = 0
        check_offline_time = 0

        while not offline_ready_flag:
            if check_offline_time < self.time_out:
                check_offline_time += 1
            else:
                self._logger.debug(f"dbname:{dbname} check offline timeout after {self.time_out}s")
                check_offline_time = self.time_out
            vnode_status_list = self.get_vnode_status_list(dbname)
            if "offline" in str(vnode_status_list):
                offline_ready_flag = True
            time.sleep(1)
        self._logger.debug(f"dbname:{dbname} check offline time --- {check_offline_time}s")

        vnode_status_list = self.get_vnode_status_list(dbname)
        leader_subelm1 = ["leader"]
        leader_subelm2 = ["leader*"]
        leader_ready_flag = all([word1 in text or word2 in text for word1 in leader_subelm1 for word2 in leader_subelm2 for text in vnode_status_list])

        self._logger.debug(f"waiting for select leader ----dbname:{dbname}")
        if leader_ready_flag and offline_ready_flag:
            self._logger.debug(f"dbname:{dbname} select leader success after 0s")
        while not leader_ready_flag:
            if select_leader_time < self.time_out:
                select_leader_time += 1
            else:
                self._logger.debug(f"dbname:{dbname} select leader timeout after {self.time_out}s")
                select_leader_time = self.time_out
                return select_leader_time
            vnode_status_list = self.get_vnode_status_list(dbname)
            leader_ready_flag = all([word1 in text or word2 in text for word1 in leader_subelm1 for word2 in leader_subelm2 for text in vnode_status_list])

            if leader_ready_flag:
                self._logger.debug(f"dbname:{dbname} select leader success after {select_leader_time}s")
            time.sleep(1)
        return select_leader_time

    def wait_sync_ready(self, dbname, sync_value=[("follower", "leader", "offline")]):
        vnode_status_list = self.get_vnode_status_list(dbname)
        vnode_status_unique = list(set([tuple(t) for t in vnode_status_list]))
        sync_time = 0
        self._logger.debug(f"waiting for sync----dbname:{dbname}")
        if vnode_status_unique == sync_value:
            self._logger.debug(f"dbname:{dbname} sync success after 0s")
        while vnode_status_unique != sync_value:
            vnode_status_list = self.get_vnode_status_list(dbname)
            vnode_status_unique = list(set([tuple(t) for t in vnode_status_list]))
            if sync_time < self.time_out:
                sync_time += 1
            else:
                self._logger.debug(f"dbname:{dbname} sync timeout after {self.time_out}s")
                sync_time = self.time_out
                return sync_time
            if vnode_status_unique == sync_value:
                self._logger.debug(f"dbname:{dbname} sync success after {sync_time}s")
            time.sleep(1)
        return sync_time

    def get_db_vgroup_status(self, dbname, stop1dnode=False, stop2dnode=False, dnode_count=3, history_replica=None):
        db_replica = self.get_replica(dbname)
        if db_replica == 1:
            if history_replica:
                if history_replica == 3:
                    return [tuple(sorted(["leader", "follower", "follower"]))]
            return [tuple(sorted(["leader"] + ['None']*(dnode_count-1)))]
        elif db_replica == 3:
            if history_replica:
                if history_replica == 3:
                    return [tuple(sorted(["leader"] + ['None']*(dnode_count-1)))]
            if stop1dnode:
                return [tuple(sorted(["leader", "follower", "offline"]))]
            elif stop2dnode:
                return [tuple(sorted(["leader", "offline", "offline"]))]
            else:
                return [tuple(sorted(["leader", "follower", "follower"]))]
        elif db_replica == 5:
            if stop1dnode and stop2dnode:
                return [tuple(sorted(["leader", "follower", "follower", "offline", "offline"]))]
            elif stop1dnode and not stop2dnode:
                return [tuple(sorted(["leader", "follower", "follower", "follower", "offline"]))]
            else:
                return [tuple(sorted(["leader", "follower", "follower", "follower", "follower"]))]
        else:
            pass

    @staticmethod
    def _compare_row(row1, row2):
        """
        比较两行数据
        @param row1: 可以是list也可以是tuple
        @param row2: 可以是list也可以是tuple
        @return: row1和row2对应位置的值是否相等
        """
        raise NotImplementedError

    def close(self):
        """
        关闭数据库连接
        """
        if self._conn:
            try:
                self._conn.close()
            except:
                pass
