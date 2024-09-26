import time
import random
import taos
from enum import Enum

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.srvCtl import *


class TDTestCase(TBase):
    """
    Description: This class is used to verify the feature of 'drop table by uid' for task TS-5111
    FS: https://taosdata.feishu.cn/wiki/JgeDwZkH3iTNv2ksVkWcHenKnTf
    TS: https://taosdata.feishu.cn/wiki/DX3FwopwGiXCeRkBNXFcj0MBnnb
    create: 
        2024-09-23 created by Charles
    update:
        None
    """
    
    class TableType(Enum):
        STABLE = 0
        CHILD_TABLE = 1
        REGULAR_TABLE = 2

    def init(self, conn, logSql, replicaVar=1):
        """Initialize the TDengine cluster environment
        """
        super(TDTestCase, self).init(conn, logSql, replicaVar, db="db")
        tdSql.init(conn.cursor(), logSql)

    def get_uid_by_db_table_name(self, db_name, table_name, table_type=TableType.STABLE):
        """Get table uid with db name and table name from system table
        :param db_name: database name
        :param table_name: table name
        :param table_type: table type, default is stable
        :return: table uid
        """
        if table_type == self.TableType.STABLE:
            tdSql.query(f"select * from information_schema.ins_stables where db_name='{db_name}' and stable_name like '%{table_name}%';")
        elif table_type == self.TableType.CHILD_TABLE:
            tdSql.query(f"select * from information_schema.ins_tables where db_name='{db_name}' and table_name like '%{table_name}%' and stable_name is not null;")
        else:
            tdSql.query(f"select * from information_schema.ins_tables where db_name='{db_name}' and table_name like '%{table_name}%' and stable_name is null;")
        # check whether the table uid is empty
        if len(tdSql.res) == 0:
            tdLog.debug(f"Can't get table uid with db name: {db_name} and table name: {table_name}")
            return None
        # get table uid list
        if table_type == self.TableType.STABLE:
            return [item[10] for item in tdSql.res]
        else:
            return [item[5] for item in tdSql.res]

    def get_uid_by_db_name(self, db_name, table_type=TableType.STABLE):
        """Get table uid with db name and table type from system table
        :param db_name: database name
        :param table_type: table type, default is stable
        :return: table uid list
        """
        if table_type == self.TableType.STABLE:
            tdSql.query(f"select * from information_schema.ins_stables where db_name='{db_name}';")
        elif table_type == self.TableType.CHILD_TABLE:
            tdSql.query(f"select * from information_schema.ins_tables where db_name='{db_name}' and stable_name is not null;")
        else:
            tdSql.query(f"select * from information_schema.ins_tables where db_name='{db_name}' and stable_name is null;")
        # check whether the table uid is empty
        if len(tdSql.res) == 0:
            tdLog.debug(f"Can't get table uid with db name: {db_name}")
            return None
        # get table uid list
        if table_type == self.TableType.STABLE:
            return [item[10] for item in tdSql.res]
        else:
            return [item[5] for item in tdSql.res]

    def drop_table_by_uid(self, uid_list, table_type=TableType.STABLE, exist_ops=False):
        """Drop the specified tables by uid list
        :db_name: database name
        :param uid_list: table uid list to be dropped
        :param exist_ops: whether to use exist option, default is False
        :return: None
        """
        # check whether the uid list is empty
        if len(uid_list) == 0:
            return
        # drop table by uid
        if exist_ops and table_type == self.TableType.STABLE:
            for uid in uid_list:
                tdSql.execute(f"drop stable with if exists `{uid}`;")
        else:
            uids = ','.join(["`" + str(item) + "`" for item in uid_list])
            tdSql.execute(f"drop table with {uids};")

    def test_drop_single_table_by_uid(self):
        """Verify the feature of dropping a single stable/child table/regular table by uid with root user
        """
        db_name = "test_drop_single_table_by_uid"
        tdLog.info("Start test case: test_drop_single_table_by_uid")
        # data for case test_drop_single_table_by_uid
        tdLog.info("Prepare data for test case test_drop_single_table_by_uid")
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        # table with normal characters
        tdSql.execute("create stable st1 (ts timestamp, c1 int) tags (t1 int);")
        tdSql.execute("create table ct1_1 using st1 tags(1);")
        tdSql.execute("create table t1 (ts timestamp, c1 int, c2 float);")
        tdLog.info("Finish preparing data for test case test_drop_single_table_by_uid of normal characters")
        # get table uid
        uid_st1 = self.get_uid_by_db_table_name(db_name, "st1")
        tdLog.debug(f"uid_st1: {uid_st1}")
        uid_ct1_1 = self.get_uid_by_db_table_name(db_name, "ct1_1", self.TableType.CHILD_TABLE)
        tdLog.debug(f"uid_ct1_1: {uid_ct1_1}")
        uid_t1 = self.get_uid_by_db_table_name(db_name, "t1", self.TableType.REGULAR_TABLE)
        tdLog.debug(f"uid_t1: {uid_t1}")
        # drop table by uid
        self.drop_table_by_uid(uid_t1, self.TableType.REGULAR_TABLE)
        self.drop_table_by_uid(uid_ct1_1, self.TableType.CHILD_TABLE)
        self.drop_table_by_uid(uid_st1, self.TableType.STABLE, True)
        
        # table with special characters
        tdSql.execute("create stable `st2\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
        tdSql.execute("create table `ct2_1\u00cf\u00ff` using `st2\u00bf\u200bfnn1` tags(1);")
        tdSql.execute("create table `t2\u00ef\u00fa` (ts timestamp, c1 int, c2 float);")
        tdLog.info("Finish preparing data for test case test_drop_single_table_by_uid of special characters")
        # get table uid
        uid_st2 = self.get_uid_by_db_table_name(db_name, "st2")
        tdLog.debug(f"uid_st2: {uid_st2}")
        uid_ct2_1 = self.get_uid_by_db_table_name(db_name, "ct2_1", self.TableType.CHILD_TABLE)
        tdLog.debug(f"uid_ct2_1: {uid_ct2_1}")
        uid_t2 = self.get_uid_by_db_table_name(db_name, "t2", self.TableType.REGULAR_TABLE)
        tdLog.debug(f"uid_t2: {uid_t2}")
        # drop table by uid
        self.drop_table_by_uid(uid_t2, self.TableType.REGULAR_TABLE)
        self.drop_table_by_uid(uid_ct2_1, self.TableType.CHILD_TABLE)
        self.drop_table_by_uid(uid_st2, self.TableType.STABLE, True)
        tdSql.execute(f"drop database {db_name};")
        tdLog.info("Finish test case: test_drop_single_table_by_uid")

    def test_drop_multiple_tables_by_uid(self):
        """Verify the feature of dropping multiple tables by uid with root user
        """
        db_name = "test_drop_multiple_tables_by_uid"
        table_number = 100
        tdLog.info("Start test case: test_drop_multiple_tables_by_uid")
        # data for case test_drop_multiple_tables_by_uid
        tdLog.info("Prepare data for test case test_drop_multiple_tables_by_uid")
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        # table with normal characters
        for i in range(table_number):
            tdSql.execute(f"create stable st{i} (ts timestamp, c1 int) tags (t1 int);")
            tdSql.execute(f"create table ct{i}_{i} using st{i} tags({i+1});")
            tdSql.execute(f"create table t{i} (ts timestamp, c1 int, c2 float);")
        tdLog.info("Finish preparing data for test case test_drop_multiple_tables_by_uid of normal characters")
        # get table uid
        uid_st = self.get_uid_by_db_table_name(db_name, "st")
        # tdLog.debug(f"Get multiple stable uid list: {uid_st}")
        uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
        # tdLog.debug(f"Get multiple child table uid list: {uid_ct}")
        uid_t = self.get_uid_by_db_table_name(db_name, "t", self.TableType.REGULAR_TABLE)
        # tdLog.debug(f"Get multiple regular table uid list: {uid_t}")
        # drop table by uid
        self.drop_table_by_uid(uid_t, self.TableType.REGULAR_TABLE)
        self.drop_table_by_uid(uid_ct, self.TableType.CHILD_TABLE)
        self.drop_table_by_uid(uid_st, self.TableType.STABLE, True)
        
        # table with special characters
        for i in range(table_number):
            tdSql.execute(f"create stable `st{i}\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
            tdSql.execute(f"create table `ct{i}_{i}\u00cf\u00ff` using `st{i}\u00bf\u200bfnn1` tags(1);")
            tdSql.execute(f"create table `t{i}\u00ef\u00fa` (ts timestamp, c1 int, c2 float);")
        # get table uid
        uid_st = self.get_uid_by_db_table_name(db_name, "st")
        # tdLog.debug(f"Get multiple stable uid list: {uid_st}")
        uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
        # tdLog.debug(f"Get multiple child table uid list: {uid_ct}")
        uid_t = self.get_uid_by_db_table_name(db_name, "t", self.TableType.REGULAR_TABLE)
        # tdLog.debug(f"Get multiple regular table uid list: {uid_t}")
        # drop table by uid
        self.drop_table_by_uid(uid_t, self.TableType.REGULAR_TABLE)
        self.drop_table_by_uid(uid_ct, self.TableType.CHILD_TABLE)
        self.drop_table_by_uid(uid_st, self.TableType.STABLE, True)
        tdSql.execute(f"drop database {db_name};")
        tdLog.info("Finish test case: test_drop_multiple_tables_by_uid")

    def test_uid_as_table_name(self):
        """Verify using uid as table name, drop table with uid doesn't affect other tables
        """
        db_name = "test_uid_as_table_name"
        tdLog.info("Start test case: test_uid_as_table_name")
        # data for case test_uid_as_table_name
        tdLog.info("Prepare data for test case test_uid_as_table_name")
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        # super table
        tdSql.execute(f"create stable `st1\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
        uid_st = self.get_uid_by_db_table_name(db_name, "st")
        tdSql.execute(f"create stable `{uid_st[0]}` (ts timestamp, c1 int) tags (t1 int);")
        self.drop_table_by_uid(uid_st, self.TableType.STABLE, True)
        uid_st = self.get_uid_by_db_table_name(db_name, str(uid_st[0]))
        assert uid_st is not None
        tdLog.info(f"Drop stable with special characters with uid {uid_st[0]}, stable named as {uid_st[0]} doesn't be affected")
        # child table 
        tdSql.execute(f"create stable `st2\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
        tdSql.execute(f"create table `ct2_1\u00cf\u00ff` using `st2\u00bf\u200bfnn1` tags(1);")
        uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
        tdSql.execute(f"create table `{uid_ct[0]}` using `st2\u00bf\u200bfnn1` tags(2);")
        self.drop_table_by_uid(uid_ct, self.TableType.CHILD_TABLE)
        uid_ct = self.get_uid_by_db_table_name(db_name, str(uid_ct[0]), self.TableType.CHILD_TABLE)
        assert uid_ct is not None
        tdLog.info(f"Drop child table with special characters with uid {uid_ct[0]}, child table named as {uid_ct[0]} doesn't be affected")
        # regular table
        tdSql.execute(f"create table `t2\u00bf\u200bfnn1` (ts timestamp, c1 int);")
        uid_t = self.get_uid_by_db_table_name(db_name, "t2", self.TableType.REGULAR_TABLE)
        tdSql.execute(f"create table `{uid_t[0]}` (ts timestamp, c1 int);")
        self.drop_table_by_uid(uid_t, self.TableType.REGULAR_TABLE)
        uid_t = self.get_uid_by_db_table_name(db_name, str(uid_t[0]), self.TableType.REGULAR_TABLE)
        assert uid_t is not None
        tdLog.info(f"Drop regular table with special characters with uid {uid_t[0]}, regular table named as {uid_t[0]} doesn't be affected")
        tdSql.execute(f"drop database {db_name};")
        tdLog.info("Finish test case: test_uid_as_table_name")

    def test_abnormal_non_exist_uid(self):
        """Verify dropping table with non-exist uid
        """
        db_name = "test_abnormal_non_exist_uid"
        tdLog.info("Start test case: test_abnormal_non_exist_uid")
        # data for case test_abnormal_non_exist_uid
        tdLog.info("Prepare data for test case test_abnormal_non_exist_uid")
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        # drop table with non-exist uid
        tdSql.error(f"drop stable with if exists `1234567890`;", expectErrInfo="STable not exist:")
        tdSql.error(f"drop table with `1234567890`;", expectErrInfo="Table does not exist:")
        tdSql.execute(f"drop database {db_name};")
        tdLog.info("Finish test case: test_abnormal_non_exist_uid")

    def test_abnormal_incorrect_table_type(self):
        """Verify dropping table with incorrect sql, like drop stable sql with table or child table uid
        """
        try:
            db_name = "test_abnormal_incorrect_table_type"
            tdLog.info("Start test case: test_abnormal_incorrect_table_type")
            # data for case test_abnormal_incorrect_table_type
            tdLog.info("Prepare data for test case test_abnormal_incorrect_table_type")
            tdSql.execute(f"create database {db_name};")
            tdSql.execute(f"use {db_name};")
            tdSql.execute("create stable `st3\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
            tdSql.execute("create table `ct3_1\u00cf\u00ff` using `st3\u00bf\u200bfnn1` tags(1);")
            tdSql.execute("create table `t3\u00ef\u00fa` (ts timestamp, c1 int, c2 float);")
            tdLog.info("Finish preparing data for test case test_abnormal_incorrect_table_type of special characters")
            # get table uid
            uid_st = self.get_uid_by_db_table_name(db_name, "st")
            uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
            uid_t = self.get_uid_by_db_table_name(db_name, "t", self.TableType.REGULAR_TABLE)
            # drop table with incorrect sql
            tdSql.error(f"drop stable with `{uid_ct[0]}`;", expectErrInfo="STable not exist")
            tdSql.error(f"drop stable with `{uid_t[0]}`;", expectErrInfo="STable not exist")
            tdLog.info("Finish test case: test_abnormal_incorrect_table_type")
        except Exception as e:
            tdLog.exit("Failed to run test case test_abnormal_incorrect_table_type with msg: %s" % str(e))
        finally:
            tdSql.execute(f"drop database {db_name};")

    def test_abnormal_mixed_uid(self):
        """Verify dropping table with mixed uid
        """
        db_name = "test_abnormal_mixed_uid"
        tdLog.info("Start test case: test_abnormal_mixed_uid")
        # data for case test_abnormal_mixed_uidF
        tdLog.info("Prepare data for test case test_abnormal_mixed_uid")
        tdSql.execute(f"create database {db_name};")
        tdSql.execute(f"use {db_name};")
        tdSql.execute("create stable `st3\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
        tdSql.execute("create table `ct3_1\u00cf\u00ff` using `st3\u00bf\u200bfnn1` tags(1);")
        tdSql.execute("create table `t3\u00ef\u00fa` (ts timestamp, c1 int, c2 float);")
        tdLog.info("Finish preparing data for test case test_abnormal_mixed_uid of special characters")
        # get table uid
        uid_st = self.get_uid_by_db_table_name(db_name, "st")
        uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
        uid_t = self.get_uid_by_db_table_name(db_name, "t", self.TableType.REGULAR_TABLE)
        # drop table with incorrect sql
        tdSql.error(f"drop stable with `{uid_st[0]}`,`{uid_ct[0]}`;", expectErrInfo="syntax error")
        tdSql.error(f"drop table with `{uid_st[0]}`,`{uid_ct[0]}`,`{uid_t[0]}`;", expectErrInfo="Cannot drop super table in batch")
        tdSql.execute(f"drop database {db_name};")
        tdLog.info("Finish test case: test_abnormal_mixed_uid")

    def test_abnormal_system_tables(self):
        """Verify dropping system tables
        """
        try:
            uid_list = self.get_uid_by_db_name("information_schema", self.TableType.REGULAR_TABLE)
            uid = random.choice(uid_list)
            assert uid is None
        except Exception as e:
            tdLog.exit("Failed to run test case test_abnormal_system_tables with msg: %s" % str(e))

    def test_abnormal_drop_table_with_non_root_user(self):
        """Verify dropping table with non-root user
        """
        try:
            # create new user and grant create database priviledge
            tdSql.execute("create user test pass 'test';")
            tdSql.execute("alter user test createdb 1;")
            conn = taos.connect(user="test", password="test")
            cursor = conn.cursor()
            # create database and tables with new user
            tdLog.info("Prepare data for test case test_abnormal_drop_table_with_non_root_user")
            db_name = "test_abnormal_drop_table_with_non_root_user"
            cursor.execute(f"create database {db_name};")
            cursor.execute(f"use {db_name};")
            time.sleep(3)
            cursor.execute("create stable `st4\u00bf\u200bfnn1` (ts timestamp, c1 int) tags (t1 int);")
            cursor.execute("create table `ct4_1\u00cf\u00ff` using `st4\u00bf\u200bfnn1` tags(1);")
            cursor.execute("create table `t4\u00ef\u00fa` (ts timestamp, c1 int, c2 float);")
            tdLog.info("Finish preparing data for test case test_abnormal_drop_table_with_non_root_user of special characters")
            # get table uid
            uid_st = self.get_uid_by_db_table_name(db_name, "st")
            uid_ct = self.get_uid_by_db_table_name(db_name, "ct", self.TableType.CHILD_TABLE)
            uid_t = self.get_uid_by_db_table_name(db_name, "t", self.TableType.REGULAR_TABLE)
            # drop stable with sql by non-root user
            try:
                cursor.execute(f"drop stable with `{uid_st[0]}`;")
            except Exception as e:
                assert "Permission denied or target object not exist" in str(e)
                tdLog.info("Drop stable with non-root user failed as expected")
            # drop child table with sql by non-root user
            try:
                cursor.execute(f"drop table with `{uid_ct[0]}`;")
            except Exception as e:
                assert "Permission denied or target object not exist" in str(e)
                tdLog.info("Drop child table with non-root user failed as expected")
            # drop regular table with sql by non-root user
            try:
                cursor.execute(f"drop table with `{uid_t[0]}`;")
            except Exception as e:
                assert "Permission denied or target object not exist" in str(e)
                tdLog.info("Drop regular table with non-root user failed as expected")
            tdLog.info("Finish test case: test_abnormal_drop_table_with_non_root_user")
        except Exception as e:
            tdLog.exit("Failed to run test case test_abnormal_drop_table_with_non_root_user with msg: %s" % str(e))
        finally:
            tdSql.execute(f"drop database {db_name};")
            tdSql.execute("drop user test;")

    def run(self):
        # normal cases
        self.test_drop_single_table_by_uid()
        self.test_drop_multiple_tables_by_uid()
        self.test_uid_as_table_name()
        # abnormal cases
        self.test_abnormal_non_exist_uid()
        self.test_abnormal_incorrect_table_type()
        self.test_abnormal_mixed_uid()
        self.test_abnormal_system_tables()
        self.test_abnormal_drop_table_with_non_root_user()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
