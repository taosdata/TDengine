from new_test_framework.utils import tdLog, tdSql, etool, TDSql

import os
import sys

import taos

_FQ_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..",
                                    "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from federated_query_common import ExtSrcEnv, FederatedQueryCaseHelper, FederatedQueryVersionedMixin


class TestVtableAuthExtSource(FederatedQueryVersionedMixin):
    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        FederatedQueryCaseHelper(__file__).require_external_source_feature()
        ExtSrcEnv.ensure_env()

    def _normal_sql(self, user):
        conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path,
                            user=user, password="Test1234")
        sql = TDSql()
        sql.init(conn.cursor())
        return conn, sql

    def _check_values(self, sql, table):
        sql.query(f"select val from {table} order by ts")
        sql.checkRows(5)
        for index in range(5):
            sql.checkData(index, 0, index + 1)

    def do_external_source_auth(self):
        db = "vt_ext_auth_local"
        origin = "vt_ext_auth_origin"
        for database in (db, origin):
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database}")
        tdSql.execute(f"create table {origin}.local_t(ts timestamp, val int)")
        tdSql.execute(f"insert into {origin}.local_t values(1704067200000, 1)")

        def check_source(src):
            user = f"{src}_user"
            tdSql.execute(f"drop user if exists {user}")
            tdSql.execute(f"create user {user} pass 'Test1234' sysinfo 0")
            tdSql.execute(f"drop database {db}")
            # 外部表时间戳精度为纳秒级，本地库精度需匹配。
            tdSql.execute(f"create database {db} precision 'ns'")
            tdSql.query("show external sources")
            row = next(row for row in tdSql.queryResult if row[0] == src)
            # 三段式外部引用的中间段是该 provider 的命名空间：关系库为
            # database，Oracle 没有 per-source database（DATABASE 段是 Easy
            # Connect 服务名），命名空间是 schema。
            source_type = str(row[1]).lower()
            namespace = row[7] if source_type == "oracle" else row[6]
            # Oracle NUMBER(10) 映射为 BIGINT，虚拟表列类型必须与之匹配。
            val_type = "bigint" if source_type == "oracle" else "int"
            ref = f"{src}.{namespace}.src_t"
            tdSql.execute(f"create vtable {db}.root_v(ts timestamp, val {val_type} from {ref}.val)")
            tdSql.execute(f"create stable {db}.vst(ts timestamp, val {val_type}) tags(group_id int) virtual 1")
            tdSql.execute(f"create vtable {db}.root_child({ref}.val) using {db}.vst tags(1)")
            conn, sql = self._normal_sql(user)
            try:
                sql.error(f"select val from {db}.root_v", expectedErrno=0x26E3)
                sql.error(f"create vtable {db}.blocked(ts timestamp, val {val_type} from {ref}.val)",
                          expectedErrno=0x26E3)
                tdSql.execute(f"grant use on database {db} to {user}")
                tdSql.execute("reset query cache")
                sql.error(f"select val from {db}.root_v", expectedErrno=0x2644)
                sql.error(f"create vtable {db}.blocked(ts timestamp, val {val_type} from {ref}.val)",
                          expectedErrno=0x2644)
                for table in ("root_v", "vst"):
                    tdSql.execute(f"grant select on table {db}.{table} to {user}")
                    tdSql.execute("reset query cache")
                    self._check_values(sql, f"{db}.{table}")
                self._check_values(sql, f"{db}.root_child")
                tdSql.execute(f"grant create table on database {db} to {user}")
                tdSql.execute("reset query cache")
                for table, path in (("normal_full", ref), ("normal_short", f"{src}.src_t")):
                    sql.execute(f"create vtable {db}.{table}(ts timestamp, val {val_type} from {path}.val)")
                    tdSql.execute(f"grant select on table {db}.{table} to {user}")
                    tdSql.execute("reset query cache")
                    self._check_values(sql, f"{db}.{table}")
                sql.execute(f"create vtable {db}.user_child({ref}.val) using {db}.vst tags(2)")
                self._check_values(sql, f"{db}.user_child")
                tdSql.execute(f"grant use on database {origin} to {user}")
                tdSql.execute("reset query cache")
                # 引用未授权 SELECT 的本地表时按表级权限拒绝（0x26E6）。
                sql.error(f"create vtable {db}.mixed(ts timestamp, val {val_type} from {ref}.val, "
                          f"local_val int from {origin}.local_t.val)", expectedErrno=0x26E6)
                tdSql.execute(f"grant select on table {origin}.local_t to {user}")
                tdSql.execute("reset query cache")
                sql.execute(f"create vtable {db}.mixed(ts timestamp, val {val_type} from {ref}.val, "
                            f"local_val int from {origin}.local_t.val)")
                sql.error(f"alter vtable {db}.root_v alter column val set {ref}.val",
                          expectedErrno=0x2644)
                tdSql.execute(f"grant alter on table {db}.root_v to {user}")
                tdSql.execute("reset query cache")
                sql.execute(f"alter vtable {db}.root_v alter column val set {ref}.val")
                self._check_values(sql, f"{db}.root_v")
                sql.execute(f"alter table {db}.root_v alter column val set {ref}.val")
                self._check_values(sql, f"{db}.root_v")
                tdSql.execute(f"revoke select on table {origin}.local_t from {user}")
                tdSql.execute("reset query cache")
                sql.error(f"alter vtable {db}.root_v alter column val set {origin}.local_t.val",
                          expectedErrno=0x26E6)
                tdSql.execute(f"revoke use on database {origin} from {user}")
            finally:
                sql.close()
                conn.close()
                tdSql.execute(f"drop user if exists {user}")

        try:
            self._with_std_sources("vt_ext_auth", check_source, skip_influx=True,
                                   skip_sqlserver=True)
        finally:
            for database in (db, origin):
                tdSql.execute(f"drop database if exists {database}")
        print("外部引用虚拟表使用权限与本地权限边界 [passed]")

    def test_external_source_auth(self):
        """普通用户使用 root 外部源创建和查询虚拟表，本地权限仍生效。

        普通用户免授权使用外部源创建/查询虚拟表，同时验证目标库表权限与本地引用权限仍然生效。

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 wpan 补齐外部引用虚拟表权限回归
        """
        self.do_external_source_auth()

    def do_external_series_auth(self):
        user = "vt_series_auth_user"
        db = "vt_series_auth_local"
        source = "vt_series_auth_src"
        remote_db = "vt_series_auth_remote"
        cfg = self._influx_cfg()
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"drop database if exists {db}")
        # InfluxDB 行协议时间戳为纳秒，本地库精度需与外部表一致。
        tdSql.execute(f"create database {db} precision 'ns'")
        tdSql.execute(f"create user {user} pass 'Test1234' sysinfo 0")
        tdSql.execute(f"grant use on database {db} to {user}")
        tdSql.execute(f"grant create table on database {db} to {user}")
        ExtSrcEnv.influx_create_db_cfg(cfg, remote_db)
        try:
            ExtSrcEnv.influx_write_cfg(cfg, remote_db, [
                f"measure,site=one val={index}i {(1704067200000 + index * 60000) * 1000000}"
                for index in range(1, 6)
            ])
            self._mk_influx_real(source, database=remote_db)
            conn, sql = self._normal_sql(user)
            try:
                series = f"series remote_series as {source}.{remote_db}.measure (site='one')"
                sql.execute(f"create vtable {db}.normal_series(ts timestamp, val bigint from remote_series.val) {series}")
                sql.execute(f"create stable {db}.vst(ts timestamp, val bigint) tags(group_id int) virtual 1")
                sql.execute(f"create vtable {db}.child_series(remote_series.val) using {db}.vst tags(1) {series}")
                for table in ("normal_series", "vst"):
                    tdSql.execute(f"grant select on table {db}.{table} to {user}")
                    tdSql.execute("reset query cache")
                    self._check_values(sql, f"{db}.{table}")
                self._check_values(sql, f"{db}.child_series")
            finally:
                sql.close()
                conn.close()
        finally:
            self._cleanup(source)
            ExtSrcEnv.influx_drop_db_cfg(cfg, remote_db)
            tdSql.execute(f"drop user if exists {user}")
            tdSql.execute(f"drop database if exists {db}")
        print("普通用户外部 SERIES 引用权限 [passed]")

    def test_external_series_auth(self):
        """外部 SERIES 别名免本地源权限，目标数据库权限仍生效。

        普通用户通过 SERIES 别名引用 InfluxDB 外部源创建虚拟表，验证目标库权限边界仍然生效。

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-09 wpan 补齐外部 SERIES 引用权限回归
        """
        self.do_external_series_auth()
