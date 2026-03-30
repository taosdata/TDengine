import socket
import time

from new_test_framework.utils import tdLog, tdSql


def _is_local_port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) != 0


def _pick_local_ports():
    for port in range(16030, 20000):
        mqtt_port = port + 53
        if _is_local_port_free(port) and _is_local_port_free(mqtt_port):
            return str(port), str(mqtt_port)
    raise RuntimeError("failed to find free local ports for taosd audit test")


SERVER_PORT, MQTT_PORT = _pick_local_ports()


class TestTaosdAudit:
    hostname = "localhost"
    serverPort = SERVER_PORT
    mqttPort = MQTT_PORT
    rpcDebugFlagVal = "143"

    clientCfgDict = {
        "serverPort": serverPort,
        "firstEp": f"{hostname}:{serverPort}",
        "secondEp": f"{hostname}:{serverPort}",
        "rpcDebugFlag": rpcDebugFlagVal,
        "fqdn": hostname,
    }

    updatecfgDict = {
        "clientCfg": clientCfgDict,
        "serverPort": serverPort,
        "firstEp": f"{hostname}:{serverPort}",
        "secondEp": f"{hostname}:{serverPort}",
        "fqdn": hostname,
        "audit": "1",
        "auditLevel": "5",
        "auditCreateTable": "1",
        "auditInterval": "500",
        "auditHttps": "0",
        "auditUseToken": "1",
        "encryptAlgorithm": "sm4",
        "encryptScope": "all",
        "enableAuditDelete": "1",
        "enableAuditInsert": "1",
        "enableAuditSelect": "1",
        "mqttPort": mqttPort,
        "uDebugFlag": "143",
    }

    encryptConfig = {
        "svrKey": "sdfsadfasdfasfas",
        "dbKey": "sdfsadfasdfasfas",
        "dataKey": "sdfsadfasdfasfas",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True,
    }

    auditDb = "audit"
    businessDb = "db3"
    auditUser = "audit"
    auditToken = "audit_token"
    auditPassword = "123456Ab@"

    def wait_until(self, sql, checker, timeout=30, desc="condition"):
        last_rows = None
        for _ in range(timeout):
            tdSql.query(sql)
            last_rows = tdSql.queryResult
            if checker(last_rows):
                return last_rows
            time.sleep(1)

        tdSql.query(sql)
        raise AssertionError(
            f"waited {timeout}s for {desc}, sql={sql}, rows={tdSql.queryResult}"
        )

    def prepare_audit_sink(self):
        tdLog.info("create audit database")
        tdSql.execute(
            "create database audit precision 'ns' is_audit 1 wal_level 2 "
            "ENCRYPT_ALGORITHM 'SM4-CBC'"
        )

        tdLog.info("create audit user")
        tdSql.execute(f"create user {self.auditUser} pass '{self.auditPassword}' sysinfo 0")

        tdLog.info("grant SYSAUDIT_LOG to audit user")
        tdSql.execute(f"grant role `SYSAUDIT_LOG` to {self.auditUser}")

        tdLog.info("create audit token")
        token = tdSql.getFirstValue(
            f"create token {self.auditToken} from user {self.auditUser}"
        )
        assert isinstance(token, str) and len(token) > 0

        # audit db/token is delivered to dnode by status heartbeat, not synchronously.
        time.sleep(3)

    def generate_audit_records(self):
        tdLog.info("create business database and tables")
        tdSql.execute(f"create database {self.businessDb} vgroups 4")
        tdSql.execute(
            f"create table {self.businessDb}.stb (ts timestamp, f int) tags (t int)"
        )
        tdSql.execute(
            f"create table {self.businessDb}.tb using {self.businessDb}.stb tags (1)"
        )

        tdLog.info("generate insert/select/delete audit records")
        tdSql.execute(
            f"insert into {self.businessDb}.tb using {self.businessDb}.stb tags (1) values (now, 2)"
        )
        tdSql.query(f"select * from {self.businessDb}.stb")
        tdSql.execute(f"delete from {self.businessDb}.tb")

    def test_taosd_audit(self):
        """Taosd local audit persistence

        1. Enable audit without starting taoskeeper or an HTTP receiver
        2. Create the audit database, audit user and token
        3. Generate createTable/insert/select/delete audit events
        4. Verify taosd creates `audit.operations` and `audit.t_operations_<cluster>`
        5. Verify audit records are flushed into the local audit database

        Since: v3.4.0.10

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Added direct local audit sink validation
        """
        tdSql.prepare()

        self.prepare_audit_sink()
        self.generate_audit_records()

        stable_sql = (
            "select count(*) from information_schema.ins_stables "
            f"where db_name = '{self.auditDb}' and stable_name = 'operations'"
        )
        subtable_sql = (
            "select count(*) from information_schema.ins_tables "
            f"where db_name = '{self.auditDb}' and stable_name = 'operations' "
            "and table_name like 't_operations_%'"
        )
        record_sql = (
            "select operation, details from audit.operations "
            f"where `db` = '{self.businessDb}' "
            "and operation in ('createTable', 'insert', 'select', 'delete') "
            "order by ts"
        )

        self.wait_until(
            stable_sql,
            lambda rows: rows and rows[0][0] == 1,
            timeout=30,
            desc="audit stable operations to be created locally",
        )
        self.wait_until(
            subtable_sql,
            lambda rows: rows and rows[0][0] >= 1,
            timeout=30,
            desc="audit child table to be created locally",
        )
        records = self.wait_until(
            record_sql,
            lambda rows: rows
            and {"createTable", "insert", "select", "delete"}.issubset(
                {row[0] for row in rows}
            ),
            timeout=30,
            desc="expected audit records to be flushed locally",
        )

        details_by_op = {row[0]: row[1] for row in records}
        delete_detail = details_by_op["delete"]
        assert delete_detail is not None
        assert "delete from db3.tb" in delete_detail.lower()
