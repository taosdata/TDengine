import logging
from time import sleep
import pytest
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.env import ENV

audit_test_logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def input_data():
    audit_test_logger.info("before auditlog test...")
    TaosAdapter.run_sql(ENV.taosadapter_host, f"drop database if exists ci_audit")
    yield
    audit_test_logger.info("after auditlog test...")


def operation_check(sql: str, operation_type: str):
    TaosAdapter.run_sql(ENV.taosadapter_host, "delete from audit.operations;")
    TaosAdapter.run_sql(ENV.taosadapter_host, sql)
    if operation_type in ["createTable", "dropTable"]:
        sleep(5)
    operation_log = TaosAdapter.run_sql(
        ENV.taosadapter_host, "select * from audit.operations;"
    )
    assert operation_log["data"]
    assert operation_type in operation_log["data"][0], audit_test_logger.error(
        f"test case failed: {operation_type} operation is not in auditlog"
    )
    TaosAdapter.run_sql(ENV.taosadapter_host, "delete from audit.operations;")


def test_audit_operation():
    operations = [
        {"createDB": "create database ci_audit"},
        {"createStb": "create stable ci_audit.stb (ts timestamp,c0 int) tags(t0 int)"},
        {"createTable": "create table ci_audit.tb using ci_audit.stb tags(1)"},
        {"dropTable": "drop table ci_audit.tb"},
        {"dropStb": "drop stable ci_audit.stb"},
        {"dropDB": "drop database ci_audit"},
    ]
    for operation in operations:
        operation_check(list(operation.values())[0], list(operation.keys())[0])
