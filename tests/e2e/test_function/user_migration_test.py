import logging
import pytest
import taosws
import urllib.parse

from testng_taosx.util import Util
from testng_taosx.file import TaskType
from testng_taosx.requests_wrapper import http
from testng_taosx.constant import *

user_migration_logger = logging.getLogger(__name__)

MIGRATE_USER_1 = "test1"
MIGRATE_USER_2 = "test2"
MIGRATE_USER_PASS = "tBase1234!@#$"

TEST_DATABASE_1 = "ci_test_privileges1"
TEST_DATABASE_2 = "ci_test_privileges2"


def prepare_source_data(source_conn):
    """
    准备需要同步的用户数据
    2 个用户: test1/tBase1234!@#$ 白名单 [192.168.2.18,192.168.1.45], test2/tBase1234!@#$ 白名单[192.168.1.45/26]
    2 个数据库：ci_test_privileges1, ci_test_privileges2，ci_test_privileges3
    3 个 topic：ci_test_privileges1，ci_test_privileges2，ci_test_privileges2.stb1，ci_test_privileges3
    test1 有 ci_test_privileges1, ci_test_privileges2 的读写权限，有 ci_test_privileges1 和 ci_test_privileges2 的订阅权限
    test2 有 ci_test_privileges1 的读权限，有 ci_test_privileges2 的读写权限，有 ci_test_privileges2 和 ci_test_privileges2.stb1 的订阅权限
    """
    # 1.创建库表
    source_conn.execute("create database if not exists `ci_test_privileges1`")
    source_conn.execute("create database if not exists `ci_test_privileges2`")
    source_conn.execute(
        "create stable if not exists `ci_test_privileges2`.`stb1` (`ts` TIMESTAMP, `c0` int, `c1` varchar(256), `c2` double, `c3` bool) tags (`t0` varchar(256), `t1` int)"
    )
    source_conn.execute("create database if not exists `ci_test_privileges3`")

    # 2.创建 topic
    source_conn.execute(
        "create topic if not exists `ci_test_privileges1` as database ci_test_privileges1"
    )
    source_conn.execute(
        "create topic if not exists `ci_test_privileges2` as database ci_test_privileges2"
    )
    source_conn.execute(
        "create topic if not exists `ci_test_privileges2.stb1` as stable `ci_test_privileges2`.`stb1`"
    )
    # 3.创建用户
    # 若用户不存在则创建
    # print(f"show_users_result: {show_users_result.rows_iter()}")
    try:
        source_conn.execute(
            "CREATE USER `test1` PASS 'tBase1234!@#$' createdb 1 host '192.168.2.18','192.168.1.45'"
        )
    except Exception as err:
        user_migration_logger.warning(f"创建用户 test1 发生错误: {err}")

    try:
        source_conn.execute(
            "CREATE USER `test2` PASS 'tBase1234!@#$' host '192.168.1.45/26'"
        )
    except Exception as err:
        user_migration_logger.warning(f"创建用户 test2 发生错误: {err}")
    # 4.1 权限赋予 test1
    source_conn.execute("GRANT Read ON `ci_test_privileges1`.*  to `test1`")
    source_conn.execute("GRANT Write ON `ci_test_privileges1`.*  to `test1`")
    source_conn.execute("GRANT Read ON `ci_test_privileges2`.*  to `test1`")
    source_conn.execute("GRANT Write ON `ci_test_privileges2`.*  to `test1`")
    source_conn.execute("GRANT Read ON `ci_test_privileges3`.*  to `test1`")
    source_conn.execute("GRANT Write ON `ci_test_privileges3`.*  to `test1`")
    source_conn.execute("GRANT subscribe ON `ci_test_privileges1` to `test1`")
    source_conn.execute("GRANT subscribe ON `ci_test_privileges2` to `test1`")
    source_conn.execute("GRANT subscribe ON `ci_test_privileges2.stb1` to `test1`")
    # 4.2 权限赋予 test2
    source_conn.execute("GRANT Read ON `ci_test_privileges1`.*  to `test2`")
    source_conn.execute("GRANT Read ON `ci_test_privileges2`.*  to `test2`")
    source_conn.execute("GRANT Write ON `ci_test_privileges2`.*  to `test2`")
    source_conn.execute("GRANT subscribe ON `ci_test_privileges2` to `test2`")
    source_conn.execute("GRANT subscribe ON `ci_test_privileges2.stb1` to `test2`")


def prepare_target_data(target_conn):
    """
    准备目标数据
    目标库有 ci_test_privileges1 和 ci_test_privileges2
    topic 有 ci_test_privileges1 和 ci_test_privileges2
    """
    target_conn.execute("create database if not exists `ci_test_privileges1`")
    target_conn.execute("create database if not exists `ci_test_privileges2`")
    target_conn.execute(
        "create topic if not exists `ci_test_privileges1` as database ci_test_privileges1"
    )


@pytest.fixture(scope="module")
def input_data():
    user_migration_logger.info("before all user_migration test...")
    env_data = Util.get_env_data()
    source_dsn = f"ws://{env_data['data_source']['user_migration'][0]}"
    target_dsn = f"ws://{env_data['username']}:{env_data['password']}@{env_data['taosadapter_host']}:{env_data['taosadapter_port']}"
    source_conn = None
    target_conn = None
    try:
        source_conn = taosws.connect(source_dsn)
        prepare_source_data(source_conn)
        source_conn.close()
    except Exception as err:
        user_migration_logger.error(f"数据准备发生错误: {source_dsn} , ErrMessage:{err}")
        raise err
    finally:
        if source_conn:
            source_conn.close()
    try:
        target_conn = taosws.connect(target_dsn)
        prepare_target_data(target_conn)
        target_conn.close()
    except Exception as err:
        user_migration_logger.error(f"数据准备发生错误: {target_dsn} , ErrMessage:{err}")
        raise err
    finally:
        if target_conn:
            target_conn.close()

    yield env_data
    user_migration_logger.info("after all user_migration test...")


@pytest.mark.parametrize(
    "passwords,privileges,whitelist",
    [
        (True, False, False),
        (True, True, False),
        (True, False, True),
        (True, True, True),
    ],
)
@pytest.mark.skip(reason="由于库不兼容，容器内的需要等待出 3.3.3.0 发布之后再重新构建镜像，暂时不执行")
def test_migration_user(passwords, privileges, whitelist, input_data):
    """
    用例概述：测试迁移用户
    passwords,privileges,whitelist 对应接口需要发送的数据，取值类型为 bool
    验证点：
        1.用户名及密码同步成功之后，使用迁移的用户可以正常与目标库建立连接
        2.权限同步成功之后，迁移后的用户信息包含正确的权限信息
        3.白名单同步成功之后，迁移后的用户信息包含正确的白名单信息
    """
    env_data = input_data
    # 1.执行迁移
    source_dsn = env_data["data_source"]["user_migration"][0]
    split_result = source_dsn.split("@")
    source_server = None
    if split_result.__len__() == 2:
        source_server = f"http://{split_result[1]}"
    else:
        source_server = f"http://{split_result[0]}"
    post_body = {
        "server": source_server,
        "passwords": passwords,
        "privileges": privileges,
        "whitelist": whitelist,
    }
    import_response = http.request(
        "POST",
        f"{env_data['taos_explorer_root_endpoint']}{EXPLORER_BASE_URL}/import",
        json=post_body,
    )
    assert (
        import_response.status_code == 200
    ), f"import user failed, response: {import_response.text}"
    encoded_pass = urllib.parse.quote(MIGRATE_USER_PASS)
    target_user1_dsn = f"ws://{MIGRATE_USER_1}:{encoded_pass}@{env_data['taosadapter_host']}:{env_data['taosadapter_port']}"
    target_user2_dsn = f"ws://{MIGRATE_USER_2}:{encoded_pass}@{env_data['taosadapter_host']}:{env_data['taosadapter_port']}"
    # 2.迁移之后新的用户的应能正常建立连接
    user1_conn = None
    try:
        user1_conn = taosws.connect(target_user1_dsn)
        user_migration_logger.info(f"target_user1_conn 连接建立成功")
        user1_conn.query("select now()")
    except Exception as err:
        user_migration_logger.error(
            f"target_user1_conn 连接建立失败: {err}, dsn : {target_user1_dsn}"
        )
        raise err
    finally:
        if user1_conn:
            user1_conn.close()

    user2_conn = None
    try:
        user2_conn = taosws.connect(target_user2_dsn)
        user_migration_logger.info(f"target_user2_conn 连接建立成功")
        user2_conn.query("select now()")
    except Exception as err:
        user_migration_logger.error(
            f"target_user2_conn 连接建立失败: {err}, dsn: {target_user2_dsn}"
        )
        raise err
    if user2_conn:
        user2_conn.close()

    target_root_conn = None
    try:

        target_root_conn = taosws.connect(
            f"ws://{env_data['username']}:{env_data['password']}@{env_data['taosadapter_host']}:{env_data['taosadapter_port']}"
        )
        # 3.若迁移权限，则迁移后的用户应该包含正确的权限信息
        # if privileges:
        # 3.若迁移白名单，则迁移后的用户信息应该包含正确的白名单信息
        query_privileges = target_root_conn.query(
            f"select * from information_schema.ins_user_privileges"
        )
        privileges_list = []
        for row in query_privileges:
            privileges_list.append(f"{row[0]}::{row[1]}::{row[2]}")
        if privileges:
            assert (
                f"{MIGRATE_USER_1}::read::ci_test_privileges1" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::read::ci_test_privileges1 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::write::ci_test_privileges1" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges1 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::read::ci_test_privileges2" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::read::ci_test_privileges2 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::write::ci_test_privileges2" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges2 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::subscribe::ci_test_privileges1" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges2 not in privileges_list, {privileges_list}"
            )

            assert (
                f"{MIGRATE_USER_2}::read::ci_test_privileges1" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::read::ci_test_privileges1 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_2}::read::ci_test_privileges2" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::read::ci_test_privileges2 not in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_2}::write::ci_test_privileges2" in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::write::ci_test_privileges2 not in privileges_list, {privileges_list}"
            )
        else:
            assert (
                f"{MIGRATE_USER_1}::read::ci_test_privileges1" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges1 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::write::ci_test_privileges1" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges1 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::read::ci_test_privileges2" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::read::ci_test_privileges2 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::write::ci_test_privileges2" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::write::ci_test_privileges2 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_1}::subscribe::ci_test_privileges1"
                not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_1}::subscribe::ci_test_privileges1 in privileges_list, {privileges_list}"
            )

            assert (
                f"{MIGRATE_USER_2}::read::ci_test_privileges1" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::read::ci_test_privileges1 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_2}::read::ci_test_privileges2" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::read::ci_test_privileges2 in privileges_list, {privileges_list}"
            )
            assert (
                f"{MIGRATE_USER_2}::write::ci_test_privileges2" not in privileges_list
            ), user_migration_logger.error(
                f"{MIGRATE_USER_2}::write::ci_test_privileges2 in privileges_list, {privileges_list}"
            )

        # 4.若迁移白名单，则迁移后的用户信息应该包含正确的白名单信息
        # if whitelist:
        query_users = target_root_conn.query(
            f"select * from information_schema.ins_users"
        )
        for row in query_users:
            if row[0] == MIGRATE_USER_1:
                # 应当有创建库的权限
                assert 1 == row[4], f"{MIGRATE_USER_1} 应当有创建库的权限，实际为：{row}"
                if whitelist:
                    assert "192.168.2.18" in row[6]
                    assert "192.168.1.45" in row[6]
                else:
                    assert "192.168.2.18" not in row[6]
                    assert "192.168.1.45" not in row[6]
            elif row[0] == MIGRATE_USER_2:
                assert 0 == row[4], f"{MIGRATE_USER_2} 不应该有创建库的权限, 实际为: {row}"
                if whitelist:
                    assert "192.168.1.45/26" in row[6]
                else:
                    assert "192.168.1.45/26" not in row[6]
        # 5.清理迁移的用户信息
        target_root_conn.execute(f"DROP USER {MIGRATE_USER_1}")
        target_root_conn.execute(f"DROP USER {MIGRATE_USER_2}")
    except Exception as err:
        user_migration_logger.error(f"用户迁移验证错误: {err}")
        raise err
    finally:
        if target_root_conn:
            target_root_conn.close()
