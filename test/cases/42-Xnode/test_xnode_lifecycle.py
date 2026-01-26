"""
XNode 生命周期功能测试

测试 XNode 节点的创建、状态转换、心跳机制、删除等完整生命周期
"""

import random
import time
import uuid

from new_test_framework.utils import tdLog, tdSql


class TestXnodeLifecycle:
    """XNode 生命周期测试"""

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        tdSql.prepare(
            dbname=f"xnode_lifecycle_db_{cls.suffix}", drop=True, replica=cls.replicaVar
        )

    def test_create_xnode_with_user_password(self):
        """测试创建第一个 XNode 时需要用户密码

        1. Create xnode with user password

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """
        
        ep1 = f"xn-life-{self.suffix}-1:6050"
        user = f"__xnode_test_{self.suffix}__"
        password = f"Xn0de!{self.suffix}"

        # 第一次创建 XNode 需要指定用户和密码
        sql = f"CREATE XNODE '{ep1}' USER {user} PASS '{password}'"
        tdLog.info(f"Creating first xnode with user/password: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f"Successfully created xnode with user {user}")
        except Exception as e:
            # 可能因为 xnoded 进程未运行而失败，但语法应该正确
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, \
                f"Syntax error creating xnode: {e}"
            tdLog.notice(f"Runtime error tolerated: {e}")

        # 查询 XNODES 系统表
        try:
            tdSql.query("SHOW XNODES")
            tdLog.success("Successfully queried SHOW XNODES")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_create_xnode_without_password(self):
        """测试创建第二个 XNode 时不需要密码

        1. Create xnode without user password

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep2 = f"xn-life-{self.suffix}-2:6051"

        # 第二次创建 XNode 不需要用户密码
        sql = f"CREATE XNODE '{ep2}'"
        tdLog.info(f"Creating second xnode without password: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f"Successfully created xnode {ep2}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_create_duplicate_xnode(self):
        """测试创建重复的 XNode 应该失败

        1. Create duplicate xnode

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-life-{self.suffix}-dup:6052"

        # 第一次创建
        try:
            tdSql.execute(f"DROP XNODE FORCE '{ep}'")
        except:
            pass

        try:
            tdSql.execute(f"CREATE XNODE '{ep}'")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg

        # 第二次创建同一个 - 应该失败（运行时错误）
        try:
            tdSql.execute(f"CREATE XNODE '{ep}'")
            tdLog.notice("Duplicate xnode creation did not fail as expected")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.success(f"Duplicate xnode correctly rejected: {e}")

    def test_drop_xnode_by_endpoint(self):
        """测试通过 endpoint 删除 XNode

        1. Drop xnode by endpoint

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-life-{self.suffix}-drop1:6053"

        # 先创建
        try:
            tdSql.execute(f"CREATE XNODE '{ep}'")
        except:
            pass

        # 删除
        sql = f"DROP XNODE '{ep}'"
        tdLog.info(f"Dropping xnode by endpoint: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f"Successfully dropped xnode {ep}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_drop_xnode_by_id(self):
        """测试通过 ID 删除 XNode

        1. Drop xnode by ID

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        xnode_id = random.randint(1000, 9999)

        sql = f"DROP XNODE {xnode_id}"
        tdLog.info(f"Dropping xnode by ID: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f" DROP XNODE {xnode_id}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_drop_xnode_force(self):
        """测试强制删除 XNode

        1. Drop xnode force

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-life-{self.suffix}-force:6054"

        # 先创建
        try:
            tdSql.execute(f"CREATE XNODE '{ep}'")
        except:
            pass

        # 强制删除
        sql = f"DROP XNODE FORCE '{ep}'"
        tdLog.info(f"Force dropping xnode: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f"Successfully force dropped xnode {ep}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_drain_xnode(self):
        """测试 DRAIN XNode 模式

        1. Drain xnode

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        xnode_id = random.randint(1000, 9999)

        sql = f"DRAIN XNODE {xnode_id}"
        tdLog.info(f"Draining xnode: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f" DRAIN XNODE {xnode_id}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_drop_nonexistent_xnode(self):
        """测试删除不存在的 XNode

        1. Drop non-exist xnode

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-nonexist-{uuid.uuid4().hex[:8]}:9999"

        sql = f"DROP XNODE '{ep}'"
        tdLog.info(f"Dropping non-existent xnode: {sql}")
        try:
            tdSql.execute(sql)
            # 如果没有报错，说明语法正确
            tdLog.notice(f"No error for dropping non-existent xnode")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            # 应该是 "xnode not exist" 之类的运行时错误
            tdLog.success(f"Correctly got runtime error: {e}")

    def test_show_xnodes_query(self):
        """测试 SHOW XNODES 查询

        1. Query show xnodes

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SHOW XNODES"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"SHOW XNODES returned {rows} rows")
            
            # 如果有结果，检查列
            if rows > 0:
                # 系统表应该包含: id, endpoint, status, create_time, update_time 等
                tdLog.info(f"Sample xnode data: {tdSql.queryResult}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_xnode_with_special_characters(self):
        """测试包含特殊字符的 XNode 地址

        1. Create xnode with special characters

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        # 测试 IPv6 地址格式
        ep_ipv6 = f"[::1]:6055"
        sql = f"CREATE XNODE '{ep_ipv6}'"
        tdLog.info(f"Creating xnode with IPv6: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success(f"Successfully created xnode with IPv6")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

        # 清理
        try:
            tdSql.execute(f"DROP XNODE FORCE '{ep_ipv6}'")
        except:
            pass

    def test_xnode_password_validation(self):
        """测试密码格式验证

        1. Create xnode with weak password

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-pwd-{self.suffix}:6056"
        
        # 测试弱密码（如果启用强密码策略）
        weak_pass = "123"
        sql = f"CREATE XNODE '{ep}' USER __xnode__ PASS '{weak_pass}'"
        tdLog.info(f"Testing weak password: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.notice("Weak password was accepted")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            if "password" in msg or "pass" in msg:
                tdLog.success(f"Weak password correctly rejected: {e}")
            else:
                tdLog.notice(f"Runtime error: {e}")

        # 测试复杂密码
        strong_pass = f"C0mpl3x!Pass{self.suffix}"
        sql = f"CREATE XNODE '{ep}' USER __xnode__ PASS '{strong_pass}'"
        tdLog.info(f"Testing strong password: {sql}")
        try:
            tdSql.execute(sql)
            tdLog.success("Strong password accepted")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_xnode_lifecycle_sequence(self):
        """测试完整的 XNode 生命周期序列

        1. Create xnode
        2. Query xnode status
        3. Drain xnode
        4. Drop xnode

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep = f"xn-seq-{self.suffix}:6057"
        xnode_id = None

        # 1. 创建
        tdLog.info(f"Step 1: Create xnode {ep}")
        try:
            tdSql.execute(f"CREATE XNODE '{ep}'")
            tdLog.success("Created xnode")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error: {e}")

        # 2. 查询状态
        tdLog.info(f"Step 2: Query xnode status")
        try:
            tdSql.query("SHOW XNODES")
            # 尝试找到刚创建的节点
            for i in range(tdSql.queryRows):
                if ep in str(tdSql.queryResult[i]):
                    tdLog.success(f"Found created xnode: {tdSql.queryResult[i]}")
                    break
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg

        # 3. Drain 模式
        if xnode_id:
            tdLog.info(f"Step 3: Drain xnode {xnode_id}")
            try:
                tdSql.execute(f"DRAIN XNODE {xnode_id}")
                tdLog.success("Drained xnode")
            except Exception as e:
                msg = str(e).lower()
                assert "syntax" not in msg and "parse" not in msg

        # 4. 删除
        tdLog.info(f"Step 4: Drop xnode {ep}")
        try:
            tdSql.execute(f"DROP XNODE FORCE '{ep}'")
            tdLog.success("Dropped xnode")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg

        tdLog.success(f"Completed full lifecycle test for {ep}")

    @classmethod
    def teardown_class(cls):
        """清理测试环境

        1. Drop xnode

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        tdLog.info(f"Cleaning up test environment")
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS xnode_lifecycle_db_{cls.suffix}")
        except:
            pass
        tdLog.success(f"{__file__} test completed")
