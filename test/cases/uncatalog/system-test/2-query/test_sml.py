from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom

import os
import platform
import subprocess


class TestSml:
    updatecfgDict = {'clientCfg': {'smlChildTableName': 'dataModelName', 'smlAutoChildTableNameDelimiter': '', 'fqdn': 'localhost', 'smlDot2Underline': 0}, 'fqdn': 'localhost'}
    print("===================: ", updatecfgDict)

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def checkContent(self, dbname="sml_db"):
        simClientCfg = os.path.join(tdDnodes.getSimCfgPath(), "taos.cfg")
        exe_file = "sml_test" if platform.system() != "Windows" else "sml_test.exe"
        cmdStr = os.path.join(tdCom.getBuildPath(), "build", "bin", exe_file) + " " + simClientCfg
        tdLog.info(cmdStr)
        ret =subprocess.run(cmdStr, shell=True, capture_output=True)
        tdLog.info(f"{cmdStr} output: {ret.stdout.decode()}")
        if ret.returncode != 0:
            tdLog.exit("sml_test ret != 0")

        tdSql.query(f"select * from ts3303.stb2")
        tdSql.query(f"select * from ts3303.meters")

        # tdSql.execute('use sml_db')
        tdSql.query(f"select * from {dbname}.t_b7d815c9222ca64cdf2614c61de8f211")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, '2016-01-01 08:00:07.000')
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, 200)
        tdSql.checkData(0, 3, 15)
        tdSql.checkData(0, 4, 24.5208)
        tdSql.checkData(0, 5, 28.09377)
        tdSql.checkData(0, 6, 428)
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, 304)
        tdSql.checkData(0, 9, 0)
        tdSql.checkData(0, 10, 25)

        tdSql.query(f"select * from {dbname}.readings")
        tdSql.checkRows(9)

        tdSql.query(f"select distinct tbname from {dbname}.readings")
        tdSql.checkRows(4)

        tdSql.query(f"select * from {dbname}.t_0799064f5487946e5d22164a822acfc8 order by _ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "kk")
        tdSql.checkData(1, 3, "")

        tdSql.query(f"select distinct tbname from {dbname}.`sys.if.bytes.out`")
        tdSql.checkRows(2)

        tdSql.query(f"select * from {dbname}.t_fc70dec6677d4277c5d9799c4da806da order by _ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1.300000000)
        tdSql.checkData(1, 1,13.000000000)

        tdSql.query(f"select * from {dbname}.`sys.procs.running`")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42.000000000)
        tdSql.checkData(0, 2, "web01")

        tdSql.query(f"select distinct tbname from {dbname}.`sys.cpu.nice`")
        tdSql.checkRows(3)

        tdSql.query(f"select * from {dbname}.`sys.cpu.nice` order by _ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 13.000000000)
        tdSql.checkData(0, 2, "web01")
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 1)

        tdSql.checkData(1, 1, 9.000000000)
        tdSql.checkData(1, 2, "web02")
        tdSql.checkData(3, 3, "t1")
        tdSql.checkData(2, 4, 4)

        tdSql.query(f"select * from {dbname}.macylr")
        tdSql.checkRows(2)

        tdSql.query(f"select * from {dbname}.qelhxo")
        tdSql.checkRows(5)

        tdSql.query(f"desc {dbname}.macylr")
        tdSql.checkRows(25)

        tdSql.query(f"select * from ts3724.`.stb2`")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.`stb.2`")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.`stb2.`")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts5528.device_log_yuelan_cs1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '{"deviceId":"星宇公司-861701069493741","headers":{"_uid":"4e3599eacd62834995c77b38ad95f88d","creatorId":"1199596756811550720","deviceNmae":"861701069493741","productId":"yuelan","productName":"悦蓝cat1穿戴设备"},"messageType":"REPORT_PROPERTY","properties":{"lat":35.265527067449185,"lng":118.49713144245987,"location":"118.49713144245987,35.265527067449185"},"timestamp":1728719963230}')
        tdSql.checkData(1, 1, '{"deviceId":"星宇公司-861701069065507","headers":{"_uid":"9045d6b78b4ffaf1e2d244e912ebbff8","creatorId":"1199596756811550720","deviceNmae":"861701069065507","productId":"yuelan","productName":"悦蓝cat1穿戴设备"},"messageType":"REPORT_PROPERTY","properties":{"lat":36.788241914043425,"lng":119.15042325460891,"location":"119.15042325460891,36.788241914043425"},"timestamp":1728719964105}')
        # tdSql.query(f"select * from td24559.stb order by _ts")
        # tdSql.checkRows(4)
        # tdSql.checkData(0, 2, "POINT (4.343000 89.342000)")
        # tdSql.checkData(3, 2, "GEOMETRYCOLLECTION (MULTIPOINT ((0.000000 0.000000), (1.000000 1.000000)), POINT (3.000000 4.000000), LINESTRING (2.000000 3.000000, 3.000000 4.000000))")
        return

    def test_sml(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        tdSql.prepare()
        self.checkContent()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
