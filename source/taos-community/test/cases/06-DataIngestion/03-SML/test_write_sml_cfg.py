from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, sc, clusterComCheck

import os
import platform
import subprocess
import taos


class TestSml:
    updatecfgDict = {'clientCfg': {'smlChildTableName': 'dataModelName', 'smlAutoChildTableNameDelimiter': '', 'fqdn': 'localhost', 'smlDot2Underline': 0}, 'fqdn': 'localhost'}
    print("===================: ", updatecfgDict)

    #
    # ------------------- test_sml.py ----------------
    #
    def checkContent1(self, dbname="sml_db"):
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
        tdSql.execute(f"drop database {dbname};")
        

    def do_sml(self):
        tdSql.prepare()
        self.checkContent1()

    #
    # ------------------- test_sml.py ----------------
    #
    def check(self):
        conn = taos.connect()
        dbname = "td19291"
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s precision 'us'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=3i64,t2=4f64,dataModelName=ttt c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=3i64,t2=4f.64 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t1=ioiooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t2=q,t1=iooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
            'st,t2=a,t1=ooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4 c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
        ]
        conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)
        print("inserted")

        tdSql.query("select table_name from information_schema.ins_tables where type = 'CHILD_TABLE' order by table_name")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "3i64-4f64-\"t3\"")
        tdSql.checkData(1, 0, "3i64-4f64-ttt")
        tdSql.checkData(2, 0, "3i64-4f_64")
        tdSql.checkData(3, 0, "a-ooo3i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i643i64uuuuuuuuuuuuuuuuuuuuuuuuuu4")
        tdSql.checkData(4, 0, "t_418c134a0f00c7f536886e132d5fbfff")
        tdSql.checkData(5, 0, "t_cb0dbf4ee9c9052815c17fc6483b0139")
        # tdSql.query(f"select * from td24559.stb order by _ts")
        # tdSql.checkRows(4)
        # tdSql.checkData(0, 2, "POINT (4.343000 89.342000)")
        # tdSql.checkData(3, 2, "GEOMETRYCOLLECTION (MULTIPOINT ((0.000000 0.000000), (1.000000 1.000000)), POINT (3.000000 4.000000), LINESTRING (2.000000 3.000000, 3.000000 4.000000))")
        return

    def do_sml_TD19291(self):
        tdSql.prepare()
        self.check()


    #
    # ------------------- test_sml.py ----------------
    #
    def checkContent2(self, dbname="sml_db"):
        simClientCfg = os.path.join(tdDnodes.getSimCfgPath(), "taos.cfg")
        exe_file = "sml_test" if platform.system() != "Windows" else "sml_test.exe"
        cmdStr = os.path.join(tdCom.getBuildPath(), "build", "bin", exe_file) + " " + simClientCfg
        print("cmdStr:", cmdStr)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        print(f"cmd={cmdStr} ret:{ret}")
        if ret != 0:
            tdLog.info("sml_test ret != 0")

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

        tdSql.query(f"select distinct tbname from {dbname}.`sys_if_bytes_out`")
        tdSql.checkRows(2)

        tdSql.query(f"select * from {dbname}.t_f67972b49aa8adf8bca5d0d54f0d850d order by _ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1.300000000)
        tdSql.checkData(1, 1, 13.000000000)

        tdSql.query(f"select * from {dbname}.`sys_procs_running`")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42.000000000)
        tdSql.checkData(0, 2, "web01")

        tdSql.query(f"select distinct tbname from {dbname}.`sys_cpu_nice`")
        tdSql.checkRows(3)

        tdSql.query(f"select * from {dbname}.`sys_cpu_nice` order by _ts")
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

        tdSql.query(f"select * from ts3724._stb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.stb_2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.stb2_")
        tdSql.checkRows(1)
        return

    def do_sml_TS_3724(self):
        tdSql.prepare()
        self.checkContent2()

    #
    # ------------------- odbc ----------------
    #
    def check_get_db_name(self):
        buildPath = tdCom.getBuildPath()
        exe_file = "get_db_name_test" if platform.system() != "Windows" else "get_db_name_test.exe"
        cmdStr = os.path.join(buildPath, "build", "bin", exe_file)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("sml_test get_db_name_test != 0")

    #
    # ------------------- main ----------------
    #
    def test_sml_bugs(self):
        """Sml config

        1. Update taos.cfg with sml related configurations
        2. Restart taosd to make configurations take effect
        3. Use sml_test tool to ingest sml data
        4. Verify the data correctness after ingestion        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-20 Alex Duan Migrated from uncatalog/system-test/2-query/test_sml.py
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_odbc.py

        """
        self.do_sml()
        self.check_get_db_name()
        
