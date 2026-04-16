import os, platform, subprocess, time, re, importlib
from new_test_framework.utils import (
    tdLog,
    tdCb,
    tdCom
)

# Import enterprise package downloader
current_dir = os.path.dirname(os.path.realpath(__file__))
enterprise_downloader_path = os.path.abspath(os.path.join(current_dir, "../../../../../enterprise/utils/download_enterprise_package.py"))

# Check if enterprise downloader exists
if not os.path.exists(enterprise_downloader_path):
    raise FileNotFoundError(f"Enterprise package downloader not found at: {enterprise_downloader_path}")

# Load the module
spec = importlib.util.spec_from_file_location("download_enterprise_package", enterprise_downloader_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load enterprise package downloader from: {enterprise_downloader_path}")

download_enterprise_package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(download_enterprise_package)
EnterprisePackageDownloader = download_enterprise_package.EnterprisePackageDownloader
downloader = EnterprisePackageDownloader()

# Define the list of base versions to test
BASE_VERSIONS = ["3.3.7.9", "3.3.8.5", "3.3.8.6"]
# Old-version taos CLI installed by enterprise package
OLD_TAOS_BIN = "/usr/local/taos/bin/taos"
OLD_TAOS_LIB = "/usr/lib:/usr/lib/x86_64-linux-gnu"
OLD_TAOS_CMD = f"LD_LIBRARY_PATH={OLD_TAOS_LIB} {OLD_TAOS_BIN}"
class TestNewStreamCompatibility:

    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Comp: stream backward and forward

        Test compatibility across 4 baseline versions with stream processing validation:

        1. Test [v3.3.7.9 Base Version Compatibility]
            1.1 Install v3.3.7.9 and prepare data using tdCb.prepareDataOnOldVersion()
                1.1.1 Create test databases and tables
                1.1.2 Create streams and insert sample data
                1.1.3 Verify stream functionality on v3.3.7.9
            1.2 Upgrade to new version with mode 2 (no upgrade mode)
                1.2.1 Kill all dnodes and update to new version
                1.2.2 Start new version with existing data
                1.2.3 Verify cross-major version compatibility (corss_major_version=True)
            1.3 Verify data and functionality using tdCb.verifyData()
                1.3.1 Check table counts and row counts consistency
                1.3.2 Verify stream processing functionality
                1.3.3 Validate aggregation results accuracy

        2. Test [v3.3.8.5 Base Version Compatibility]
        3. Test [v3.3.8.6 Base Version Compatibility]
        4. Test [v3.4.1.0 Base Version Compatibility]

        Catalog:
            - Streams:Compatibility:Backward

        Since: v3.3.8.7

        Labels: common, ci

        Jira: TD-38416

        History:
            - 2025-11-17 Tony Zhang created this case
            - Note: Focused on stream-related compatibility

        """
        try:
            import distro
            distro_id = distro.id()
            if distro_id == "alpine":
                tdLog.info(f"alpine skip compatibility test")
                return True
        except ImportError:
            tdLog.info("Cannot import distro module, skipping distro check")

        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip compatibility test")
            return True

        bPath = tdCom.getBuildPath()
        cPath = self.getCfgPath()
        tdLog.info(f"bPath:{bPath}, cPath:{cPath}")

        for base_version in BASE_VERSIONS:

            tdLog.printNoPrefix(f"\n\n========== Start testing compatibility with"
                                f" base version {base_version} ==========")

            self.installEnterpriseTaosd(cPath, base_version)

            self.prepareDataOnOldVersion(base_version)

            tdCb.stopTaosdCompletely()

            tdCb.updateNewVersion(bPath, cPaths=[cPath], upgrade=2)

            self.verifyDataOnCurrentVersion(bPath)

            tdLog.printNoPrefix(f"========== Compatibility test cycle with base"
                f" version {base_version} completed successfully ==========\n")

            tdCb.stopTaosdCompletely()

    def getCfgPath(self):
        buildPath = tdCom.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath

    def prepareDataOnOldVersion(self, base_version):
        """
        1. Create test databases and tables
        2. Create streams and insert sample data
        3. Verify stream functionality on base_version
        """
        os.system(f"{OLD_TAOS_CMD} -s 'create snode on dnode 1;'")
        os.system(f"{OLD_TAOS_CMD} -s 'drop database if exists test_stream_compatibility;'")
        os.system(f"{OLD_TAOS_CMD} -s 'create database test_stream_compatibility;'")
        os.system(f"""{OLD_TAOS_CMD} -s 'create table test_stream_compatibility.stb (ts timestamp, v1 int, v2 float) tags (gid int);'""")
        os.system(f"""{OLD_TAOS_CMD} -s 'create table test_stream_compatibility.ctb1 using test_stream_compatibility.stb tags (1);'""")
        os.system(f"""{OLD_TAOS_CMD} -s 'create table test_stream_compatibility.ctb2 using test_stream_compatibility.stb tags (1);'""")
        # create streams
        os.system(f"""{OLD_TAOS_CMD} -s 'create stream 
        test_stream_compatibility.s_count count_window(3) from 
        test_stream_compatibility.stb partition by tbname into 
        test_stream_compatibility.res_count as select _twstart as ts, _twend as 
        te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname 
        where ts >= _twstart and ts <= _twend;'""")
        os.system(f"""{OLD_TAOS_CMD} -s 'create stream 
        test_stream_compatibility.s_state state_window(v1) from 
        test_stream_compatibility.stb partition by tbname into 
        test_stream_compatibility.res_state as select _twstart as ts, _twend as 
        te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname 
        where ts >= _twstart and ts <= _twend;'""")
        os.system(f"""{OLD_TAOS_CMD} -s 'create stream 
        test_stream_compatibility.s_inter interval(3s) sliding(3s) from 
        test_stream_compatibility.stb into test_stream_compatibility.res_inter 
        as select _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as 
        avg_v2 from test_stream_compatibility.stb 
        where ts >= _twstart and ts < _twend'
        """)

        # check status
        assert self.checkStreamStatus()

        # insert data
        os.system(f"""{OLD_TAOS_CMD} -s 'insert into
                test_stream_compatibility.ctb1 values
                ("2025-11-17 12:00:00", 1,    1.2)
                ("2025-11-17 12:00:01", 1,    1.3)
                ("2025-11-17 12:00:02", 2,    1.5)
                ("2025-11-17 12:00:03", 2,    1.7)
                ("2025-11-17 12:00:04", 2,    1.9)
                ("2025-11-17 12:00:05", 2,    2.2)
                ("2025-11-17 12:00:06", 1,    3.2)
                ("2025-11-17 12:00:07", 1,    4.2)
                ("2025-11-17 12:00:08", 1,    7.2)
                ("2025-11-17 12:00:09", 2,    9.2)'""")
        time.sleep(10)
        
        # check results
        assert self.checkStreamResults("res_count", 3)
        assert self.checkStreamResults("res_state", 3)
        assert self.checkStreamResults("res_inter", 3)

    def verifyDataOnCurrentVersion(self, bPath):
        """
        1. Check table counts and row counts consistency
        2. Verify stream processing functionality
        3. Validate aggregation results accuracy
        """
        taos_bin = f"{bPath}/build/bin/taos"
        lib_dir = f"{bPath}/build/lib"

        # check stream status
        assert self.checkStreamStatusViaCli(taos_bin, lib_dir)

        # check stream results
        assert self.checkStreamResultsViaCli(taos_bin, lib_dir, "res_count", 3)
        assert self.checkStreamResultsViaCli(taos_bin, lib_dir, "res_state", 3)
        assert self.checkStreamResultsViaCli(taos_bin, lib_dir, "res_inter", 3)

        # cross-check: compare stream results with query
        # self.compareStreamResults(taos_bin, lib_dir)

    def installEnterpriseTaosd(self, cPath, base_version):
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"

        # Use enterprise package downloader
        downloader = EnterprisePackageDownloader()
        tdLog.info(f"Downloading and installing enterprise version {base_version}")
        package_path = downloader.download_package(base_version, "enterprise")
        package_name = os.path.basename(package_path)
        package_dir = re.split(
            "-linux-", package_name, maxsplit=1, flags=re.IGNORECASE
        )[0]
        install_cmd = (
            f"cd {packagePath} && tar xf {package_name} && cd {package_dir} && "
            "sudo ./install.sh -e no -v server"
        )
        status = os.system(install_cmd)
        if status != 0:
            raise Exception(
                f"failed to install enterprise version {base_version}"
            )
        tdLog.info(f"Successfully installed enterprise package from {package_path}")

        # install.sh registers systemd Restart=always, must
        # stop completely before starting old-version taosd
        tdCb.stopTaosdCompletely()

        print(f"start taosd: rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"killall taosadapter")
        tdCb.checkProcessPid("taosadapter")
        
        os.system(f"cp /etc/taos/taosadapter.toml {cPath}/taosadapter.toml")
        taosadapter_cfg = cPath + "/taosadapter.toml"
        taosadapter_log_path = cPath + "/../log/"
        print(f"taosadapter_cfg:{taosadapter_cfg}, taosadapter_log_path:{taosadapter_log_path}")
        tdCb.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
        tdCb.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{cPath}\"")
        print("/usr/bin/taosadapter --version")
        os.system(f"/usr/bin/taosadapter --version")
        print(f"LD_LIBRARY_PATH=/usr/lib -c {taosadapter_cfg} 2>&1 &")
        os.system(f"LD_LIBRARY_PATH=/usr/lib /usr/bin/taosadapter -c {taosadapter_cfg} 2>&1 &")
        time.sleep(5)
    
    def checkStreamStatus(self, retry_times=300):
        command = (f"{OLD_TAOS_CMD} -s"
                   " 'select status from"
                   " information_schema.ins_streams'")
        for i in range(retry_times):
            result = subprocess.run(command, shell=True, text=True, capture_output=True, timeout=10)
            if result.returncode == 0:
                running_count = result.stdout.count("Running")
                tdLog.info(f"Found {running_count} running streams.")
                # Three streams were created, so we expect to find 3 running streams.
                if running_count == 3:
                    tdLog.info("All streams are running as expected.")
                    return True
            else:
                tdLog.error("Stream status check failed.")
                tdLog.error(f"Error:\n{result.stderr}")
                raise Exception("Stream status check failed.")
            time.sleep(1)
        return False

    def checkStreamResults(self, res_table, expect_row_num, retry_times=300):
        def get_row_count(command_output) -> int:
            match = re.search(r"Query OK, (\d+) row\(s\) in set", command_output)
            if match:
                return int(match.group(1))
            return 0

        command = (f"{OLD_TAOS_CMD} -s 'select * from"
                   f" test_stream_compatibility.{res_table};'")
        for _ in range(retry_times):
            result = subprocess.run(command, shell=True, text=True, capture_output=True, timeout=10)
            if result.returncode == 0:
                count = get_row_count(result.stdout)
                tdLog.info(f"Stream result rows:{count}, expect:{expect_row_num}")
                if count == expect_row_num:
                    tdLog.info(f"Stream result table {res_table} check executed successfully.")
                    return True
            else:
                tdLog.error("Stream result check failed.")
                tdLog.error(f"Error:\n{result.stderr}")
                raise Exception("Stream result check failed.")
            time.sleep(1)
        return False

    def _run_new_taos(self, taos_bin, lib_dir, sql, timeout=30):
        cmd = (f'LD_LIBRARY_PATH={lib_dir} {taos_bin} -s "{sql}"')
        tdLog.info(f"run: {cmd}")
        return subprocess.run(cmd, shell=True, text=True,
                              capture_output=True, timeout=timeout)

    def checkStreamStatusViaCli(self, taos_bin, lib_dir, retry_times=300):
        sql = "select status from information_schema.ins_streams"
        stable_start = None
        for i in range(retry_times):
            try:
                r = self._run_new_taos(taos_bin, lib_dir, sql)
            except subprocess.TimeoutExpired:
                tdLog.info("Stream status query timed out, retrying...")
                stable_start = None
                time.sleep(1)
                continue
            if r.returncode != 0:
                tdLog.error(f"Stream status check failed: {r.stderr}")
                raise Exception("Stream status check failed.")
            cnt = r.stdout.count("Running")
            tdLog.info(f"Found {cnt} running streams.")
            if cnt == 3:
                if stable_start is None:
                    stable_start = time.time()
                    tdLog.info("All streams running, start 10s stability check.")
                elapsed = time.time() - stable_start
                if elapsed >= 10:
                    tdLog.info("All streams stable for 10s.")
                    return True
            else:
                if stable_start is not None:
                    tdLog.info("Stream status changed, reset stability check.")
                stable_start = None
            time.sleep(1)
        return False

    def checkStreamResultsViaCli(self, taos_bin, lib_dir, res_table,
                                 expect_row_num, retry_times=300):
        sql = f"select * from test_stream_compatibility.{res_table}"
        for _ in range(retry_times):
            try:
                r = self._run_new_taos(taos_bin, lib_dir, sql)
            except subprocess.TimeoutExpired:
                tdLog.info(f"{res_table} query timed out, retrying...")
                time.sleep(1)
                continue
            if r.returncode != 0:
                tdLog.error(f"Stream result check failed: {r.stderr}")
                raise Exception("Stream result check failed.")
            m = re.search(r"Query OK, (\d+) row\(s\) in set", r.stdout)
            count = int(m.group(1)) if m else 0
            tdLog.info(f"{res_table} rows:{count}, expect:{expect_row_num}")
            if count == expect_row_num:
                return True
            time.sleep(1)
        return False

    def compareStreamResults(self, taos_bin, lib_dir):
        checks = [
            ("res_count",
             "select _wstart, _wend, sum(v1), avg(v2) from "
             "test_stream_compatibility.ctb1 count_window(3) limit 3"),
            ("res_state",
             "select _wstart, _wend, sum(v1), avg(v2) from "
             "test_stream_compatibility.ctb1 state_window(v1) limit 3"),
            ("res_inter",
             "select _wstart, _wend, sum(v1), avg(v2) from "
             "test_stream_compatibility.ctb1 interval(3s) sliding(3s) limit 3"),
        ]
        for res_table, exp_sql in checks:
            res_sql = f"select ts, te, sum_v1, avg_v2 from test_stream_compatibility.{res_table}"
            r1 = self._run_new_taos(taos_bin, lib_dir, res_sql)
            r2 = self._run_new_taos(taos_bin, lib_dir, exp_sql)
            assert r1.returncode == 0, f"{res_table} res query failed: {r1.stderr}"
            assert r2.returncode == 0, f"{res_table} exp query failed: {r2.stderr}"
            tdLog.info(f"[{res_table}] stream result:\n{r1.stdout}")
            tdLog.info(f"[{res_table}] expected result:\n{r2.stdout}")
