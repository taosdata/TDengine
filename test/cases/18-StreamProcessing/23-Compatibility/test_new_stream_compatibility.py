import os, platform, subprocess, time, re, importlib
from pathlib import Path
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamItem,
    tdCb,
    tdCom
)

# Import enterprise package downloader
current_dir = os.path.dirname(os.path.realpath(__file__))
enterprise_downloader_path = os.path.abspath(os.path.join(current_dir, "../../../../../taos-internal/utils/download_enterprise_package.py"))

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

# Default taos command prefix for the currently-installed (new) version
_SYS_TAOS_PREFIX = "LD_LIBRARY_PATH=/usr/lib /usr/bin/taos"

class TestNewStreamCompatibility:

    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")
        cls.old_bin_dir = ""
        cls.old_lib_dir = ""

    @property
    def _old_taos_prefix(self):
        """Taos command prefix pointing to the extracted old-version binaries."""
        if not self.old_bin_dir or not self.old_lib_dir:
            raise RuntimeError("old_bin_dir / old_lib_dir not set; call installTaosd first")
        return f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taos"

    def test_stream_compatibility(self):
        """Comp: stream backward and forward

        Test compatibility across 3 baseline versions with stream processing validation:

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

            tdLog.printNoPrefix(f"========== Start testing compatibility with base version {base_version} ==========")

            self.installTaosd(cPath, base_version)

            time.sleep(5)

            self.prepareDataOnOldVersion(base_version)

            tdCb.killAllDnodes()
            
            tdCb.updateNewVersion(bPath, cPaths=[cPath], upgrade=2)

            self.startStream()

            self.verifyDataOnCurrentVersion()

            tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")

    def getCfgPath(self):
        buildPath = tdCom.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
    
    def prepareDataOnOldVersion(self, base_version):
        """
        1. Create test databases and tables
        2. Create streams and insert sample data
        3. Verify stream functionality on base_version
        """
        tp = self._old_taos_prefix  # shorthand for old-version taos invocation
        tdLog.info(f"Preparing data on old version {base_version} using taos prefix: {tp}")
        
        os.system(f"{tp} -s 'create snode on dnode 1;'")
        os.system(f"{tp} -s 'drop database if exists test_stream_compatibility;'")
        os.system(f"{tp} -s 'create database test_stream_compatibility;'")
        os.system(f"""{tp} -s 'create table test_stream_compatibility.stb (ts timestamp, v1 int, v2 float) tags (gid int);'""")
        os.system(f"""{tp} -s 'create table test_stream_compatibility.ctb1 using test_stream_compatibility.stb tags (1);'""")
        os.system(f"""{tp} -s 'create table test_stream_compatibility.ctb2 using test_stream_compatibility.stb tags (1);'""")
        # create streams
        os.system(f"""{tp} -s 'create stream 
        test_stream_compatibility.s_count count_window(3) from 
        test_stream_compatibility.stb partition by tbname into 
        test_stream_compatibility.res_count as select _twstart as ts, _twend as 
        te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname 
        where ts >= _twstart and ts <= _twend;'""")
        os.system(f"""{tp} -s 'create stream 
        test_stream_compatibility.s_state state_window(v1) from 
        test_stream_compatibility.stb partition by tbname into 
        test_stream_compatibility.res_state as select _twstart as ts, _twend as 
        te, sum(v1) as sum_v1, avg(v2) as avg_v2 from %%tbname 
        where ts >= _twstart and ts <= _twend;'""")
        os.system(f"""{tp} -s 'create stream 
        test_stream_compatibility.s_inter interval(3s) sliding(3s) from 
        test_stream_compatibility.stb into test_stream_compatibility.res_inter 
        as select _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as 
        avg_v2 from test_stream_compatibility.stb 
        where ts >= _twstart and ts < _twend'
        """)

        # check status
        assert self.checkStreamStatus(taos_prefix=tp)

        # insert data
        os.system(f"""{tp} -s 'insert into
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
        assert self.checkStreamResults("res_count", 3, taos_prefix=tp)
        assert self.checkStreamResults("res_state", 3, taos_prefix=tp)
        assert self.checkStreamResults("res_inter", 3, taos_prefix=tp)

        # stop stream
        self.stopStream(taos_prefix=tp)

    def stopStream(self, taos_prefix=None):
        tp = taos_prefix or _SYS_TAOS_PREFIX
        tdLog.info("stop stream:")
        result = subprocess.run(
            f"{tp} -s 'select stream_name from information_schema.ins_streams;'",
            shell=True, text=True, capture_output=True
        )
        # Each data line looks like: " s_count                 |"
        stream_names = re.findall(r"^\s*(\S+)\s*\|", result.stdout, re.MULTILINE)
        # Drop the header row
        stream_names = [n for n in stream_names if n != "stream_name"]

        for name in stream_names:
            tdLog.info(f"stop stream {name}")
            os.system(f"""{tp} -d test_stream_compatibility -s 'stop stream {name};'""")

        time.sleep(5)

        result = subprocess.run(
            f"{tp} -s 'select stream_name, status from information_schema.ins_streams;'",
            shell=True, text=True, capture_output=True
        )
        # Lines like: " s_count   | Stopped |" — find any that are NOT Stopped
        not_stopped = re.findall(r"^\s*(\S+)\s*\|\s*(?!Stopped\s*\|)(\S+)", result.stdout, re.MULTILINE)
        not_stopped = [(n, s) for n, s in not_stopped if n != "stream_name"]
        if not_stopped:
            raise Exception(f"Stop stream failed, streams not stopped: {not_stopped}")
        tdLog.info("stop all stream success")

    def startStream(self):
        tdLog.info("start stream:")
        tdSql.execute("use test_stream_compatibility")
        tdSql.query("select stream_name from information_schema.ins_streams;")
        stream_names = [row[0] for row in tdSql.queryResult]

        for name in stream_names:
            tdLog.info(f"start stream {name}")
            tdSql.execute(f"start stream {name};")

        assert self.checkStreamStatus()
        tdLog.info("start all stream success")


    def verifyDataOnCurrentVersion(self):
        """
        1. Check table counts and row counts consistency
        2. Verify stream processing functionality
        3. Validate aggregation results accuracy
        """
        streams: list[StreamItem] = []
        stream = StreamItem(
            id=0,
            stream="""create stream test_stream_compatibility.s_count 
                count_window(3) from test_stream_compatibility.stb partition by 
                tbname into test_stream_compatibility.res_count as select 
                _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as 
                avg_v2 from %%tbname where ts >= _twstart and ts <= _twend""",
            res_query="""select ts, te, sum_v1, avg_v2 from 
                test_stream_compatibility.res_count;""",
            exp_query="""select _wstart, _wend, sum(v1) as sum_v1, avg(v2) as 
                avg_v2 from test_stream_compatibility.ctb1 count_window(3) 
                limit 3;""",
        )
        streams.append(stream)

        stream = StreamItem(
            id=1,
            stream="""create stream test_stream_compatibility.s_state 
                state_window(v1) from test_stream_compatibility.stb partition by 
                tbname into test_stream_compatibility.res_state as select 
                _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as avg_v2 from 
                %%tbname where ts >= _twstart and ts <= _twend""",
            res_query="""select ts, te, sum_v1, avg_v2 from 
                test_stream_compatibility.res_state;""",
            exp_query="""select _wstart, _wend, sum(v1) as sum_v1, avg(v2) as 
                avg_v2 from test_stream_compatibility.ctb1 state_window(v1) 
                limit 3;"""
        )
        streams.append(stream)

        stream = StreamItem(
            id=2,
            stream="""create stream test_stream_compatibility.s_inter 
                interval(3s) sliding(3s) from test_stream_compatibility.stb 
                into test_stream_compatibility.res_inter as select 
                _twstart as ts, _twend as te, sum(v1) as sum_v1, avg(v2) as 
                avg_v2 from test_stream_compatibility.stb where ts >= _twstart 
                and ts < _twend""",
            res_query="""select ts, te, sum_v1, avg_v2 from 
                test_stream_compatibility.res_inter;""",
            exp_query="""select _wstart, _wend, sum(v1) as sum_v1, avg(v2) as 
                avg_v2 from test_stream_compatibility.ctb1 interval(3s) 
                sliding(3s) limit 3;"""
        )
        streams.append(stream)

        # check status
        tdStream.checkStreamStatus()

        # check results
        for stream in streams:
            stream.checkResults()

    def installTaosd(self, cPath, base_version):
        """Extract the old-version package (no install) and start its taosd."""
        dataPath = cPath + "/../data/"

        tdLog.info(f"Downloading and extracting enterprise version {base_version} (no install)")
        bin_dir, lib_dir = downloader.download_and_extract(base_version, "enterprise")
        self.old_bin_dir = bin_dir
        self.old_lib_dir = lib_dir
        tdLog.info(f"Extracted: bin={bin_dir}, lib={lib_dir}")

        os.system("pkill -9 taosd")
        tdCb.checkProcessPid("taosd")

        taosd_bin = os.path.join(bin_dir, "taosd")
        tdLog.info(f"start taosd: rm -rf {dataPath}/* && LD_LIBRARY_PATH={lib_dir} nohup {taosd_bin} -c {cPath} &")
        os.system(f"rm -rf {dataPath}/* && LD_LIBRARY_PATH={lib_dir} nohup {taosd_bin} -c {cPath} &")
    
    def checkStreamStatus(self, retry_times=300, taos_prefix=None):
        tp = taos_prefix or _SYS_TAOS_PREFIX
        command = f"{tp} -s 'select status from information_schema.ins_streams'"
        for i in range(retry_times):
            result = subprocess.run(command, shell=True, text=True, capture_output=True)
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

    def checkStreamResults(self, res_table, expect_row_num, retry_times=300, taos_prefix=None):
        tp = taos_prefix or _SYS_TAOS_PREFIX

        def get_row_count(command_output) -> int:
            match = re.search(r"Query OK, (\d+) row\(s\) in set", command_output)
            if match:
                return int(match.group(1))
            return 0

        command = f"{tp} -s 'select * from test_stream_compatibility.{res_table};'"
        for _ in range(retry_times):
            result = subprocess.run(command, shell=True, text=True, capture_output=True)
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
