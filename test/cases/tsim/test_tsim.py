import pytest
import subprocess
import os
import shutil
from new_test_framework import tdLog


class TestTsim:
    def setup_class(cls):
        if cls.tsim_file is None:
            pytest.skip("No tsim file provided")
        cls.SIM_DIR = cls.work_dir
        cls.PRG_DIR = os.path.join(cls.SIM_DIR, "tsim")
        cls.CFG_DIR = os.path.join(cls.PRG_DIR, "cfg")
        cls.LOG_DIR = os.path.join(cls.PRG_DIR, "log")
        cls.DATA_DIR = os.path.join(cls.PRG_DIR, "data")
        cls.ASAN_DIR = os.path.join(cls.SIM_DIR, "asan")
        cls.CODE_DIR = os.path.dirname(os.path.abspath(__file__))
        cls.TAOS_BIN_PATH = cls.taos_bin_path

        
        if cls.SIM_DIR and os.path.exists(cls.SIM_DIR):
            shutil.rmtree(cls.SIM_DIR)

        if cls.LOG_DIR and os.path.exists(cls.LOG_DIR):
            shutil.rmtree(cls.LOG_DIR)

        if cls.CFG_DIR and os.path.exists(cls.CFG_DIR):
            shutil.rmtree(cls.CFG_DIR)

        if cls.ASAN_DIR and os.path.exists(cls.ASAN_DIR):
            shutil.rmtree(cls.ASAN_DIR)

        os.makedirs(cls.PRG_DIR, exist_ok=True)
        os.makedirs(cls.LOG_DIR, exist_ok=True)
        os.makedirs(cls.CFG_DIR, exist_ok=True)
        os.makedirs(cls.ASAN_DIR, exist_ok=True)

        HOSTNAME="localhost"
        cls.cfg_file = os.path.join(cls.CFG_DIR, "taos.cfg")
        with open(cls.cfg_file, "w") as f:
            f.write(f"firstEp    {HOSTNAME}:7100 \n \
                      secondEp           {HOSTNAME}:7200 \n \
                      serverPort         7100 \n \
                      dataDir            {cls.DATA_DIR} \n \
                      logDir             {cls.LOG_DIR} \n \
                      scriptDir          {cls.CODE_DIR} \n \
                      numOfLogLines      100000000 \n \
                      rpcDebugFlag       143 \n \
                      tmrDebugFlag       131 \n \
                      cDebugFlag         143 \n \
                      udebugFlag         143 \n \
                      debugFlag          143 \n \
                      wal                0 \n \
                      asyncLog           0 \n \
                      locale             en_US.UTF-8 \n \
                      enableCoreFile     1 \n \
                      minReservedMemorySize     1024 \n \
                    ")
        cls.env_vars = os.environ.copy()
        cls.env_vars["SIM_DIR"] = cls.SIM_DIR
        cls.env_vars["PRG_DIR"] = cls.PRG_DIR
        cls.env_vars["CFG_DIR"] = cls.CFG_DIR
        cls.env_vars["LOG_DIR"] = cls.LOG_DIR
        cls.env_vars["DATA_DIR"] = cls.DATA_DIR
        cls.env_vars["ASAN_DIR"] = cls.ASAN_DIR
        cls.env_vars["CODE_DIR"] = cls.CODE_DIR
        cls.env_vars["TAOS_BIN_PATH"] = cls.TAOS_BIN_PATH
        tdLog.debug(f"env_vars: {cls.env_vars}")

    @pytest.mark.tsim
    def test_tsim_file(self):
        tdLog.info(f"Start tsim test {self.tsim_file}")
        
        tsim_file = self.tsim_file
        #tsim_file_path = os.path.join("cases", tsim_file)
        tsim_path = self.tsim_path
        bin_path = self.taos_bin_path
        lib_path = self.lib_path
        asan_path = os.path.join(self.work_dir, "asan", "tsim.asan")
        os.makedirs(os.path.join(self.work_dir, "asan"), exist_ok=True)
        tdLog.debug(f"tsim_file: {tsim_file}, tsim_path: {tsim_path}, bin_path: {bin_path}, lib_path: {lib_path}, asan_path: {asan_path}")

        with open(asan_path, "w") as f:
            result = subprocess.run([f"{tsim_path} -f {tsim_file} -c {self.CFG_DIR}"], check=True, text=True, shell=True, stdout=f, stderr=f, cwd=os.path.dirname(__file__), env=self.env_vars)
        if result.returncode != 0:
            tdLog.error(f"Tsim test failed, return code: {result.returncode}")
            assert False
        else:
            tdLog.info(f"Tsim test passed")
            assert True
        tdLog.info("%s successfully executed" % __file__)
