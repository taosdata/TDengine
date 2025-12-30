from new_test_framework.utils import tdLog, tdSql
import os
import platform
import subprocess

class TestPasswd:
    def apiPath(self):
        apiPath = None
        currentFilePath = os.path.dirname(os.path.realpath(__file__))
        tdLog.info(f"current file path: {currentFilePath}")
        if (os.sep.join(["community", "test"]) in currentFilePath):
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["community", "test"]))+ len(os.sep.join(["community", "test"]))]
        else:
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["TDengine", "test"]))+ len(os.sep.join(["TDengine", "test"]))]
        tdLog.info(f"test file path: {testFilePath}")
        for root, dirs, files in os.walk(testFilePath):
            if ("passwdTest.c" in files):
                apiPath = root
                break
        return apiPath

    def test_passwd(self):
        """Password call c unit test

        1. Compile script/api/passwdTest.c to passwdTest
        2. Run passwdTest and check retcode is 0
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/user/test_passwd.py

        """
        apiPath = self.apiPath()
        tdLog.info(f"api path: {apiPath}")
        if platform.system().lower() == 'linux':
            p = subprocess.Popen(f"cd {apiPath} && make", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if 0 != p.returncode:
                tdLog.exit("Test script passwdTest.c make failed")
        else:
            p = subprocess.Popen(f"cd {apiPath} && jom -f makefile_win64.mak", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if 0 != p.returncode:
                tdLog.exit("Test script passwdTest.c make failed")
        
        p = subprocess.Popen(f"ls {apiPath}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        tdLog.info(f"test files: {out}")
        if apiPath:
            test_file_cmd = os.sep.join([apiPath, "passwdTest localhost"])
            try:
                p = subprocess.Popen(test_file_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if 0 != p.returncode:
                    tdLog.exit("Failed to run passwd test with output: %s \n error: %s" % (out, err))
                else:
                    tdLog.info(out)
                tdLog.success(f"{__file__} successfully executed")
            except Exception as e:
                tdLog.exit(f"Failed to execute {__file__} with error: {e}")
        else:
            tdLog.exit("passwdTest.c not found")


