import os
import subprocess
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.epath import *
from frame import *

class TDTestCase(TBase):
    def apiPath(self):
        apiPath = None
        currentFilePath = os.path.dirname(os.path.realpath(__file__))
        if (os.sep.join(["community", "tests"]) in currentFilePath):
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["community", "tests"]))]
        else:
            testFilePath = currentFilePath[:currentFilePath.find(os.sep.join(["TDengine", "tests"]))]

        for root, dirs, files in os.walk(testFilePath):
            if ("passwdTest.c" in files):
                apiPath = root
                break
        return apiPath

    def run(self):
        apiPath = self.apiPath()
        tdLog.info(f"api path: {apiPath}")
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


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
