import subprocess
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.epath import *
from frame import *


class TDTestCase(TBase):
    def run(self):
        c_file_path = os.sep.join([binPath(), "passwdTest localhost"])
        p = subprocess.Popen(c_file_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if 0 != p.returncode:
            tdLog.exit("Failed to run passwd test with output: %s \n error: %s" % (out, err))
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
