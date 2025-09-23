"""perform the build release package and install and then test the restful service"""

import unittest
import os


class ForecastTest(unittest.TestCase):

    def test_release(self):
        """ test the package """
        pass

        # print("build install package")
        # os.system("../../script/release.sh")
        # print("build completed")
        #
        # self.assertEqual(os.path.exists("../../release/TDengine-enterprise-anode-1.0.0.tar.gz"), 1)

    def test_install(self):
        """ test """
        pass

        # print("start to install package")
        # os.system("tar zxvf ../../release/TDengine-enterprise-anode-1.0.0.tar.gz")
        # os.chdir("../../release/TDengine-enterprise-anode-1.0.0/")
        #
        # os.system("./install.sh")
