# -----!/usr/bin/python3.7
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

import sys
from crash_gen.crash_gen_main import MainExec

if __name__ == "__main__":
    
    mExec = MainExec()
    mExec.init()
    exitCode = mExec.run()

    print("\nCrash_Gen is now exiting with status code: {}".format(exitCode))
    sys.exit(exitCode)
