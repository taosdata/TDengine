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
# from pycallgraph2 import PyCallGraph
# from pycallgraph2 import Config
# from pycallgraph2.globbing_filter import GlobbingFilter

# from pycallgraph2.output import GraphvizOutput

if __name__ == "__main__":

    mExec = MainExec()
    mExec.init()
    exitCode = mExec.run()

    print("\nCrash_Gen is now exiting with status code: {}".format(exitCode))
    sys.exit(exitCode)
    # config = Config(max_depth=10)
    # config.trace_filter = GlobbingFilter(exclude=['*pycallgraph*', '*_buildCmdLineParser*', '*ArgumentParser*', '*argparse*', '*signal*', '*textwrap*',
    #                                               '*gettext*', '*pydevd_tracing*', '*re.match*', '*re._compile*', '*re.compile*', '*re.sub*', '*re._compile*',
    #                                               '*traceback*', '*posixpath*', 'types.Dynamic*', '*sre_compile*', '*locale*', '*genericpath*', '*linecache*',
    #                                               '*sre_parse*', '*shutil*', '*collections*', '*enum.Regex*', '*pydev_bundle*', '*weakrefset*'])

    # graphviz = GraphvizOutput()
    # graphviz.output_file = 'basic.png'
    # with PyCallGraph(output=graphviz, config=config):
        # mExec = MainExec()
        # mExec.init()
        # exitCode = mExec.run()
    # mExec = MainExec()
    # mExec.init()
    # exitCode = mExec.run()

    # print("\nCrash_Gen is now exiting with status code: {}".format(exitCode))
    # sys.exit(exitCode)