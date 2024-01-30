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

# -*- coding: utf-8 -*-
from util.common import *

class TDCmd:
    def __init__(self):
        self.buildPath = tdCom.getBuildPath()
        self.taos = os.path.join(self.buildPath, 'taos')

    # @staticmethod
    def run_command(self, path: str = None, command: str = None, check_return_code: bool = True):
        try:
            # Run the Bash script using subprocess
            final_path = path if path else self.buildPath

            tdLog.info(f"run command '{command}' in path '{final_path}'")

            result = subprocess.run(command, check=check_return_code, shell=True, cwd=final_path, stdout=subprocess.PIPE)
            if check_return_code:
                if result.returncode == 0:
                    tdLog.info("Command execution is done, return message：{0}".format(result.stdout.decode('utf-8').strip()))
                    return result.stdout.decode('utf-8').strip()
                else:
                    tdLog.exit("Command run failed, return message：{0}".format(result.stderr.decode('utf-8').strip()))
                    return result.stderr.decode('utf-8').strip()
            else:
                tdLog.info("Command execution is done, return message：{0}".format(result.stdout.decode('utf-8').strip()))
                return result.stdout.decode('utf-8').strip()
        except subprocess.CalledProcessError as e:
            tdLog.exit(f"Error running Bash script: {e}")
            raise subprocess.CalledProcessError(e)
        except FileNotFoundError as e:
            tdLog.exit(f"File not found: {e}")
            raise FileNotFoundError(e)

tdCmd = TDCmd()
