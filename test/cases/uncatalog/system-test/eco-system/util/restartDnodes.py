# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
import os
import subprocess
import random
import platform

class dnode():
    def __init__(self, pid, path):
        self.pid  = pid
        self.path = path

# run exePath no wait finished
def runNoWait(exePath):
    if platform.system().lower() == 'windows':
        cmd = f"mintty -h never {exePath}"
    else:
        cmd = f"nohup {exePath} > /dev/null 2>&1 & "

    if os.system(cmd) != 0:
        return False
    else:
        return True

# get online dnodes
def getDnodes():
    cmd = "ps aux | grep taosd | awk '{{print $2,$11,$12,$13}}'"
    result = os.system(cmd)
    result=subprocess.check_output(cmd,shell=True)
    strout = result.decode('utf-8').split("\n")
    dnodes = []

    for line in strout:
        cols = line.split(' ')
        if len(cols) != 4:
            continue
        exepath = cols[1]
        if len(exepath) < 5 :
            continue
        if exepath[-5:] != 'taosd':
            continue

        # add to list
        path = cols[1] + " " + cols[2] + " " + cols[3]
        dnodes.append(dnode(cols[0], path))

    print(" show dnodes cnt=%d...\n"%(len(dnodes)))
    for dn in dnodes:
        print(f"  pid={dn.pid} path={dn.path}")

    return dnodes

def restartDnodes(dnodes, cnt, seconds):
    print(f"start dnode cnt={cnt} wait={seconds}s")
    selects = random.sample(dnodes, cnt)
    for select in selects:
        print(f" kill -9 {select.pid}")
        cmd = f"kill -9 {select.pid}"
        os.system(cmd)
        print(f" restart {select.path}")
        if runNoWait(select.path) == False:
            print(f"run {select.path} failed.")
            raise Exception("exe failed.")
        print(f" sleep {seconds}s ...")
        time.sleep(seconds)

def run():
    # kill seconds interval
    killLoop = 10
    minKill = 1
    maxKill = 10
    for i in range(killLoop):
        dnodes = getDnodes()
        killCnt = 0
        if len(dnodes) > 0:
            killCnt = random.randint(1, len(dnodes))
            restartDnodes(dnodes, killCnt, random.randint(1, 5))

        seconds = random.randint(minKill, maxKill)
        print(f"----------- kill loop i={i} killCnt={killCnt} done. do sleep {seconds}s ... \n")
        time.sleep(seconds)


if __name__ == '__main__':
    run()