import os
import time
import subprocess
import random

count = 0
while count < 100:
    print(f"count: {count}")
    i =  random.randint(1,3)
    print(i)
    print(f"ps -ef | grep dnode{i} |grep taosd | grep -v grep | awk '{{print $2}}' | xargs kill -15")
    os.system(f"ps -ef | grep dnode{i} |grep taosd | grep -v grep | awk '{{print $2}}' | xargs kill -15")
    time.sleep(10)
    print(f"nohup taosd -c /home/chr/TDinternal/sim/dnode{i}/cfg &")
    os.system(f"nohup taosd -c /home/chr/TDinternal/sim/dnode{i}/cfg &")
    time.sleep(10)

    try:
        print("check old vnode path:tq/offset*")
        output = subprocess.check_output("du  /home/chr/TDinternal/sim/dnode*/data/vnode/vnode*/tq/offset-ver0 -csh", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    try:
        print("check old vnode path:tq/subscribe/offset*")
        output = subprocess.check_output("du  /home/chr/TDinternal/sim/dnode*/data/vnode/vnode*/tq/subscribe/offset-ver0 -csh ", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    try:
        print("check new vnode path:tq/subscribe/main.tdb")
        output = subprocess.check_output("du  /home/chr/TDinternal/sim/dnode*/data/vnode/vnode*/tq/subscribe/main.tdb -csh ", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    count += 1
