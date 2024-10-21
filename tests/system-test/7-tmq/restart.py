import os
import time
import subprocess

count = 0
while count < 100:
    print(f"count: {count}")
    os.system("nohup taosd &")
    time.sleep(5)
    os.system("ps -ef | grep taosd | grep -v grep | awk '{print $2}' | xargs kill -15")
    time.sleep(5)


    try:
        print("check old vnode path:tq/offset*")
        output = subprocess.check_output("du  /var/lib/taos/vnode/vnode*/tq/offset-ver0 -csh", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    try:
        print("check old vnode path:tq/subscribe/offset*")
        output = subprocess.check_output("du /var/lib/taos/vnode/vnode*/tq/subscribe/offset-ver0 -csh ", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    try:
        print("check new vnode path:tq/subscribe/main.tdb")
        output = subprocess.check_output("du /var/lib/taos/vnode/vnode*/tq/subscribe/main.tdb -csh ", shell=True, stderr=subprocess.STDOUT)
        print(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print("命令执行出错:", e.output.decode('utf-8'))
    count += 1
