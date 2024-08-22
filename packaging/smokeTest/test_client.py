import pytest
import subprocess
import os
import sys
import platform
import getopt
import re
import time
import taos


# python3 smokeTestClient.py -h 192.168.0.22 -P 6031 -v ${version} -u

@pytest.fixture(scope="module")
def get_config(request):
    # verMode = request.config.getoption("--verMode")
    taosVersion = request.config.getoption("--tVersion")
    # baseVersion = request.config.getoption("--baseVersion")
    # sourcePath = request.config.getoption("--sourcePath")
    config = {
        # "verMode": verMode,
        "taosVersion": taosVersion,
        # "baseVersion": baseVersion,
        # "sourcePath": sourcePath,
        "system": platform.system(),
        "arch": platform.machine(),
        "serverHost": "192.168.0.22",
        "serverPort": 6031,
        "databaseName": re.sub(r'[^a-zA-Z0-9]', '', subprocess.getoutput("hostname")).lower()
    }
    return config


@pytest.fixture(scope="module")
def setup_module(get_config):
    config = get_config
    # install taospy
    if config["system"] == 'Windows':
        taospy_version = subprocess.getoutput("pip3 show taospy|findstr Version")
    else:
        taospy_version = subprocess.getoutput("pip3 show taospy|grep Version| awk -F ':' '{print $2}' ")

    print("taospy version %s " % taospy_version)
    if taospy_version == "":
        subprocess.getoutput("pip3 install git+https://github.com/taosdata/taos-connector-python.git")
        print("install taos python connector")
    else:
        subprocess.getoutput("pip3 install taospy")


def get_connect(host, port, database=None):
    conn = taos.connect(host=host,
                        user="root",
                        password="taosdata",
                        database=database,
                        port=port,
                        timezone="Asia/Shanghai")  # default your host's timezone
    return conn


def run_cmd(command):
    print("CMD: %s" % command)
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print("Return Code:", result.returncode)
    assert result.returncode == 0
    return result


class TestClient:
    @pytest.mark.all
    def test_basic(self, get_config, setup_module):
        config = get_config
        # prepare data by taosBenchmark
        cmd = "taosBenchmark -y -a 3 -n 100 -t 100 -d %s -h %s -P %s &" % (
            config["databaseName"], config["serverHost"], config["serverPort"])
        run_cmd(cmd)
        # os.system("taosBenchmark -y -a 3 -n 100 -t 100 -d %s -h %s -P %d" % (databaseName, serverHost, serverPort))
        time.sleep(5)
        conn = get_connect(config["serverHost"], config["serverPort"], config["databaseName"])
        sql = "SELECT count(*) from meters"
        result: taos.TaosResult = conn.query(sql)
        data = result.fetch_all()
        print("SQL: %s" % sql)
        print("Result: %s" % data)
        if data[0][0] != 10000:
            raise " taosBenchmark work not as expected "
        # drop database of test
        cmd = 'taos -s "drop database %s;"  -h %s  -P %d' % (
            config["databaseName"], config["serverHost"], config["serverPort"])
        result = run_cmd(cmd)
        assert "Drop OK" in result.stdout
        conn.close()

    @pytest.mark.all
    def test_version(self, get_config, setup_module):
        config = get_config
        conn = get_connect(config["serverHost"], config["serverPort"])
        server_version = conn.server_info
        print("server_version: ", server_version)
        client_version = conn.client_info
        print("client_version: ", client_version)
        if config["system"] == "Windows":
            taos_V_output = subprocess.getoutput("taos -V | findstr version")
        else:
            taos_V_output = subprocess.getoutput("taos -V | grep version")
        assert config["taosVersion"] in taos_V_output
        assert config["taosVersion"] in client_version
        if config["taosVersion"] not in server_version:
            print("warning: client version is not same as server version")
        conn.close()

    @pytest.mark.all
    def test_uninstall(self, get_config, setup_module):
        config = get_config
        print("Start to run rmtaos")
        leftFile = False
        print("Platform: ", config["system"])

        if config["system"] == "Linux":
            # 创建一个subprocess.Popen对象，并使用stdin和stdout进行交互
            process = subprocess.Popen(['rmtaos'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            # 向子进程发送输入
            process.stdin.write("y\n")
            process.stdin.flush()  # 确保输入被发送到子进程
            process.stdin.write("I confirm that I would like to delete all data, log and configuration files\n")
            process.stdin.flush()  # 确保输入被发送到子进程
            # 关闭子进程的stdin，防止它无限期等待更多输入
            process.stdin.close()
            # 等待子进程结束
            process.wait()
            # 检查目录清除情况
            out = subprocess.getoutput("ls /etc/systemd/system/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/bin/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/bin/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/lib/libtaos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/lib64/libtaos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/include/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/taos")
            # print(out)
            if "No such file or directory" not in out:
                print("Uninstall left some files in /usr/local/taos：%s" % out)
                leftFile = True
            if not leftFile:
                print("*******Test Result: uninstall test passed ************")

        elif config["system"] == "Darwin":
            # 创建一个subprocess.Popen对象，并使用stdin和stdout进行交互
            process = subprocess.Popen(['sudo', 'rmtaos'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            # 向子进程发送输入
            process.stdin.write("y\n")
            process.stdin.flush()  # 确保输入被发送到子进程
            process.stdin.write("I confirm that I would like to delete all data, log and configuration files\n")
            process.stdin.flush()  # 确保输入被发送到子进程
            # 关闭子进程的stdin，防止它无限期等待更多输入
            process.stdin.close()
            # 等待子进程结束
            process.wait()
            # 检查目录清除情况
            out = subprocess.getoutput("ls /usr/local/bin/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/lib/libtaos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/include/taos*")
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            # out = subprocess.getoutput("ls /usr/local/Cellar/tdengine/")
            # print(out)
            # if out:
            #    print("Uninstall left some files: /usr/local/Cellar/tdengine/%s" % out)
            #    leftFile = True
            # if not leftFile:
            #    print("*******Test Result: uninstall test passed ************")

        elif config["system"] == "Windows":
            process = subprocess.Popen(['unins000', '/silent'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            process.wait()
            time.sleep(10)
            out = subprocess.getoutput("ls C:\TDengine")
            print(out)
            if len(out.split("\n")) > 3:
                leftFile = True
                print("Uninstall left some files: %s" % out)

        if leftFile:
            raise "Uninstall fail"
