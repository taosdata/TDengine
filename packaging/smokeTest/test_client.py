import pytest
import subprocess
import os
import sys
import platform
import getopt
import re
import time
import taos
from versionCheckAndUninstallforPytest import UninstallTaos

# python3 smokeTestClient.py -h 192.168.0.22 -P 6031 -v ${version} -u

OEM = ["ProDB"]


@pytest.fixture(scope="module")
def get_config(request):
    verMode = request.config.getoption("--verMode")
    taosVersion = request.config.getoption("--tVersion")
    baseVersion = request.config.getoption("--baseVersion")
    sourcePath = request.config.getoption("--sourcePath")
    config = {
        "verMode": verMode,
        "taosVersion": taosVersion,
        "baseVersion": baseVersion,
        "sourcePath": sourcePath,
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
        name = "taos"

        if config["baseVersion"] in OEM:
            name = config["baseVersion"].lower()
        if config["baseVersion"] in OEM and config["system"] == 'Windows':
            cmd = f'{name} -s "create database {config["databaseName"]};"  -h {config["serverHost"]}  -P {config["serverPort"]}'
            run_cmd(cmd)
            cmd = f'{name} -s "CREATE STABLE {config["databaseName"]}.meters (`ts` TIMESTAMP,`current` FLOAT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24));"  -h {config["serverHost"]}  -P {config["serverPort"]}'
            run_cmd(cmd)
        else:
            cmd = f'{name}Benchmark -y -a 3 -n 100 -t 100 -d {config["databaseName"]} -h {config["serverHost"]} -P {config["serverPort"]} &'
            run_cmd(cmd)
        # os.system("taosBenchmark -y -a 3 -n 100 -t 100 -d %s -h %s -P %d" % (databaseName, serverHost, serverPort))
        time.sleep(5)
        conn = get_connect(config["serverHost"], config["serverPort"], config["databaseName"])
        sql = "SELECT count(*) from meters"
        result: taos.TaosResult = conn.query(sql)
        data = result.fetch_all()
        print("SQL: %s" % sql)
        print("Result: %s" % data)
        if config["system"] == 'Windows' and config["baseVersion"] in OEM:
            pass
        elif data[0][0] != 10000:
            raise f"{name}Benchmark work not as expected "
        # drop database of test
        cmd = f'{name} -s "drop database {config["databaseName"]};"  -h {config["serverHost"]}  -P {config["serverPort"]}'
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
        name = "taos"
        if config["baseVersion"] in OEM:
            name = config["baseVersion"].lower()
        if config["system"] == "Windows":
            taos_V_output = subprocess.getoutput(f"{name} -V | findstr version")
        else:
            taos_V_output = subprocess.getoutput(f"{name} -V | grep version")
        assert config["taosVersion"] in taos_V_output
        assert config["taosVersion"] in client_version
        if config["taosVersion"] not in server_version:
            print("warning: client version is not same as server version")
        conn.close()

    @pytest.mark.all
    def test_uninstall(self, get_config, setup_module):
        config = get_config
        name = "taos"
        if config["baseVersion"] in OEM:
            name = config["baseVersion"].lower()
            subprocess.getoutput("rm /usr/local/bin/taos")
            subprocess.getoutput("pkill taosd")
        UninstallTaos(config["taosVersion"], config["verMode"], True, name)
