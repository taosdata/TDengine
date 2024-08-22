import pytest
import subprocess
import os
from versionCheckAndUninstallforPytest import UninstallTaos
import platform
import re
import time

current_path = os.path.abspath(os.path.dirname(__file__))
with open("%s/test_server.txt" % current_path) as f:
    cases = f.read().splitlines()


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
        "arch": platform.machine()
    }
    return config


@pytest.fixture(scope="module")
def setup_module(get_config):
    def run_cmd(command):
        print("CMD:", command)
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("Return Code:", result.returncode)
        assert result.returncode == 0
        return result

    # setup before module tests
    config = get_config
    # bash getAndRunInstaller.sh -m ${verMode} -f server -l false -c x64  -v ${version} -o ${baseVersion} -s ${sourcePath}  -t tar
    # t = "tar"
    # if config["system"] == "Darwin":
    #     t = "pkg"
    # cmd = "bash getAndRunInstaller.sh -m %s -f server -l false -c x64 -v %s -o %s -s %s -t %s" % (
    #     config["verMode"], config["taosVersion"], config["baseVersion"], config["sourcePath"], t)
    # run_cmd(cmd)
    cmd = "mkdir -p ../../debug/build/bin/"
    run_cmd(cmd)
    if config["system"] == "Darwin":
        cmd = "sudo cp /usr/local/bin/taos*  ../../debug/build/bin/"
    else:
        cmd = "sudo cp /usr/bin/taos*  ../../debug/build/bin/"
    run_cmd(cmd)

    yield

    UninstallTaos(config["taosVersion"], config["verMode"], True)


# use pytest fixture to exec case
@pytest.fixture(params=cases)
def run_command(request):
    commands = request.param
    if commands.strip().startswith("#"):
        pytest.skip("This case has been marked as skipped")
    d, command = commands.strip().split(",")
    print("cd %s/../../tests/%s&&sudo %s" % (current_path, d, command))
    result = subprocess.run("cd %s/../../tests/%s&&sudo %s" % (current_path, d, command), capture_output=True,
                            text=True, shell=True)
    return {
        "command": command,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }


class TestServer:
    @pytest.mark.all
    def test_execute_cases(self, setup_module, run_command):
        # assert the result
        if run_command['returncode'] != 0:
            print(f"Running command: {run_command['command']}")
            print("STDOUT:", run_command['stdout'])
            print("STDERR:", run_command['stderr'])
            print("Return Code:", run_command['returncode'])
        else:
            print(f"Running command: {run_command['command']}")
            if len(run_command['stdout']) > 1000:
                print("STDOUT:", run_command['stdout'][:1000] + "...")
            else:
                print("STDOUT:", run_command['stdout'])
            print("STDERR:", run_command['stderr'])
            print("Return Code:", run_command['returncode'])

        assert run_command[
                   'returncode'] == 0, f"Command '{run_command['command']}' failed with return code {run_command['returncode']}"

    @pytest.mark.all
    @pytest.mark.check_version
    def test_check_version(self, get_config, setup_module):
        config = get_config
        databaseName = re.sub(r'[^a-zA-Z0-9]', '', subprocess.getoutput("hostname")).lower()
        # install taospy
        taospy_version = ""
        system = config["system"]
        version = config["taosVersion"]
        verMode = config["verMode"]
        if system == 'Windows':
            taospy_version = subprocess.getoutput("pip3 show taospy|findstr Version")
        else:
            taospy_version = subprocess.getoutput("pip3 show taospy|grep Version| awk -F ':' '{print $2}' ")

        print("taospy version %s " % taospy_version)
        if taospy_version == "":
            subprocess.getoutput("pip3 install git+https://github.com/taosdata/taos-connector-python.git")
            print("install taos python connector")
        else:
            subprocess.getoutput("pip3 install taospy")

        # start taosd server
        if system == 'Windows':
            cmd = ["C:\\TDengine\\start-all.bat"]
        elif system == 'Linux':
            cmd = "systemctl start taosd".split(' ')
        else:
            cmd = "sudo launchctl start com.tdengine.taosd".split(' ')
        process_out = subprocess.Popen(cmd,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        print(cmd)
        time.sleep(5)

        import taos
        conn = taos.connect()
        server_version = conn.server_info
        print("server_version", server_version)
        client_version = conn.client_info
        print("client_version", client_version)
        # Execute sql get version info
        result: taos.TaosResult = conn.query("SELECT server_version()")
        select_server = result.fetch_all()[0][0]
        print("SELECT server_version():" + select_server)
        result: taos.TaosResult = conn.query("SELECT client_version()")
        select_client = result.fetch_all()[0][0]
        print("SELECT client_version():" + select_client)
        conn.close()

        taos_V_output = ""
        taosd_V_output = ""
        taosadapter_V_output = ""
        taoskeeper_V_output = ""
        taosx_V_output = ""
        taosB_V_output = ""
        taosxVersion = False
        if system == "Windows":
            taos_V_output = subprocess.getoutput("taos -V | findstr version")
            taosd_V_output = subprocess.getoutput("taosd -V | findstr version")
            taosadapter_V_output = subprocess.getoutput("taosadapter -V | findstr version")
            taoskeeper_V_output = subprocess.getoutput("taoskeeper -V | findstr version")
            taosB_V_output = subprocess.getoutput("taosBenchmark -V | findstr version")
            if verMode == "Enterprise":
                taosx_V_output = subprocess.getoutput("taosx -V | findstr version")
        else:
            taos_V_output = subprocess.getoutput("taos -V | grep version | awk -F ' ' '{print $3}'")
            taosd_V_output = subprocess.getoutput("taosd -V | grep version | awk -F ' ' '{print $3}'")
            taosadapter_V_output = subprocess.getoutput("taosadapter -V | grep version | awk -F ' ' '{print $3}'")
            taoskeeper_V_output = subprocess.getoutput("taoskeeper -V | grep version | awk -F ' ' '{print $3}'")
            taosB_V_output = subprocess.getoutput("taosBenchmark -V | grep version | awk -F ' ' '{print $3}'")
            if verMode == "Enterprise":
                taosx_V_output = subprocess.getoutput("taosx -V | grep version | awk -F ' ' '{print $3}'")

        print("taos -V output is: %s" % taos_V_output)
        print("taosd -V output is: %s" % taosd_V_output)
        print("taosadapter -V output is: %s" % taosadapter_V_output)
        print("taoskeeper -V output is: %s" % taoskeeper_V_output)
        print("taosBenchmark -V output is: %s" % taosB_V_output)
        assert version in client_version
        assert version in server_version
        assert version in select_server
        assert version in select_client
        assert version in taos_V_output
        assert version in taosd_V_output
        assert version in taosadapter_V_output
        assert version in taoskeeper_V_output
        assert version in taosB_V_output
        if verMode == "Enterprise":
            print("taosx -V output is: %s" % taosx_V_output)
            assert version in taosx_V_output
