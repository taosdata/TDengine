import pytest
import subprocess
import os
from versionCheckAndUninstallforPytest import UninstallTaos
import platform
import re
import time
import signal
import logging



system = platform.system()
current_path = os.path.abspath(os.path.dirname(__file__))

with open("%s/test_server_unix_tdgpt" % current_path) as f:
    cases = f.read().splitlines()

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
    if config["system"] == "Windows":
        cmd = r"mkdir ..\..\debug\build\bin"
    else:
        cmd = "mkdir -p ../../debug/build/bin/"
    subprocess.getoutput(cmd)
    if config["system"] == "Linux" or config["system"] == "Darwin" :  # add tmq_sim
        cmd = "cp -rf ../../../debug/build/bin/tmq_sim ../../debug/build/bin/."
        subprocess.getoutput(cmd)
    if config["system"] == "Darwin":
        cmd = "sudo cp -rf /usr/local/bin/taos*  ../../debug/build/bin/"
    elif config["system"] == "Windows":
        cmd = r"xcopy  C:\TDengine\taos*.exe ..\..\debug\build\bin /Y"
    else:
        if config["baseVersion"] in OEM:
            cmd = '''sudo find /usr/bin -name 'prodb*' -exec sh -c 'for file; do cp "$file" "../../debug/build/bin/taos${file##/usr/bin/%s}"; done' sh {} +''' % (
                config["baseVersion"].lower())
        else:
            cmd = "sudo cp /usr/bin/taos*  ../../debug/build/bin/"
    run_cmd(cmd)
    if config["baseVersion"] in OEM:  # mock OEM
        cmd = "sed -i 's/taos.cfg/%s.cfg/g' ../../tests/pytest/util/dnodes.py" % config["baseVersion"].lower()
        run_cmd(cmd)
        cmd = "sed -i 's/taosdlog.0/%sdlog.0/g' ../../tests/pytest/util/dnodes.py" % config["baseVersion"].lower()
        run_cmd(cmd)
        cmd = "sed -i 's/taos.cfg/%s.cfg/g' ../../tests/army/frame/server/dnode.py" % config["baseVersion"].lower()
        run_cmd(cmd)
        cmd = "sed -i 's/taosdlog.0/%sdlog.0/g' ../../tests/army/frame/server/dnode.py" % config["baseVersion"].lower()
        run_cmd(cmd)
        cmd = "ln -s /usr/bin/prodb /usr/local/bin/taos"
        subprocess.getoutput(cmd)

    # yield
    #
    # name = "taos"
    # if config["baseVersion"] in OEM:
    #     name = config["baseVersion"].lower()
    #     subprocess.getoutput("rm /usr/local/bin/taos")
    #     subprocess.getoutput("pkill taosd")
    # UninstallTaos(config["taosVersion"], config["verMode"], True, name)


# use pytest fixture to exec case
@pytest.fixture(params=cases)
def run_command(request):
    commands = request.param
    if commands.strip().startswith("#"):
        pytest.skip("This case has been marked as skipped")
    d, command = commands.strip().split(",")
    if system == "Windows":
        cmd = r"cd %s\..\..\tests\%s && %s" % (current_path, d, command)
    else:
        cmd = "cd %s/../../tests/%s&&sudo %s" % (current_path, d, command)
    print(cmd)
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
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
