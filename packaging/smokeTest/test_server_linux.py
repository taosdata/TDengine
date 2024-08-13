import pytest
import subprocess
import os
from versionCheckAndUninstallforPytest import UninstallTaos
import platform

current_path = os.path.abspath(os.path.dirname(__file__))
with open("%s/test_server_linux.txt" % current_path) as f:
    cases = f.read().splitlines()

system = platform.system()
arch = platform.machine()


@pytest.fixture(scope="module")
def setup_module(request):
    def run_cmd(command):
        print("CMD:", command)
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("Return Code:", result.returncode)
        # assert result.returncode == 0
        return result

    # setup before module tests
    # bash getAndRunInstaller.sh -m ${verMode} -f server -l false -c x64  -v ${version} -o ${baseVersion} -s ${sourcePath}  -t tar
    verMode = request.config.getoption("--verMode")
    taosVersion = request.config.getoption("--taosVersion")
    baseVersion = request.config.getoption("--baseVersion")
    sourcePath = request.config.getoption("--sourcePath")
    cmd = "bash getAndRunInstaller.sh -m %s -f server -l false -c x64 -v %s -o %s -s %s -t tar" % (
        verMode, taosVersion, baseVersion, sourcePath)
    run_cmd(cmd)
    cmd = "mkdir -p ../../debug/build/bin/"
    run_cmd(cmd)
    if system == "Darwin":
        cmd = "cp /usr/local/bin/taos*  ../../debug/build/bin/"
    else:
        cmd = "cp /usr/bin/taos*  ../../debug/build/bin/"
    run_cmd(cmd)
    return taosVersion, verMode


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


class TestServerLinux:
    @pytest.mark.main
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

    @pytest.mark.uninstall
    def test_uninstall(self, setup_module):
        taosVersion, verMode = setup_module
        # teardown after module tests
        # python3 versionCheckAndUninstall.py -v ${version} -m ${verMode} -u
        # cmd = "python3 versionCheckAndUninstall.py -v %s -m %s -u" % (taosVersion, verMode)
        # run_command(cmd)
        UninstallTaos(taosVersion, verMode, True)
