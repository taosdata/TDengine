import pytest
import subprocess
import os

current_path = os.path.abspath(os.path.dirname(__file__))
with open("%s/test_server_linux.txt" % current_path) as f:
    cases = f.read().splitlines()


@pytest.fixture(scope="module")
def setup_module():
    def run_command(command):
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        print("CMD:", command)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("Return Code:", result.returncode)
        assert result.returncode == 0
        return result

    # setup before module tests
    cmd = "bash getAndRunInstaller.sh -m enterprise -f server -l false -c x64 -v 3.3.2.6 -o smoking -s nas -t tar"
    run_command(cmd)
    cmd = "cp /usr/bin/taos*  ../../debug/build/bin/"
    run_command(cmd)

    yield

    # teardown after module tests
    cmd = "python3 versionCheckAndUninstall.py -v 3.3.2.6 -m enterprise -u"
    run_command(cmd)


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
