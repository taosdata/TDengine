import pytest
import subprocess
from main import run_command
import os

current_path = os.path.abspath(os.path.dirname(__file__))
with open("%s/test_server_linux.txt" % current_path) as f:
    cases = f.read().splitlines()


# use pytest fixture to exec case
@pytest.fixture(params=cases)
def run_command(request):
    commands = request.param
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


# define function，use fixture
def test_execute_cases(run_command):
    # assert the result
    if run_command['returncode'] != 0:
        print(f"Running command: {run_command['command']}")
        print("STDOUT:", run_command['stdout'])
        print("STDERR:", run_command['stderr'])
        print("Return Code:", run_command['returncode'])
    else:
        print(f"Running command: {run_command['command']}")
        if len(run_command['stdout']) > 500:
            print("STDOUT:", run_command['stdout'][:500] + "...")
        else:
            print("STDOUT:", run_command['stdout'])
        print("STDERR:", run_command['stderr'])
        print("Return Code:", run_command['returncode'])

    assert run_command[
               'returncode'] == 0, f"Command '{run_command['command']}' failed with return code {run_command['returncode']}"
