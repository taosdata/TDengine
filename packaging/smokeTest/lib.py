
import subprocess


def run_cmd(command):
    print("CMD:", command)
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print("Return Code:", result.returncode)
    #assert result.returncode == 0
    return result
