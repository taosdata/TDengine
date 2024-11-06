import pytest
import subprocess
import os
from versionCheckAndUninstallforPytest import UninstallTaos
import platform
import re
import time
import signal

system = platform.system()
current_path = os.path.abspath(os.path.dirname(__file__))
if system == 'Windows':
    with open(r"%s\test_server_windows.txt" % current_path) as f:
        cases = f.read().splitlines()
else:
    with open("%s/test_server.txt" % current_path) as f:
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
        result = subprocess.run(command, capture_output=True, text=False, shell=False)
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
    if config["system"] == "Windows":
        cmd = r"mkdir ..\..\debug\build\bin"
    else:
        cmd = "mkdir -p ../../debug/build/bin/"
    subprocess.getoutput(cmd)
    if config["system"] == "Linux":  # add tmq_sim
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
    def test_taosd_up(self, setup_module):
        # start process
        if system == 'Windows':
            subprocess.getoutput("taskkill /IM taosd.exe /F")
            cmd = "..\\..\\debug\\build\\bin\\taosd.exe"
        else:
            subprocess.getoutput("pkill taosd")
            cmd = "../../debug/build/bin/taosd"
        process = subprocess.Popen(
            [cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # monitor output
        while True:
            line = process.stdout.readline()
            if line:
                print(line.strip())
            if "succeed to write dnode" in line:
                time.sleep(15)
                # 发送终止信号
                os.kill(process.pid, signal.SIGTERM)
                break

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
        # elif system == 'Linux':
        #     cmd = "systemctl start taosd".split(' ')
        else:
            #    cmd = "sudo launchctl start com.tdengine.taosd".split(' ')
            cmd = "start-all.sh"
        process_out = subprocess.Popen(cmd,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        print(cmd)
        time.sleep(5)

        import taos
        conn = taos.connect()
        check_list = {}
        check_list["server_version"] = conn.server_info
        check_list["client_version"] = conn.client_info
        # Execute sql get version info
        result: taos.TaosResult = conn.query("SELECT server_version()")
        check_list["select_server"] = result.fetch_all()[0][0]
        result: taos.TaosResult = conn.query("SELECT client_version()")
        check_list["select_client"] = result.fetch_all()[0][0]
        conn.close()

        binary_files = ["taos", "taosd", "taosadapter", "taoskeeper", "taosBenchmark"]
        if verMode.lower() == "enterprise":
            binary_files.append("taosx")
        if config["baseVersion"] in OEM:
            binary_files = [i.replace("taos", config["baseVersion"].lower()) for i in binary_files]
        if system == "Windows":
            for i in binary_files:
                check_list[i] = subprocess.getoutput("%s -V | findstr version" % i)
        else:
            for i in binary_files:
                check_list[i] = subprocess.getoutput("%s -V | grep version | awk -F ' ' '{print $3}'" % i)
        for i in check_list:
            print("%s version is: %s" % (i, check_list[i]))
            assert version in check_list[i]

    @pytest.mark.all
    def test_uninstall(self, get_config, setup_module):
        config = get_config
        name = "taos"
        if config["baseVersion"] in OEM:
            name = config["baseVersion"].lower()
            subprocess.getoutput("rm /usr/local/bin/taos")
            subprocess.getoutput("pkill taosd")
        UninstallTaos(config["taosVersion"], config["verMode"], True, name)
