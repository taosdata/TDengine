import argparse
import os
import platform
import shutil
import subprocess
import sys
import toml
import re
from datetime import datetime

mqtt_default_version = "1.0.0"
opc_default_version = "1.0.0"
cus_name = "TDengine"
taosx_agent_name = "taosx-agent"
connector_array = []
version = ''
install_path = ''
package_server_name = ''
current_os = platform.system()
script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)
release_path = os.path.join(script_dir, "..", "release")
taosx_dir = os.path.abspath(os.path.join(script_dir, ".."))

test_process = []

# connector name
pi_connector = "pi"
opc_connector = "opc"
mqtt_connector = "mqtt"


class connector_version:
    def __init__(self, name, version, commit, time):
        self.Name = name
        self.Version = version
        self.Commit = commit
        self.BuildTime = time


def get_taosx_version():
    global version
    cargo_toml_path = os.path.join(taosx_dir, "Cargo.toml")
    print("cargo_toml_path:", cargo_toml_path)
    with open(cargo_toml_path, 'r') as f:
        cargo_toml = toml.load(f)
        version = cargo_toml['package']['version']


def get_taosx_agent_output_name():
    if current_os == 'Windows':  # Windows操作系统
        return taosx_agent_name + ".exe"
    else:
        return taosx_agent_name


def set_default_param():
    global install_path

    if current_os == 'Windows':  # Windows操作系统
        install_path = f'C:\\{cus_name}'
    elif current_os == 'Darwin':  # macOS操作系统
        install_path = '/usr/local/{cus_name}'
    elif current_os == 'Linux':  # Linux操作系统
        install_path = '/usr/local/{cus_name}'
    else:
        print('Unknown operating system:', current_os)
        sys.exit()


def is_connector(element):
    if element == pi_connector or element == opc_connector or element == mqtt_connector:
        return True
    return False


def read_args():
    global connector_array, package_server_name, test_process
    get_taosx_version()
    parser = argparse.ArgumentParser()

    # 添加 -c 参数，connector集合
    parser.add_argument('-c', '--connector_array', nargs='+', help='An array of strings')
    parser.add_argument('-t', '--test_process', nargs='+', help='test single process(pi,opc,mqtt,taosx, package)')

    args, unknown_args = parser.parse_known_args()
    if args.connector_array:
        connector_array = args.connector_array
    if args.test_process:
        test_process = args.test_process
        connector_array = args.test_process
    if unknown_args:
        print(f"Unknown args: {unknown_args}")

    result = "".join([f"-{elem}" for elem in connector_array if is_connector(elem)])

    if current_os == 'Windows':
        package_server_name = f'{taosx_agent_name}-v{version}{result}-installer'
    elif current_os == 'Darwin':
        package_server_name = f'{taosx_agent_name}-v{version}{result}-Mac-installer'
    elif current_os == 'Linux':
        package_server_name = f'{taosx_agent_name}-v{version}{result}-Linux-installer'
    else:
        print('Unknown operating system:', current_os)
        sys.exit()


def print_param():
    print('PARAM connector_array', connector_array)
    print('PARAM version:', version)
    print('PARAM install_path:', install_path)
    print('PARAM release_path:', release_path)
    print('PARAM packagServerName:', package_server_name)
    print('')


def change_assemble_file_key(file, key, value):
    # 构造新的属性字符串
    new_attribute = f'[assembly: AssemblyMetadata("{key}", "{value}")]'
    # 使用正则表达式替换旧的属性值
    pattern = f'\[assembly: AssemblyMetadata\("{key}", "[^"]*"\)\]'
    replace_file_content(file, pattern, new_attribute)


def change_assemble_version(file, version):
    new_attribute = f'[assembly: AssemblyVersion("{version}")]'
    pattern = f'\[assembly: AssemblyVersion\("[^"]*"\)\]'
    replace_file_content(file, pattern, new_attribute)


def replace_file_content(file, pattern, new_attribute):
    with open(file, "r") as f:
        content = f.read()
    new_content = re.sub(pattern, new_attribute, content)
    with open(file, "w") as f:
        f.write(new_content)


def change_piconnector_assemble_file(pi_version):
    pi_connector_assembly_file_path = os.path.join(taosx_dir, "plugins", "pi", "src", \
                                                   "TDPIConnector", "TDPIConnector.Service", "Properties",
                                                   "AssemblyInfo.cs")
    change_assemble_file_key(pi_connector_assembly_file_path, "BuildTime", pi_version.BuildTime)
    change_assemble_file_key(pi_connector_assembly_file_path, "Commit", pi_version.Commit)

    pi_backfill_assembly_file_path = os.path.join(taosx_dir, "plugins", "pi", "src", \
                                                  "TDPIConnector", "TDBackfill", "Properties", "AssemblyInfo.cs")
    change_assemble_file_key(pi_backfill_assembly_file_path, "BuildTime", pi_version.BuildTime)
    change_assemble_file_key(pi_backfill_assembly_file_path, "Commit", pi_version.Commit)

    if pi_version.Version != "":
        change_assemble_version(pi_connector_assembly_file_path, pi_version.Version + ".*")
        change_assemble_version(pi_backfill_assembly_file_path, pi_version.Version + ".*")


def check_piconnector_version(version):
    pattern = r'^\d+\.\d+\.\d+$'
    if version != "" and not re.match(pattern, version):
        print("pi connector version input error! Please use fomat as *.*.*")
        sys.exit()


def build_and_install_pi():
    pi_version = get_connector_version(pi_connector)
    check_piconnector_version(pi_version.Version)
    if current_os != 'Windows':
        print(" PI Connector is only compatible with the Windows operating system.")
        sys.exit()
    print("build_and_install_pi start...")
    pi_connector_path = os.path.join(taosx_dir, "plugins", "pi", "src", "TDPIConnector")
    os.chdir(pi_connector_path)
    print("solution clean...")
    os.system('devenv TDPIConnector.sln /clean')
    change_piconnector_assemble_file(pi_version)
    build_cmd = f'devenv TDPIConnector.sln /build Release '
    print(build_cmd)
    build_result = os.system(build_cmd)
    if build_result == 0:
        print("PI Connector Solution built successfully.")
    else:
        print("Error building PI Connector solution.")
        sys.exit()

    pi_install_path = os.path.join(install_path, "xplugins", "pi")
    init_directory(pi_install_path)
    if not os.path.isdir(pi_install_path):
        print(f"Error: {pi_install_path} is not a directory, delete it and retry")
        sys.exit()

    backfill_path = os.path.join(pi_connector_path, "TDBackfill", "bin", "Release")
    for filename in os.listdir(backfill_path):
        if filename.startswith("TDBackfill"):
            filepath = os.path.join(backfill_path, filename)
            if os.path.isfile(filepath):
                shutil.copy2(filepath, pi_install_path)

    connector_path = os.path.join(pi_connector_path, "TDPIConnector.Service", "bin", "Release")
    for filename in os.listdir(connector_path):
        filepath = os.path.join(connector_path, filename)
        if os.path.isfile(filepath):
            shutil.copy2(filepath, pi_install_path)


def build_and_install_opc_on_windows():
    opc_version = get_connector_version(opc_connector)
    print("buildAndInstallOPC on windows start...")
    opc_connector_path = os.path.join(taosx_dir, "taosx",  "plugins", "opc")
    os.chdir(opc_connector_path)

    # 32位编译
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "386"
    opc_app_name = "opc-collector.exe"
    os.system(f"go build -ldflags "
              f"\"-s -w -X 'collector/version.Version={opc_version.Version}' "
              f"-X 'collector/version.BuildAt={opc_version.BuildTime}' "
              f"-X 'collector/version.CommitID={opc_version.Commit}'\" "
              f"-o dist/windows_386/{opc_app_name}")

    opc_install_path = os.path.join(install_path, "xplugins", "opc")
    init_directory(opc_install_path)
    opc_path = os.path.join(opc_connector_path, "dist", "windows_386", opc_app_name)
    try:
        shutil.copy2(opc_path, opc_install_path)
    except FileNotFoundError as e:
        print("Build OPC failed: ", e.strerror)
        sys.exit()


def build_and_install_mqtt_on_windows():
    mqtt_version = get_connector_version(mqtt_connector)
    print("buildAndInstallMQTT on windows start...")
    mqtt_connector_path = os.path.join(taosx_dir, "plugins", "mqtt")
    os.chdir(mqtt_connector_path)
    print(mqtt_connector_path)
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "amd64"
    mqtt_app_name = "taosmqtt.exe"
    base_build = f"go build -o dist/{mqtt_app_name}"
    extend = f" -ldflags \"-X github.com/taosdata/taosx/plugins/mqtt/version.Version={mqtt_version.Version} " \
             f"-X github.com/taosdata/taosx/plugins/mqtt/version.Commit={mqtt_version.Commit} " \
             f"-X \'github.com/taosdata/taosx/plugins/mqtt/version.BuildTime={mqtt_version.BuildTime}\'\""
    build = base_build + extend
    print(build)
    os.system(build)

    mqtt_install_path = os.path.join(install_path, "xplugins", "mqtt")
    init_directory(mqtt_install_path)
    mqtt_path = os.path.join(mqtt_connector_path, "dist", mqtt_app_name)
    try:
        shutil.copy2(mqtt_path, mqtt_install_path)
    except FileNotFoundError as e:
        print("Build MQTT failed: ", e.strerror)
        sys.exit()


def get_connector_version(connector_name):
    version = ""
    for i, arg in enumerate(connector_array):
        if arg == connector_name:
            if i < len(connector_array) - 1 and not is_connector(connector_array[i + 1]):
                version = connector_array[i + 1]
            break
    now = datetime.now()
    print("connector_array:", connector_array)
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('ascii')
    commit = subprocess.check_output(['git', 'rev-list', '-1', branch]).strip().decode('ascii')

    if connector_name == mqtt_connector:
        if version == "":
            version = mqtt_default_version
    if connector_name == opc_connector:
        if version == "":
            version = opc_default_version

    print("Current connector_name:", connector_name)
    print("Current branch:", branch)
    print("Current commit:", commit)
    print("Build version:", version)
    print("Build time:", current_time)
    return connector_version(connector_name, version, commit, current_time)


def build_and_install_opc():
    if current_os == 'Windows':
        build_and_install_opc_on_windows()
    else:
        print('buildAndInstallOPC not supported on operating system:', current_os)
        sys.exit()


def build_and_install_mqtt():
    if current_os == 'Windows':
        build_and_install_mqtt_on_windows()
    else:
        print('buildAndInstallMQTT not supported on operating system:', current_os)
        sys.exit()


def copy_taos_agent_service_file(taosx_install_path):
    taosx_agent_path = os.path.join(taosx_dir, "taosx-agent", "bin")
    for filename in os.listdir(taosx_agent_path):
        if filename.startswith("taosx-agent-srv"):
            filepath = os.path.join(taosx_agent_path, filename)
            if os.path.isfile(filepath):
                shutil.copy2(filepath, taosx_install_path)


def copy_taos_agent_cfg(taos_cfg_path):
    taosx_agent_cfg = os.path.join(taosx_dir, "taosx-agent", "examples", "agent.example.toml")
    try:
        shutil.copy2(taosx_agent_cfg, taos_cfg_path)
    except FileNotFoundError as e:
        print("Copy TaosX Agent cfg from {} to {} failed: {}".format(taosx_agent_cfg, taos_cfg_path, e.strerror))
        sys.exit()


def build_and_install_taosx():
    print("buildAndInstallTaosX Agent start...")
    os.chdir(taosx_dir)
    os.system('cargo build --release --package taosx-agent')

    taox_install_path = os.path.join(install_path, "bin")
    check_directory(taox_install_path)
    taosx_agent_path = os.path.join(taosx_dir, "target", "release", get_taosx_agent_output_name())
    try:
        shutil.copy2(taosx_agent_path, taox_install_path)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_agent_path, e.strerror))
        sys.exit()

    copy_taos_agent_service_file(taox_install_path)
    taos_cfg_path = os.path.join(install_path, "cfg")
    copy_taos_agent_cfg(taos_cfg_path)


def package_on_windows():
    os.chdir(script_dir)
    result = subprocess.run(f'iscc /F"{package_server_name}" /DMyAppVersion="{version}" '
                            f'/DMyAppSourceDir="{install_path}" '
                            f'/DCusName="{cus_name}" '
                            f'/DTaosXAgentName="{taosx_agent_name}" '
                            f'{script_dir}/taosx.iss /O{taosx_dir}/release', shell=True)
    if result.returncode != 0:
        print(f'package {package_server_name} failed')
        sys.exit(1)


def package():
    if current_os == 'Windows':
        package_on_windows()
    else:
        print('packaging not supported on operating system:', current_os)
        sys.exit()


def init_directory(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print('Error:', e)
        sys.exit()
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as e:
        print('Error:', e)
        sys.exit()


def check_directory(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as e:
        print('Error:', e)
        sys.exit()


def init_install_directory():
    print("initInstallDirectory {}...".format(install_path))
    check_directory(install_path)
    check_directory(os.path.join(install_path, "cfg"))

    opc_install_path = os.path.join(install_path, "xplugins", "opc")
    init_directory(opc_install_path)

    pi_install_path = os.path.join(install_path, "xplugins", "pi")
    init_directory(pi_install_path)

    mqtt_install_path = os.path.join(install_path, "xplugins", "mqtt")
    init_directory(mqtt_install_path)


def init_release_directory():
    print("initReleaseDirectory {}...".format(release_path))
    check_directory(release_path)


def test_handle(process_param):
    process = process_param[0]
    if process == pi_connector:
        print("Calling PI function...")
        build_and_install_pi()
    elif process == opc_connector:
        print("Calling OPC function...")
        build_and_install_opc()
    elif process == mqtt_connector:
        print("Calling MQTT function...")
        build_and_install_mqtt()
    elif process == "package":
        print("Calling Package function...")
        package()
    elif process == "taosx":
        print("Calling Taosx function...")
        build_and_install_taosx()
    else:
        print("Invalid input. Please enter valid input.")


if __name__ == '__main__':
    set_default_param()
    read_args()
    print_param()
    if len(test_process) > 0:
        test_handle(test_process)
        sys.exit()
    init_install_directory()

    build_and_install_taosx()
    if pi_connector in connector_array:
        build_and_install_pi()
    if opc_connector in connector_array:
        build_and_install_opc()
    if mqtt_connector in connector_array:
        build_and_install_mqtt()

    init_release_directory()
    package()
