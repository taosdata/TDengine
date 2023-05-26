import argparse
import os
import platform
import shutil
import subprocess
import sys
import toml
import re
from datetime import datetime

cus_name = "TDengine"
taosx_name = "taosx"
taosx_agent_name = "taosx-agent"

script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)
taosx_dir = os.path.abspath(os.path.join(script_dir, ".."))

test_process = ""
# connector name
pi_connector = "pi"
opc_connector = "opc"
mqtt_connector = "mqtt"
influxdb_connector = "influxdb"
all_connectors = [pi_connector, opc_connector, mqtt_connector, influxdb_connector]

class SubmoduleBuildInfo:
    def __init__(self, name, build_mode):
        self.Name = name
        self.VersionMode = build_mode
sub_module = []

class ReleaseInfo:
    def __init__(self, os):
        self.OS = os
        self.DefaultBuildMode = "Release"
        self.TaosXVersion = ""
        self.ReleasePath = ""
        self.InstallPath = ""
        self.PackageName = ""
        self.Branch = ""
        self.Commit = ""
        self.BuildTime = ""
    def print(self):
        for attr in dir(self):
            if not attr.startswith("__"):
                value = getattr(self, attr)
                if not callable(value):
                    print(f"{attr:<20}: {value}")
release_info = ReleaseInfo(platform.system())

def get_taosx_version():
    version = ""
    cargo_toml_path = os.path.join(taosx_dir, "Cargo.toml")
    with open(cargo_toml_path, 'r') as f:
        cargo_toml = toml.load(f)
        version = cargo_toml['package']['version']
    return version

def get_install_path():
    if release_info.OS == 'Windows':  # Windows操作系统
        return f'C:\\Program Files\\taosX'
    else:
        return taosx_agent_name

def get_package_name():
    if release_info.OS == 'Windows':  # Windows操作系统
        return  f'{taosx_name}-{release_info.TaosXVersion}-{release_info.OS}-installer'
    else:
        return f'{taosx_name}-{release_info.TaosXVersion}-{release_info.OS}-installer'

def get_taosx_output_name():
    if release_info.OS == 'Windows':  # Windows操作系统
        return taosx_name + ".exe"
    else:
        return taosx_name

def get_taosx_agent_output_name():
    if release_info.OS == 'Windows':  # Windows操作系统
        return taosx_agent_name + ".exe"
    else:
        return taosx_agent_name

def is_connector(element):
    if element in all_connectors:
        return True
    return False

def get_build_mode(mode):
    if mode == "release" or mode == "Release":
        return "Release"
    elif mode == "debug" or mode == "Debug":
        return "Debug"
    else:
        print("build mode error, please check it.")
        sys.exit()

def reset_build_mode(sub_version_mode):
    if sub_version_mode is None or len(sub_version_mode) == 0:
        return
    global sub_module
    for i, arg in enumerate(sub_version_mode):
        if i < len(sub_version_mode) - 1 and not is_connector(sub_version_mode[i + 1]):
            for j, _ in enumerate(sub_module):
                if sub_module[j].Name == arg:
                    sub_module[j].VersionMode = get_build_mode(sub_version_mode[i + 1])
                    break

def init_build_info():
    global sub_module, release_info, test_process
    parser = argparse.ArgumentParser()

    # 添加 -c 参数，connector集合
    parser.add_argument('-l', '--connector_list', nargs='+', help='A list of connectors to package, all by default')
    parser.add_argument('-b', '--build_mode', help='Debug or Release, Release by default')
    parser.add_argument('-s', '--sub_version_mode', nargs='+', metavar=('pi', 'Debug'), \
        help='Set the compilation mode of a submodule separately')
    parser.add_argument('-t', '--test_process', help='test single process(pi,opc,mqtt,taosx, package)')

    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"Unknown args: {unknown_args}")
        sys.exit()

    release_info.InstallPath = get_install_path()
    release_info.ReleasePath = os.path.abspath(os.path.join(script_dir, "..", "release"))
    release_info.TaosXVersion = get_taosx_version()
    if args.build_mode:
        release_info.DefaultBuildMode = args.build_mode
    if args.test_process:
        test_process = args.test_process
    sub_module.append(SubmoduleBuildInfo(taosx_name, release_info.DefaultBuildMode))
    sub_module.append(SubmoduleBuildInfo(taosx_agent_name, release_info.DefaultBuildMode))
    if args.connector_list:
        for i, arg in enumerate(args.connector_list):
            sub_module.append(SubmoduleBuildInfo(arg, release_info.DefaultBuildMode))
    else:
        for i, arg in enumerate(all_connectors):
            sub_module.append(SubmoduleBuildInfo(arg, release_info.DefaultBuildMode))
    reset_build_mode(args.sub_version_mode)
    release_info.PackageName = get_package_name()

    now = datetime.now()
    release_info.BuildTime = now.strftime("%Y-%m-%d %H:%M:%S")
    branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('ascii')
    release_info.Branch = branch;
    release_info.Commit = subprocess.check_output(['git', 'rev-list', '-1', branch]).strip().decode('ascii')


def print_param():
    print('RELEASE INFO')
    release_info.print()
    print('\nBuild list:')
    for i, module in enumerate(sub_module):
        print(f"    {module.Name:<15}: {module.VersionMode}")

def change_assemble_file_key(file, key, value):
    new_attribute = f'[assembly: AssemblyMetadata("{key}", "{value}")]'
    pattern = f'\[assembly: AssemblyMetadata\("{key}", "[^"]*"\)\]'
    replace_file_content(file, pattern, new_attribute)

def replace_file_content(file, pattern, new_attribute):
    with open(file, "r") as f:
        content = f.read()
    new_content = re.sub(pattern, new_attribute, content)
    with open(file, "w") as f:
        f.write(new_content)

def change_piconnector_assemble_file():
    pi_connector_assembly_file_path = os.path.join(taosx_dir, "plugins", "pi", "src", \
                                                   "TDPIConnector", "TDPIConnector.Service", "Properties",
                                                   "AssemblyInfo.cs")
    change_assemble_file_key(pi_connector_assembly_file_path, "BuildTime", release_info.BuildTime)
    change_assemble_file_key(pi_connector_assembly_file_path, "Commit", release_info.Commit)

    pi_backfill_assembly_file_path = os.path.join(taosx_dir, "plugins", "pi", "src", \
                                                  "TDPIConnector", "TDBackfill", "Properties", "AssemblyInfo.cs")
    change_assemble_file_key(pi_backfill_assembly_file_path, "BuildTime", release_info.BuildTime)
    change_assemble_file_key(pi_backfill_assembly_file_path, "Commit", release_info.Commit)

def build_and_install_pi(mode):
    if release_info.OS != 'Windows':
        print(" PI Connector is only compatible with the Windows operating system.")
        sys.exit()
    print("build_and_install_pi start...")
    pi_connector_path = os.path.join(taosx_dir, "plugins", "pi", "src", "TDPIConnector")
    os.chdir(pi_connector_path)
    print("solution clean...")
    os.system('devenv TDPIConnector.sln /clean')
    change_piconnector_assemble_file()
    build_cmd = f'devenv TDPIConnector.sln /build {mode} '
    print(build_cmd)
    build_result = os.system(build_cmd)
    if build_result == 0:
        print("PI Connector Solution build successfully.")
    else:
        print("Error building PI Connector solution.")
        sys.exit()

    pi_install_path = os.path.join(release_info.InstallPath, "plugins", "pi")
    init_directory(pi_install_path)
    if not os.path.isdir(pi_install_path):
        print(f"Error: {pi_install_path} is not a directory, delete it and retry")
        sys.exit()

    backfill_path = os.path.join(pi_connector_path, "TDBackfill", "bin", f"{mode}")
    for filename in os.listdir(backfill_path):
        if filename.startswith("TDBackfill"):
            filepath = os.path.join(backfill_path, filename)
            if os.path.isfile(filepath):
                shutil.copy2(filepath, pi_install_path)

    connector_path = os.path.join(pi_connector_path, "TDPIConnector.Service", "bin", f"{mode}")
    for filename in os.listdir(connector_path):
        filepath = os.path.join(connector_path, filename)
        if os.path.isfile(filepath):
            shutil.copy2(filepath, pi_install_path)

def build_and_install_opc_on_windows(mode):
    print("buildAndInstallOPC on windows start...")
    opc_connector_path = os.path.join(taosx_dir, "plugins", "opc")
    os.chdir(opc_connector_path)

    # 32位编译
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "386"
    opc_app_name = "taos-opc.exe"
    os.system(f"go build -ldflags "
              f"\"-s -w -X 'collector/version.BuildAt={release_info.BuildTime}' "
              f"-X 'collector/version.CommitID={release_info.Commit}'\" "
              f"-o dist/windows_386/{opc_app_name}")

    opc_install_path = os.path.join(release_info.InstallPath, "plugins", "opc")
    init_directory(opc_install_path)
    opc_path = os.path.join(opc_connector_path, "dist", "windows_386", opc_app_name)
    try:
        shutil.copy2(opc_path, opc_install_path)
    except FileNotFoundError as e:
        print("Build OPC failed: ", e.strerror)
        sys.exit()


def build_and_install_mqtt_on_windows(mode):
    print("buildAndInstallMQTT on windows start...")
    mqtt_connector_path = os.path.join(taosx_dir, "plugins", "mqtt")
    os.chdir(mqtt_connector_path)
    print(mqtt_connector_path)
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "amd64"
    mqtt_app_name = "taos-mqtt.exe"
    base_build = f"go build -o dist/{mqtt_app_name}"
    extend = f" -ldflags \"" \
             f"-X github.com/taosdata/taosx/plugins/mqtt/version.Commit={release_info.Commit} " \
             f"-X \'github.com/taosdata/taosx/plugins/mqtt/version.BuildTime={release_info.BuildTime}\'\""
    build = base_build + extend
    print(build)
    os.system(build)

    mqtt_install_path = os.path.join(release_info.InstallPath, "plugins", "mqtt")
    init_directory(mqtt_install_path)
    mqtt_path = os.path.join(mqtt_connector_path, "dist", mqtt_app_name)
    try:
        shutil.copy2(mqtt_path, mqtt_install_path)
    except FileNotFoundError as e:
        print("Build MQTT failed: ", e.strerror)
        sys.exit()


def get_connector_version(connector_name):
    version = ""
    for i, arg in enumerate(sub_module):
        if arg == connector_name:
            if i < len(sub_module) - 1 and not is_connector(sub_module[i + 1]):
                version = sub_module[i + 1]
            break
    now = datetime.now()
    print("sub_module:", sub_module)
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
    return sub_module_build_info(connector_name, version, commit, current_time)


def build_and_install_opc(mode):
    if release_info.OS == 'Windows':
        build_and_install_opc_on_windows(mode)
    else:
        print('buildAndInstallOPC not supported on operating system:', current_os)
        sys.exit()


def build_and_install_mqtt(mode):
    if release_info.OS == 'Windows':
        build_and_install_mqtt_on_windows(mode)
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
        print("Copy taosx Agent cfg from {} to {} failed: {}".format(taosx_agent_cfg, taos_cfg_path, e.strerror))
        sys.exit()

def build_and_install_taosx(mode):
    print("buildAndInstallTaosX start...")
    os.chdir(taosx_dir)
    if mode == "Release":
        os.system(f'cargo build --release --no-default-features --features optin --features rustls')
    else:
        os.system(f'cargo build --no-default-features --features optin --features rustls')
    taox_install_path = os.path.join(release_info.InstallPath, "bin")
    check_directory(taox_install_path)
    taosx_path = os.path.join(taosx_dir, "target", mode.lower(), get_taosx_output_name())
    try:
        shutil.copy2(taosx_path, taox_install_path)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_path,  e.strerror))
        sys.exit()

def build_and_install_taosx_agent(mode):
    print("buildAndInstallTaosX Agent start...")
    os.chdir(taosx_dir)
    if mode == "Release":
        os.system('cargo build --release --package taosx-agent')
    else:
        os.system('cargo build --package taosx-agent')

    taox_install_path = os.path.join(release_info.InstallPath, "bin")
    check_directory(taox_install_path)
    taosx_agent_path = os.path.join(taosx_dir, "target", mode.lower(), get_taosx_agent_output_name())
    try:
        shutil.copy2(taosx_agent_path, taox_install_path)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_agent_path, e.strerror))
        sys.exit()

    copy_taos_agent_service_file(taox_install_path)
    taos_cfg_path = os.path.join(release_info.InstallPath, "cfg")
    copy_taos_agent_cfg(taos_cfg_path)

def build_and_install_influxdb(mode):
    print("build_and_install_influxdb on windows start...")
    influxdb_connector_path = os.path.join(taosx_dir, "taosx-influxdb")
    os.chdir(influxdb_connector_path)
    build = "mvn clean package"
    print(build)
    os.system(build)
    influxdb_install_path = os.path.join(release_info.InstallPath, "plugins", "influxdb")
    init_directory(influxdb_install_path)
    influxdb_path = os.path.join(influxdb_connector_path, "target", "taosx-influxdb-1.0.0.jar")
    try:
        shutil.copy2(influxdb_path, influxdb_install_path)
    except FileNotFoundError as e:
        print("Build influxdb failed: ", e.strerror)
        sys.exit()

def package_on_windows():
    os.chdir(script_dir) 
    result = subprocess.run(f'iscc /F"{release_info.PackageName}" '
                            f'/DMyAppVersion="{release_info.TaosXVersion}" '
                            f'/DMyAppSourceDir="{release_info.InstallPath}" '
                            f'/DCusName="{cus_name}" '
                            f'/DTaosXAgentName="{taosx_agent_name}" '
                            f'/DTaosXName="{taosx_name}" '
                            f'{script_dir}/taosx.iss /O{taosx_dir}/release', shell=True)
    if result.returncode != 0:
        print(f'package {release_info.PackageNam} failed')
        sys.exit(1)


def package():
    if release_info.OS == 'Windows':
        package_on_windows()
    else:
        print('packaging not supported on operating system:', release_info.OS)
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
    print("initInstallDirectory {}...".format(release_info.InstallPath))
    check_directory(release_info.InstallPath)
    check_directory(os.path.join(release_info.InstallPath, "cfg"))

    opc_install_path = os.path.join(release_info.InstallPath, "plugins", opc_connector)
    init_directory(opc_install_path)

    pi_install_path = os.path.join(release_info.InstallPath, "plugins", pi_connector)
    init_directory(pi_install_path)

    mqtt_install_path = os.path.join(release_info.InstallPath, "plugins", mqtt_connector)
    init_directory(mqtt_install_path)

    influxdb_install_path = os.path.join(release_info.InstallPath, "plugins", influxdb_connector)
    init_directory(influxdb_install_path)


def init_release_directory():
    print("initReleaseDirectory {}...".format(release_info.ReleasePath))
    check_directory(release_info.ReleasePath)


def test_handle(process):
    if process == pi_connector:
        print("Calling PI function...")
        build_and_install_pi("Debug")
    elif process == opc_connector:
        print("Calling OPC function...")
        build_and_install_opc("Debug")
    elif process == mqtt_connector:
        print("Calling MQTT function...")
        build_and_install_mqtt("Debug")
    elif process == "package":
        print("Calling Package function...")
        package()
    elif process == "taosx":
        print("Calling taosx function...")
        build_and_install_taosx("Debug")
    elif process == "agent":
        print("Calling taosx agent function...")
        build_and_install_taosx_agent("Debug")
    else:
        print(f"Invalid -t param: {process}. Please enter valid input.")


if __name__ == '__main__':
    init_build_info()
    print_param()
    if test_process != "":
        test_handle(test_process)
        sys.exit()
    init_install_directory()
    for task in sub_module:
        if taosx_name == task.Name:
            print("build taosx")
            build_and_install_taosx(task.VersionMode)
        if taosx_agent_name == task.Name:
            print("build taosx-agent")
            build_and_install_taosx_agent(task.VersionMode)
        if pi_connector == task.Name:
            print("build pi")
            build_and_install_pi(task.VersionMode)
        if opc_connector == task.Name:
            print("build taosx-opc")
            build_and_install_opc(task.VersionMode)
        if mqtt_connector == task.Name:
            print("build taosx-mqtt")
            build_and_install_mqtt(task.VersionMode)
        if influxdb_connector == task.Name:
            print("build influxdb_connector")
            build_and_install_influxdb(task.VersionMode)
    init_release_directory()
    package()
