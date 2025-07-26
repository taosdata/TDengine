import argparse
import os
import platform
import shutil
import subprocess
import sys
import toml
import linux_release
import zipfile

from datetime import datetime

taosx_name = "taosx"
taosx_agent_name = "taosx-agent"
taos_explorer_name = "taos-explorer"

script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)
taosx_dir = os.path.abspath(os.path.join(script_dir, ".."))

test_process = ""
# connector name
pi_connector = "pi"
opc_connector = "opc"
influxdb_connector = "influxdb"
opentsdb_connector = "opentsdb"
# hebeipower_plugin = "hebeipower"
all_connectors = [pi_connector, opc_connector, influxdb_connector, opentsdb_connector]

class SubmoduleBuildInfo:
    def __init__(self, name, build_mode):
        self.Name = name
        self.VersionMode = build_mode
sub_module = []

class ReleaseInfo:
    def __init__(self, os):
        self.OS = os
        self.CpuType = ""
        self.DefaultBuildMode = "Release"
        self.TaosXVersion = ""
        self.ReleasePath = ""
        self.InstallPath = ""
        self.Target = "taosx"
        self.PackageName = ""
        self.Branch = ""
        self.Commit = ""
        self.BuildTime = ""
        self.OnlyBuild = False
        self.TdengineVersion = ""
        self.CustomPrompt = "taos"
        self.CustomName = "TDengine"
        self.CustomEmail = "support@taosdata.com"
        self.UploadAgent = False
        self.BuildAgent = False
        self.build_without_docs = False
        self.build_with_selfhost = False
    def print(self):
        for attr in dir(self):
            if not attr.startswith("__"):
                value = getattr(self, attr)
                if not callable(value):
                    print(f"{attr:<20}: {value}")
release_info = ReleaseInfo(platform.system())

def GetCpuType():
    type = ""
    arch, _ = platform.architecture()
    machine = platform.machine()

    if arch == '32bit':
        type = '32'
    elif arch == '64bit':
        if machine.startswith('arm'):
            type = 'arm64'
        elif machine in ('x86_64', 'amd64',  'AMD64'):
            type = 'x64'
        elif machine == 'aarch64':
            type = 'AArch64'
        else:
            type = f'Unknown architecture: {machine}'
            print(f'Get cpu type failed! {machine}')
            sys.exit()
    else:
        type = f'Unknown architecture: {arch}'
        print(f'Get cpu type failed! {arch}')
        sys.exit()
    return type

def get_taosx_version():
    version = ""
    cargo_toml_path = os.path.join(taosx_dir, "Cargo.toml")
    with open(cargo_toml_path, 'r') as f:
        cargo_toml = toml.load(f)
        version = cargo_toml['workspace']['package']['version']
    return version

def get_tdengine_version(ver_number):
    if ver_number is None or len(ver_number) == 0:
        return os.environ.get('VER_NUMBER')
    else:
        return ver_number

def get_taosx_agent_version():
    version = ""
    cargo_toml_path = os.path.join(taosx_dir, "Cargo.toml")
    with open(cargo_toml_path, 'r') as f:
        cargo_toml = toml.load(f)
        version = cargo_toml['workspace']['package']['version']
    return version

def get_install_path():
    target = f"{release_info.CustomPrompt}x"
    if release_info.Target == "agent" or release_info.UploadAgent == True or release_info.BuildAgent == True:
        target = f"{release_info.CustomPrompt}x-agent"

    if release_info.OnlyBuild:
        return os.path.join(taosx_dir, "release", f"{release_info.CustomPrompt}x")
    elif release_info.OS == 'Windows':  # Windows操作系统
        return f'C:\\{release_info.CustomName}'
    else:
        product_name = release_info.CustomName.lower()
        return os.path.join(taosx_dir,"release","{0}-{1}-{2}-linux-{3}".format(product_name, target, release_info.TdengineVersion, release_info.CpuType.lower()))

def get_package_name():
    target = f"{release_info.CustomPrompt}x"
    product_name = release_info.CustomName.lower()
    if release_info.Target == "agent" or release_info.UploadAgent == True or release_info.BuildAgent == True:
        target = f"{release_info.CustomPrompt}x-agent"
    if release_info.OS == 'Windows':  # Windows操作系统
        return f'{product_name}-{target}-{release_info.TdengineVersion}-{release_info.OS.lower()}-{release_info.CpuType.lower()}'
    else:
        return f'{product_name}-{target}-{release_info.TdengineVersion}-{release_info.OS.lower()}-{release_info.CpuType.lower()}'

def get_taosx_output_name():
    if release_info.OS == 'Windows':  # Windows操作系统
        return f"{release_info.CustomPrompt}x.exe"
    else:
        return f"{release_info.CustomPrompt}x"

def get_taosx_agent_output_name():
    if release_info.OS == 'Windows':  # Windows操作系统
        return f"{release_info.CustomPrompt}x-agent.exe"
    else:
        return f"{release_info.CustomPrompt}x-agent"

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
    parser.add_argument('-c', '--cpu_type', help='cpu [aarch32 | aarch64 | x64 | x86 | mips64 | loongarch64 ...] ')
    parser.add_argument('-o', '--objective', choices=['taosx', 'agent'], help='target package type(taosx, agent)')
    parser.add_argument('-t', '--test_process', help='test single process(pi,opc,taosx,package)')
    parser.add_argument('-ob', '--only_build', nargs='?', const=get_install_path(), help='only build taosx into this path.)')
    parser.add_argument('-vn', '--ver_number', help='tdengine enterprise version')
    parser.add_argument('-cp', '--cus_prompt', help='customized prompt')
    parser.add_argument('-cn', '--cus_name', help='customized name')
    parser.add_argument('-ce', '--cus_email', help='customized email')
    parser.add_argument('-ua', '--upload_agent', help='upload taosx-agent to taosdata.com')
    parser.add_argument('-ba', '--build_agent', help='build taosx-agent')
    parser.add_argument('-bw', '--build_without_docs', action='store_true', help='build taosexplorer without docs zip')
    parser.add_argument('-bh', '--build_with_selfhost', action='store_true', help='build taosexplorer with selfhost  docs zip')

    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"Unknown args: {unknown_args}")
        sys.exit()
    if args.build_without_docs:
        release_info.build_without_docs = True
    if args.build_with_selfhost:
        release_info.build_with_selfhost = True

    release_info.InstallPath = get_install_path()
    release_info.ReleasePath = os.path.abspath(os.path.join(script_dir, "..", "release"))
    release_info.CpuType = GetCpuType()
    if args.only_build:
        release_info.OnlyBuild = True
        release_info.InstallPath = args.only_build
    if args.objective:
        release_info.Target = args.objective
    if args.build_mode:
        release_info.DefaultBuildMode = args.build_mode
    if args.cpu_type:
        release_info.CpuType = args.cpu_type
    if args.test_process:
        test_process = args.test_process
    if args.cus_prompt:
        release_info.CustomPrompt = args.cus_prompt
    if args.cus_name:
        release_info.CustomName = args.cus_name
    if args.cus_email:
        release_info.CustomEmail = args.cus_email
    if args.upload_agent:
        release_info.UploadAgent = True
        sub_module.append(SubmoduleBuildInfo(taosx_agent_name, release_info.DefaultBuildMode))
    if args.build_agent:
        release_info.BuildAgent = True
        sub_module.append(SubmoduleBuildInfo(taosx_agent_name, release_info.DefaultBuildMode))
    if release_info.Target == "taosx":
        sub_module.append(SubmoduleBuildInfo(taosx_name, release_info.DefaultBuildMode))
        sub_module.append(SubmoduleBuildInfo(taos_explorer_name, release_info.DefaultBuildMode))
        # release_info.TaosXVersion = get_taosx_version()
        release_info.TdengineVersion = get_tdengine_version(args.ver_number)
    else:
        # release_info.TaosXVersion = get_taosx_agent_version()
        release_info.TdengineVersion = get_tdengine_version(args.ver_number)
    release_info.InstallPath = get_install_path()

    # sub_module.append(SubmoduleBuildInfo(taosx_agent_name, release_info.DefaultBuildMode))
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
    release_info.Branch = branch
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
    linux_release.replace_file_content(file, pattern, new_attribute)

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
        if filename.startswith("taosx-pi-backfill"):
            filepath = os.path.join(backfill_path, filename)
            if os.path.isfile(filepath):
                shutil.copy2(filepath, pi_install_path)

    connector_path = os.path.join(pi_connector_path, "TDPIConnector.Service", "bin", f"{mode}")
    for filename in os.listdir(connector_path):
        filepath = os.path.join(connector_path, filename)
        if os.path.isfile(filepath):
            if not filename.endswith("OSIsoft.AFSDK.dll"):
                shutil.copy2(filepath, pi_install_path)

def build_and_install_opc_on_windows(mode):
    print("buildAndInstallOPC on windows start...")
    opc_connector_path = os.path.join(taosx_dir, "plugins", "opc")
    os.chdir(opc_connector_path)

    # 32位编译
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "386"
    opc_app_name = "taosx-opc.exe"
    os.system(f"go mod vendor")
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

    opc_gdba_install_path = os.path.join(release_info.InstallPath, "append", "opc_gdba_32")
    delete_directory(opc_gdba_install_path)
    opc_gdba_path = os.path.join(script_dir, "append", "opc_gdba_32")
    try:
        print(f"Copy OPC gdba from {opc_gdba_path} to {opc_gdba_install_path}.")
        shutil.copytree(opc_gdba_path, opc_gdba_install_path)
    except FileNotFoundError as e:
        print("Copy OPC gdba Failed: ", e.strerror)
        sys.exit()


def build_and_install_opc(mode):
    if release_info.OS == 'Windows':
        build_and_install_opc_on_windows(mode)
    else:
        print('buildAndInstallOPC not supported on operating system:', release_info.OS)
        sys.exit()

def copy_taosx_service_file(taosx_install_path):
    taosx_path = os.path.join(taosx_dir, "bin")
    # for filename in os.listdir(taosx_path):
    #     if filename.startswith("taosx-srv"):
    #         filepath = os.path.join(taosx_path, filename)
    #         if os.path.isfile(filepath):
    #             shutil.copy2(filepath, taosx_install_path)
    taosx_exe_path = os.path.join(taosx_path, "taosx-srv.exe")
    srv_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-srv.exe")
    shutil.copy2(taosx_exe_path, srv_target)

    taosx_target_path = os.path.join(taosx_dir, "target")
    taosx_xml_path = os.path.join(taosx_target_path, f"{release_info.CustomPrompt}x-srv.xml")
    xml_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-srv.xml")
    shutil.copy2(taosx_xml_path, xml_target)

def copy_taosx_agent_service_file(taosx_install_path):
    taosx_agent_path = os.path.join(taosx_dir, "taosx-agent", "bin")
    # for filename in os.listdir(taosx_agent_path):
    #     if filename.startswith("taosx-agent-srv"):
    #         filepath = os.path.join(taosx_agent_path, filename)
    #         if os.path.isfile(filepath):
    #             shutil.copy2(filepath, taosx_install_path)
    taosx_agent_exe_path = os.path.join(taosx_agent_path, "taosx-agent-srv.exe")
    srv_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-agent-srv.exe")
    shutil.copy2(taosx_agent_exe_path, srv_target)

    taosx_target_path = os.path.join(taosx_dir, "target")
    taosx_agent_xml_path = os.path.join(taosx_target_path, f"{release_info.CustomPrompt}x-agent-srv.xml")
    xml_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-agent-srv.xml")
    shutil.copy2(taosx_agent_xml_path, xml_target)


def copy_taosx_cfg(taos_cfg_path):
    taosx_cfg = os.path.join(taosx_dir, "examples", "taosx.toml")
    try:
        # shutil.copy2(taosx_cfg, taos_cfg_path)
        cfg_target = os.path.join(taos_cfg_path, f"{release_info.CustomPrompt}x.toml")
        shutil.copy2(taosx_cfg, cfg_target)
    except FileNotFoundError as e:
        print("Copy taosx cfg from {} to {} failed: {}".format(taosx_cfg, taos_cfg_path, e.strerror))
        sys.exit()

def copy_taosx_agent_cfg(taos_cfg_path):
    taosx_agent_cfg = os.path.join(taosx_dir, "taosx-agent", "examples", "agent.toml")
    try:
        shutil.copy2(taosx_agent_cfg, taos_cfg_path)
    except FileNotFoundError as e:
        print("Copy taosx Agent cfg from {} to {} failed: {}".format(taosx_agent_cfg, taos_cfg_path, e.strerror))
        sys.exit()

def build_and_install_taosx(mode):
    print("buildAndInstallTaosX start...")
    os.chdir(taosx_dir)
    if mode == "Release":
        os.system(f'cargo make -e VER_NUMBER={release_info.TdengineVersion} -e CUS_PROMPT={release_info.CustomPrompt} -e CUS_NAME="{release_info.CustomName}" -e CUS_EMAIL={release_info.CustomEmail} -e BUILD_PROFILE=release deploy-taosx')
    else:
        os.system(f'cargo make -e VER_NUMBER={release_info.TdengineVersion} -e CUS_PROMPT={release_info.CustomPrompt} -e CUS_NAME="{release_info.CustomName}" -e CUS_EMAIL={release_info.CustomEmail} -e BUILD_PROFILE=dev deploy-taosx')

    taosx_install_path = os.path.join(release_info.InstallPath, "bin")
    check_directory(taosx_install_path)
    taosx_path = os.path.join(taosx_dir, "target", "deploy", get_taosx_output_name())
    try:
        shutil.copy2(taosx_path, taosx_install_path)
        # if release_info.OS == 'Windows':  # Windows操作系统
        #     exe_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x.exe")
        #     shutil.copy2(taosx_path, exe_target)
        # else:
        #     exe_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x")
        #     shutil.copy2(taosx_path, exe_target)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_path,  e.strerror))
        sys.exit()
    copy_taosx_service_file(taosx_install_path)
    taos_cfg_path = os.path.join(release_info.InstallPath, "config")
    check_directory(taos_cfg_path)
    copy_taosx_cfg(taos_cfg_path)

def build_and_install_taosx_agent(mode):
    print("buildAndInstallTaosX Agent start...")
    os.chdir(taosx_dir)
    if mode == "Release":
        os.system(f'set VER_NUMBER={release_info.TdengineVersion}&set CUS_PROMPT={release_info.CustomPrompt}&set CUS_NAME={release_info.CustomName}&set CUS_EMAIL={release_info.CustomEmail}&set BUILD_PROFILE=release&cargo make deploy-taosx-agent')
    else:
        os.system(f'set VER_NUMBER={release_info.TdengineVersion}&set CUS_PROMPT={release_info.CustomPrompt}&set CUS_NAME={release_info.CustomName}&set CUS_EMAIL={release_info.CustomEmail}&set BUILD_PROFILE=dev&cargo make deploy-taosx-agent')

    taosx_install_path = os.path.join(release_info.InstallPath, "bin")
    check_directory(taosx_install_path)
    taosx_agent_path = os.path.join(taosx_dir, "target", "deploy", get_taosx_agent_output_name())
    try:
        shutil.copy2(taosx_agent_path, taosx_install_path)
        # if release_info.OS == 'Windows':  # Windows操作系统
        #     exe_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-agent.exe")
        #     shutil.copy2(taosx_agent_path, exe_target)
        # else:
        #     exe_target = os.path.join(taosx_install_path, f"{release_info.CustomPrompt}x-agent")
        #     shutil.copy2(taosx_agent_path, exe_target)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_agent_path, e.strerror))
        sys.exit()

    copy_taosx_agent_service_file(taosx_install_path)
    taos_cfg_path = os.path.join(release_info.InstallPath, "config")
    check_directory(taos_cfg_path)
    copy_taosx_agent_cfg(taos_cfg_path)

    if release_info.UploadAgent:
        shutil.copy2(taosx_agent_path, release_info.PackageName)
        print(f'upload {release_info.PackageName}.exe to taosdata.com')
        os.system(f"scp {release_info.PackageName}.exe root@taosdata.com:/data/www/assets-download/3.0/")
        print(f'upload {release_info.PackageName}.exe to tdengine.com')
        os.system(f"scp {release_info.PackageName}.exe ubuntu@tdengine.com:/data/www/assets-download/3.0/")
    if release_info.BuildAgent:
        shutil.copy2(taosx_agent_path, release_info.PackageName)

def build_and_install_influxdb(mode):
    print("build_and_install_influxdb on windows start...")
    influxdb_connector_path = os.path.join(taosx_dir,  "plugins", "influxdb")
    os.chdir(influxdb_connector_path)
    build = "mvn clean package"
    print(build)
    os.system(build)
    influxdb_install_path = os.path.join(release_info.InstallPath, "plugins", "influxdb")
    init_directory(influxdb_install_path)
    influxdb_path = os.path.join(influxdb_connector_path, "target", "taosx-influxdb.jar")
    try:
        shutil.copy2(influxdb_path, influxdb_install_path)
    except FileNotFoundError as e:
        print("Build influxdb failed: ", e.strerror)
        sys.exit()

def build_and_install_opentsdb(mode):
    print("build_and_install_opentsdb on windows start...")
    opentsdb_connector_path = os.path.join(taosx_dir,  "plugins", "opentsdb")
    os.chdir(opentsdb_connector_path)
    build = "mvn clean package"
    print(build)
    os.system(build)
    opentsdb_install_path = os.path.join(release_info.InstallPath, "plugins", "opentsdb")
    init_directory(opentsdb_install_path)
    opentsdb_path = os.path.join(opentsdb_connector_path, "target", "taosx-opentsdb.jar")
    try:
        shutil.copy2(opentsdb_path, opentsdb_install_path)
    except FileNotFoundError as e:
        print("Build opentsdb failed: ", e.strerror)
        sys.exit()

def build_and_install_hebeipower(mode):
    print("build_and_install_hebeipower on windows start...")
    hebeipower_build_path = os.path.join(taosx_dir,  "crates", "transform", "parsers", "hebeipower") 
    os.chdir(taosx_dir)
    build = "cargo make taosx-plugin-hebeipower"
    print(build)
    os.system(build)
    hebeipower_install_path = os.path.join(release_info.InstallPath, "plugins", "parsers")
    init_directory(hebeipower_install_path)
    hebeipower_path = os.path.join(hebeipower_build_path, "target", "release", "hebeipower.dll")
    print(f"copy from {hebeipower_path} to {hebeipower_install_path}")
    try:
        shutil.copy2(hebeipower_path, hebeipower_install_path)
    except FileNotFoundError as e:
        print("Build hebeipower failed: ", e.strerror)
        sys.exit()

def build_taos_explorer(explorer_path, mode):
    if release_info.build_without_docs == False:
        update_docs_zip_file(explorer_path)
        copy_docs_to_explorer(explorer_path)
    else:
        print("build taos-explorer without docs zip")
    explorer_exe_path = os.path.join(taosx_dir, "target", "deploy")
    check_directory(explorer_exe_path)
    if release_info.OS.lower() == 'windows':
        os.chdir(taosx_dir)
        os.system(f'cargo make -e VER_NUMBER={release_info.TdengineVersion} -e CUS_PROMPT={release_info.CustomPrompt} -e CUS_NAME="{release_info.CustomName}" -e CUS_EMAIL={release_info.CustomEmail} deploy-explorer')
    else:
        os.chdir(taosx_dir)
        os.system(f'cargo make -e VER_NUMBER={release_info.TdengineVersion} -e CUS_PROMPT={release_info.CustomPrompt} -e CUS_NAME="{release_info.CustomName}" -e CUS_EMAIL={release_info.CustomEmail} deploy-explorer')

def copy_taos_explorer_on_windows(explorer_path):
    explorer_exe_path = os.path.join(taosx_dir, "target", "deploy", f"{release_info.CustomPrompt}-explorer.exe")
    explorer_srv_path = os.path.join(taosx_dir, "target", "deploy", f"{release_info.CustomPrompt}-explorer-srv.exe")
    explorer_srv_xml_path = os.path.join(taosx_dir, "target", "deploy", f"{release_info.CustomPrompt}-explorer-srv.xml")
    explorer_toml_path = os.path.join(taosx_dir, "target", "deploy", "explorer.toml")

    taos_explorer_install_path = os.path.join(release_info.InstallPath, "bin")
    taos_explorer_cfg_path = os.path.join(release_info.InstallPath, "config")
    check_directory(taos_explorer_install_path)
    try:
        shutil.copy2(explorer_exe_path, taos_explorer_install_path)
        shutil.copy2(explorer_srv_path, taos_explorer_install_path)
        shutil.copy2(explorer_srv_xml_path, taos_explorer_install_path)
        shutil.copy2(explorer_toml_path, taos_explorer_cfg_path)
    except FileNotFoundError as e:
        print("Copy taos-explorer to {} failed: {}".format(taos_explorer_install_path,  e.strerror))
        sys.exit()

def build_and_install_taos_explorer(mode):
    print("build_and_install taos_explorer start...")
    explorer_path = os.path.join(taosx_dir, "explorer")
    build_taos_explorer(explorer_path, mode)
    if release_info.OS.lower() == 'windows':
        copy_taos_explorer_on_windows(explorer_path)

def package_on_windows():
    os.chdir(script_dir)
    target = f"{release_info.CustomPrompt}x"
    sub_directory = f"{release_info.CustomPrompt}x"
    app_before_install_txt = "info_before_install.txt"
    if release_info.Target == "agent" or release_info.UploadAgent == True or release_info.BuildAgent == True:
        target = f"{release_info.CustomPrompt}x-agent"
    cmd = f'iscc /F"{release_info.PackageName}" '\
        f'/DMyAppVersion="{release_info.TdengineVersion}" '\
        f'/DMyAppSourceDir="{release_info.InstallPath}" '\
        f'/DCusName="{release_info.CustomName}" '\
        f'/DCusPrompt="{release_info.CustomPrompt}" '\
        f'/DSubDirectory="{sub_directory}" '\
        f'/DMyAppBeforeInstallTxt="{app_before_install_txt}" '\
        f'/DAppName="{target}" '\
        f'{script_dir}/taosx.iss /O{taosx_dir}/release'
    print(cmd)
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f'package {release_info.PackageName} failed')
        sys.exit(1)


def copy_docs_to_explorer(explorer_path):
    print("copy docs to explorer")
    if release_info.CustomPrompt != 'taos' and release_info.CustomName != 'TDengine':
        zh_doc_zip_path = os.path.join(explorer_path, "..", f"docs-{release_info.CustomPrompt}.zip")
        zh_doc_public_path = os.path.join(explorer_path, "public", "docs")
        if os.path.exists(zh_doc_zip_path):
            unzip_docs(zh_doc_zip_path, zh_doc_public_path)
        else:
            print(f"ERROR: not found docs-{release_info.CustomPrompt}.zip")
            sys.exit(1)
    else:
        zh_doc_zip_path = os.path.join(explorer_path, "..", "docs-zh.zip")
        zh_doc_public_path = os.path.join(explorer_path, "public", "docs")
        if os.path.exists(zh_doc_zip_path):
            unzip_docs(zh_doc_zip_path, zh_doc_public_path)
        else:
            print("ERROR: not found docs-zh.zip")
            sys.exit(1)
        en_doc_zip_path = os.path.join(explorer_path, "..", "docs-en.zip")
        en_doc_public_path = os.path.join(explorer_path, "public", "docs-en")
        if os.path.exists(en_doc_zip_path):
            unzip_docs(en_doc_zip_path, en_doc_public_path)
        else:
            print("ERROR: not found docs-en.zip")
            sys.exit(1)

def unzip_docs(doc_zip_path, doc_public_path):
    try:
        if os.path.exists(doc_public_path):
            shutil.rmtree(doc_public_path)
        with zipfile.ZipFile(doc_zip_path) as doc_zip_file:
            doc_zip_file.extractall(doc_public_path)
    except Exception as e:
        print(f"ERROR: unzip docs failed: {e}")
        sys.exit(1)

def update_docs_zip_file(explorer_path):
    try:
        remote_doc_zip_path = "/root/enterprise_build_zip_work/enterprise-docs"
        local_doc_zip_path = os.path.join(explorer_path, "../../../../packaging/")
        en_doc_zip = os.path.join(local_doc_zip_path, "docs-en.zip")
        zh_doc_zip = os.path.join(local_doc_zip_path, "docs-zh.zip")
        print("update docs zip file")
        doc_zip_path = os.path.join(explorer_path, "..")
        if release_info.CustomPrompt == 'taos' or release_info.CustomName == 'TDengine':
            if release_info.build_with_selfhost:
                if os.path.exists(en_doc_zip) and os.path.exists(zh_doc_zip):
                    cmd1 = f"cp {en_doc_zip} {doc_zip_path}"
                    cmd2 = f"cp {zh_doc_zip} {doc_zip_path}"
                else:
                    print(f"ERROR: not found docs-en.zip or docs-zh.zip at {local_doc_zip_path}" )
                    sys.exit(1)
            else:
                cmd1 = f"scp root@192.168.0.30:{remote_doc_zip_path}/docs-en.zip {doc_zip_path}"
                cmd2 = f"scp root@192.168.0.30:{remote_doc_zip_path}/docs-zh.zip {doc_zip_path}"
            subprocess.run(cmd1, shell=True)
            subprocess.run(cmd2, shell=True)
        else: # for oem
            subprocess.run(f"scp root@192.168.0.30:{remote_doc_zip_path}/docs-{release_info.CustomPrompt}.zip {doc_zip_path}", shell=True)
    except Exception as e:
        print(f"ERROR: update docs zip file failed: {e}")
        sys.exit(1)

def package():
    if release_info.OS == 'Windows':
        package_on_windows()
    else:
        print('packaging not supported on operating system:', release_info.OS)
        sys.exit()

def delete_directory(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass

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
    init_directory(release_info.InstallPath)
    check_directory(os.path.join(release_info.InstallPath, "config"))

    opc_install_path = os.path.join(release_info.InstallPath, "plugins", opc_connector)
    init_directory(opc_install_path)

    pi_install_path = os.path.join(release_info.InstallPath, "plugins", pi_connector)
    init_directory(pi_install_path)

    influxdb_install_path = os.path.join(release_info.InstallPath, "plugins", influxdb_connector)
    init_directory(influxdb_install_path)

    opentsdb_install_path = os.path.join(release_info.InstallPath, "plugins", opentsdb_connector)
    init_directory(opentsdb_install_path)


def init_release_directory():
    print("initReleaseDirectory {}...".format(release_info.ReleasePath))
    check_directory(release_info.ReleasePath)


def test_handle_windows(process):
    if process == pi_connector:
        print("Calling PI function...")
        build_and_install_pi("Debug")
    elif process == opc_connector:
        print("Calling OPC function...")
        build_and_install_opc("Debug")
    elif process == "package":
        print("Calling Package function...")
        package()
    elif process == "taosx" and release_info.UploadAgent == False and release_info.BuildAgent == False:
        print("Calling taosx function...")
        build_and_install_taosx("Debug")
    elif process == "agent":
        print("Calling taosx agent function...")
        build_and_install_taosx_agent("Debug")
    elif process == "explorer" and release_info.UploadAgent == False and release_info.BuildAgent == False:
        print("Calling taos-explorer function...")
        build_and_install_taos_explorer("Debug")
    elif process == influxdb_connector:
        print("Calling influxDB function...")
        build_and_install_influxdb("Debug")
    elif process == opentsdb_connector:
        print("Calling openTSDB function...")
        build_and_install_opentsdb("Debug")
    else:
        print(f"Invalid -t param: {process}. Please enter valid input.")

def test_handle(process):
    if release_info.OS.lower() == "windows":
        test_handle_windows(process)
    else:
        if "explorer" == process:
            print("test taos_explorer on linux")
            build_and_install_taos_explorer("Release")
        linux_release.test_handle(release_info, process)

if __name__ == '__main__':
    init_build_info()
    print_param()
    if test_process != "":
        test_handle(test_process)
        sys.exit()

    if release_info.OS.lower() == 'linux':
        for task in sub_module:
            if taos_explorer_name == task.Name and release_info.UploadAgent == False and release_info.BuildAgent == False:
                print("build taos_explorer on linux")
                build_and_install_taos_explorer(task.VersionMode)
        linux_release.release(release_info=release_info, build_info=sub_module)
    elif release_info.OS.lower() == 'windows':
        init_install_directory()
        for task in sub_module:
            if taosx_name == task.Name and release_info.UploadAgent == False and release_info.BuildAgent == False:
                print("build taosx")
                build_and_install_taosx(task.VersionMode)
            elif taosx_agent_name == task.Name:
                print("build taosx-agent")
                build_and_install_taosx_agent(task.VersionMode)
            elif pi_connector == task.Name:
                print("build pi")
                build_and_install_pi(task.VersionMode)
            elif opc_connector == task.Name:
                print("build taosx-opc")
                build_and_install_opc(task.VersionMode)
            elif influxdb_connector == task.Name:
                print("build influxdb_connector")
                build_and_install_influxdb(task.VersionMode)
            elif opentsdb_connector == task.Name:
                print("build opentsdb_connector")
                build_and_install_opentsdb(task.VersionMode)
            elif hebeipower_plugin == task.Name:
                print("build hebeipower plugin not supported on windows for now")
                # build_and_install_hebeipower(task.VersionMode)
            elif taos_explorer_name == task.Name and release_info.UploadAgent == False and release_info.BuildAgent == False:
                print("build taos_explorer")
                build_and_install_taos_explorer(task.VersionMode)
        init_release_directory()
        if release_info.OnlyBuild:
            print("taosx and it's submodule build finished.")
        else:
            package()
    else:
        raise Exception("Unsupported operating system: {}".format(release_info.OS))
