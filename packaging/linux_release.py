import os;
import logging
import shutil
import sys
import re


script_dir = os.path.abspath(os.getcwd())
top_dir = os.path.abspath(os.path.join(script_dir, ".."))
release_dir = os.path.abspath(os.path.join(top_dir, "release"))
opc_dir = os.path.abspath(os.path.join(top_dir, "plugins","opc"))
influxdb_dir = os.path.abspath(os.path.join(top_dir, "plugins","influxdb"))
opentsdb_dir = os.path.abspath(os.path.join(top_dir, "plugins","opentsdb"))
explore_dir = os.path.abspath(os.path.join(top_dir, "explorer"))
systemd_path = ""
target = "taosx"

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def release(release_info,build_info):

    logging.info("release_info: {0}".format(release_info.__dict__))

    init_release_dir(release_info)
    for info in build_info:
        logging.info("build_info: {0}".format(info.__dict__))
        if info.Name =='opc':
            build_and_install_opc_on_linux(release_info,info.VersionMode)
        if info.Name =='influxdb':
            build_and_install_influxdb_on_linux(info.VersionMode)
        if info.Name =='opentsdb':
            build_and_install_opentsdb_on_linux(info.VersionMode)
        if info.Name =='taosx' and release_info.UploadAgent == False and release_info.BuildAgent == False:
            build_and_install_taosx_on_linux(release_info, info.VersionMode)
        if info.Name =='taosx-agent':
            build_and_install_taosx_agent_on_linux(release_info, info.VersionMode)
        if info.Name =='taos-explorer' and release_info.UploadAgent == False and release_info.BuildAgent == False:
            install_taos_explorer_on_linux(release_info, info.VersionMode)

    chmodReleaseDir(release_info)
    if release_info.UploadAgent or release_info.BuildAgent:
        make_agent_package(release_info)
        logging.info("release successfully")
    else:
        if release_info.OnlyBuild:
            logging.info("taosx and it's submodule build finished.")
        else:
            make_tar_package(release_info)
            logging.info("release successfully")


def init_release_dir(release_info):
    logging.info("init_release_dir")
    global release_dir, systemd_path, target
    if release_info.Target == "agent" or release_info.UploadAgent or release_info.BuildAgent:
        target = "taosx-agent"
    release_dir = release_info.InstallPath
    check_directory(release_dir)
    systemd_path = os.path.join(release_dir,"etc", "systemd", "system")
    check_directory(systemd_path)

def build_and_install_opc_on_linux(release_info,mode='release'):
    logging.info("building taosx-opc")
    platform = "linux"
    arch = "amd64"
    verMode = "release"

    dst_dir = os.path.join(release_dir,"plugins","opc")
    binary_file = os.path.join(opc_dir,"taosx-opc")
    check_directory(dst_dir)

    os.chdir(opc_dir)
    if mode.lower() == 'release':
        os.system(f"go build -ldflags "
                  f"\"-s -w -X 'collector/version.BuildAt={release_info.BuildTime}' "
                  f"-X 'collector/version.CommitID={release_info.Commit}'\" "
                  f"-o {binary_file}")
        logging.info("taosx-opc built successfully")
    else:
        os.system(f"go build -ldflags "
                  f"\"-s -w -X 'collector/version.BuildAt={release_info.BuildTime}' "
                  f"-X 'collector/version.CommitID={release_info.Commit}'\" "
                  f"-o {binary_file}")
        logging.info("taosx-opc built successfully")

    shutil.copy2(binary_file, dst_dir)
    logging.info("taosx-opc copied to {release_dir}".format(release_dir=release_dir))

    os.chdir(script_dir)

def build_and_install_influxdb_on_linux(mode='release'):
    logging.info("build_and_install taosx-influxdb on linux")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"plugins","influxdb")
    binary_file = os.path.join(influxdb_dir,"target","taosx-influxdb.jar")
    check_directory(dst_dir)

    os.chdir(influxdb_dir)
    if mode.lower() == 'release':
        build_command = "mvn clean package"
    else:
        # debug mode
        build_command = "mvn clean package"
    logging.info(f"build_command: {build_command}")
    os.system(build_command)
    logging.info("taosx-influxdb built successfully")

    shutil.copyfile(binary_file,os.path.join(release_dir,"plugins","influxdb","taosx-influxdb.jar"))
    logging.info("taosx-influxdb copied to {release_dir}".format(release_dir=dst_dir))

def build_and_install_opentsdb_on_linux(mode='release'):
    logging.info("build_and_install taosx-opentsdb on linux")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"plugins","opentsdb")
    binary_file = os.path.join(opentsdb_dir,"target","taosx-opentsdb.jar")
    check_directory(dst_dir)

    os.chdir(opentsdb_dir)
    if mode.lower() == 'release':
        build_command = "mvn clean package"
    else:
        # debug mode
        build_command = "mvn clean package"
    logging.info(f"build_command: {build_command}")
    os.system(build_command)
    logging.info("taosx-opentsdb built successfully")

    shutil.copyfile(binary_file,os.path.join(release_dir,"plugins","opentsdb","taosx-opentsdb.jar"))
    logging.info("taosx-opentsdb copied to {release_dir}".format(release_dir=dst_dir))


def build_and_install_taosx_on_linux(release_info, mode='release'):
    logging.info("build_and_install taosx under linux...")

    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"bin")
    binary_file = os.path.join(top_dir,"target","deploy",f"{release_info.CustomPrompt}x")
    check_directory(dst_dir)
    os.chdir(top_dir)

    if mode.lower() == 'release':
        os.system(f"VER_NUMBER={release_info.TdengineVersion} CUS_PROMPT={release_info.CustomPrompt} CUS_NAME='{release_info.CustomName}' CUS_EMAIL={release_info.CustomEmail} BUILD_PROFILE=release cargo make deploy-taosx")
    else:
        os.system(f"VER_NUMBER={release_info.TdengineVersion} CUS_PROMPT={release_info.CustomPrompt} CUS_NAME='{release_info.CustomName}' CUS_EMAIL={release_info.CustomEmail} BUILD_PROFILE=dev cargo make deploy-taosx")
    logging.info("taosx built successfully")

    shutil.copy(binary_file,dst_dir)
    logging.info("taosx copied to {release_dir}".format(release_dir=dst_dir))

    shutil.copy(os.path.join(top_dir,"target",f"{release_info.CustomPrompt}x.service"), systemd_path)

    cfg_path = os.path.join(release_dir, "etc", "taos")
    check_directory(cfg_path)
    shutil.copy2(os.path.join(top_dir,"examples","taosx.toml"), cfg_path)

def install_taos_explorer_on_linux(release_info, mode='release'):
    logging.info("install taosx-explore under linux...")
    dst_dir = os.path.join(release_dir,"bin")
    deploy_dir = os.path.join(top_dir, "target", "deploy")
    deploy_file = os.path.join(deploy_dir, f"{release_info.CustomPrompt}-explorer")

    check_directory(dst_dir)
    shutil.copy(deploy_file, dst_dir)
    logging.info("taosx-explorer copied to {release_dir}".format(release_dir=dst_dir))

    os.chdir(explore_dir)
    shutil.copy2(os.path.join(top_dir, "target", mode.lower(), f"{release_info.CustomPrompt}-explorer.service"), systemd_path)

    cfg_path = os.path.join(release_dir,"etc", "taos")
    check_directory(cfg_path)
    shutil.copy2(os.path.join(deploy_dir, "explorer.toml"), cfg_path)

def build_and_install_taosx_agent_on_linux(release_info, mode='release'):
    logging.info("build_and_install taosx-agent under linux...")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"bin")
    binary_file = os.path.join(top_dir,"target","deploy","taosx-agent")
    check_directory(dst_dir)

    os.chdir(top_dir)

    if mode.lower() == 'release':
        os.system(f'VER_NUMBER={release_info.TdengineVersion} CUS_PROMPT={release_info.CustomPrompt} CUS_NAME={release_info.CustomName} CUS_EMAIL={release_info.CustomEmail} BUILD_PROFILE=release cargo make deploy-taosx-agent')
    else:
        os.system(f'VER_NUMBER={release_info.TdengineVersion} CUS_PROMPT={release_info.CustomPrompt} CUS_NAME={release_info.CustomName} CUS_EMAIL={release_info.CustomEmail} BUILD_PROFILE=dev cargo make deploy-taosx-agent')

    logging.info("taosx-agent built successfully")

    shutil.copy(binary_file,dst_dir)
    logging.info("taosx-agent copied to {release_dir}".format(release_dir=dst_dir))

    shutil.copy2(os.path.join(top_dir,"target",f"{release_info.CustomPrompt}x-agent.service"), systemd_path)

    cfg_path = os.path.join(release_dir,"etc", "taos")
    check_directory(cfg_path)
    shutil.copy2(os.path.join(top_dir,"taosx-agent","examples","agent.toml"), cfg_path)

def replace_file_content(file, pattern, new_attribute):
    with open(file, "r") as f:
        content = f.read()
    new_content = re.sub(pattern, new_attribute, content)
    with open(file, "w") as f:
        f.write(new_content)

def chmodReleaseDir(release_info):
    dir = os.path.join(release_dir,"bin")
    if os.path.exists(dir):
        os.chmod(os.path.join(release_dir,"bin"),0o755)

    os.chmod(os.path.join(release_dir,"plugins"),0o755)

def make_agent_package(release_info):
    logging.info("making agent package")
    shutil.copy(os.path.join(script_dir,"uninstall.sh"),release_dir)
    shutil.copy(os.path.join(script_dir,"install.sh"),release_dir)
    replace_file_content(os.path.join(release_dir, "uninstall.sh"), f'target=""', f'target="{target}"')
    replace_file_content(os.path.join(release_dir, "install.sh"), f'target=""', f'target="{target}"')

    os.chmod(os.path.join(release_dir,"uninstall.sh"),0o755)
    os.chmod(os.path.join(release_dir,"install.sh"),0o755)

    os.chdir(os.path.join(release_dir,".."))

    filename = f"{release_dir}.tar.gz"
    code = os.system(f"tar -czvf {filename} $(basename {release_dir})")
    if code != 0:
        raise Exception("packaging {0} failed".format(release_info.TdengineVersion))
    else:
        if release_info.UploadAgent:
            logging.info(f'upload {filename} to taosdata.com')
            os.system(f"scp {release_dir}.tar.gz root@taosdata.com:/data/www/assets-download/3.0/")
            logging.info(f'upload {filename} to tdengine.com')
            os.system(f"scp {release_dir}.tar.gz ubuntu@tdengine.com:/data/www/assets-download/3.0/")

def make_tar_package(release_info):
    logging.info("making tar package")
    shutil.copy(os.path.join(script_dir,"uninstall.sh"),release_dir)
    shutil.copy(os.path.join(script_dir,"install.sh"),release_dir)
    replace_file_content(os.path.join(release_dir, "uninstall.sh"), f'target=""', f'target="{target}"')
    replace_file_content(os.path.join(release_dir, "install.sh"), f'target=""', f'target="{target}"')

    os.chmod(os.path.join(release_dir,"uninstall.sh"),0o755)
    os.chmod(os.path.join(release_dir,"install.sh"),0o755)


    os.chdir(os.path.join(release_dir,".."))
    code = os.system("tar -czvf {0}.tar.gz $(basename {1}) --remove-files".format(release_dir,release_dir))
    if code != 0:
        raise Exception("packaging {0} failed".format(release_info.TdengineVersion))
    else:
        logging.info("packaging {0} successfully".format(release_info.TdengineVersion))
        os.chdir(script_dir)

def test_handle(release_info, process):
    init_release_dir(release_info)
    if process == "opc":
        print("Calling OPC function...")
        build_and_install_opc_on_linux(release_info, "Debug")
    elif process == "package":
        print("Calling Package function...")
        chmodReleaseDir(release_info)
        make_tar_package(release_info)
    elif process == "taosx" and release_info.UploadAgent == False and release_info.BuildAgent == False:
        print("Calling taosx function...")
        build_and_install_taosx_on_linux(release_info, "Debug")
    elif process == "agent":
        print("Calling taosx agent function...")
        build_and_install_taosx_agent_on_linux(release_info, "Debug")
    elif process == "explorer" and release_info.UploadAgent == False and release_info.BuildAgent == False:
        print("Calling taos-explorer function...")
        install_taos_explorer_on_linux(release_info, "Release")
    elif process == "influxdb":
        print("Calling influxDB function...")
        build_and_install_influxdb_on_linux("Debug")
    elif process == "opentsdb":
        print("Calling opentsDB function...")
        build_and_install_opentsdb_on_linux("Debug")
    else:
        print(f"Invalid -t param: {process}. Please enter valid input.")

def check_directory(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as e:
        print('Error:', e)
        sys.exit()
