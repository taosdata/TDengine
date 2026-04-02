import os
import platform
import sys
import argparse
import shutil
import subprocess
import logging
import datetime
import time
import git
import re
from threading import Thread

current_os = platform.system()
test_process = ''
script_dir = os.getcwd()
directory = "D:\\workspace"
branch = "main"
internal_dir = os.path.join(directory, branch, "TDinternal")
community_dir = os.path.join(internal_dir, "community")
build_dir = os.path.join(community_dir, "debug")
release_dir = os.path.join(community_dir, "release")
odbc_build_type = "Release"
vcvarsall_path = r"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat"

timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logname = f"{timestamp}.log"
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logging.info(f"log file:{logname}")


class Customer:
    def __init__(self, name, email, prompt, grantValue):
        self.Name = name
        self.Email = email
        self.Prompt = prompt
        self.grantValue = grantValue


tdCustomer = Customer("TDengine", "support@taosdata.com", "taos", 10)


class TDVersion:
    def __init__(self, verType, version):
        self.verType = verType
        self.version = version


td_version = TDVersion("enterprise", "1.0.0")


class InstallInfo:
    def __init__(self, directory, branch, internal_dir, community_dir, packaging_dir, install_dir):
        self.directory = directory
        self.branch = branch
        self.internal_dir = internal_dir
        self.community_dir = community_dir
        self.packaging_dir = packaging_dir
        self.install_dir = install_dir
        self.release_dir = ""
        self.packagServerName = ""
        self.packagClientName = ""


install_info = InstallInfo(directory, branch, internal_dir, community_dir, release_dir, install_dir="C:\\TDengine")


def setLog(loglevel='info', log_persist=True):
    # generate a timestamp string
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logname = f"{tdCustomer.Name}_{td_version.verType}_{td_version.version}_{timestamp}.log"
    logging.info(f"log file:{logname}")
    if loglevel == 'info' and log_persist == True:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                            level=logging.INFO, filename=logname)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                            level=logging.DEBUG)


def print_param():
    logging.info(f"Version Type: {td_version.verType}")
    logging.info(f"Version Number: {td_version.version}")
    logging.info(f"Customer Name: {tdCustomer.Name}")
    logging.info(f"Customer Email: {tdCustomer.Email}")
    logging.info(f"Customer Prompt: {tdCustomer.Prompt}")
    logging.info(f"Grant Value: {tdCustomer.grantValue}")
    logging.info(f"internal_dir: {install_info.internal_dir}")
    logging.info(f"community_dir: {install_info.community_dir}")
    logging.info(f"package_dir: {install_info.packaging_dir}")
    logging.info(f"install_dir: {install_info.install_dir}")
    logging.info(f"release_dir: {install_info.release_dir}")
    logging.info(f"packagServerName: {install_info.packagServerName}")
    logging.info(f"packagClientName: {install_info.packagClientName}")
    logging.info(f"Only Client : {only_client}")


def set_package_name():
    logging.info("set_package_name...")
    global install_info
    suffix_package_name = ""
    if current_os == 'Windows':
        suffix_package_name = "-Windows-x64"

    if td_version.verType == "enterprise":
        install_info.packagServerName = tdCustomer.Name + "-enterprise-" + td_version.version + suffix_package_name
        install_info.packagClientName = tdCustomer.Name + "-enterprise-client-" + td_version.version + suffix_package_name
    elif td_version.verType == "industry":
        install_info.packagServerName = tdCustomer.Name + "-" + industry_name + "-" + td_version.version + suffix_package_name
        install_info.packagClientName = tdCustomer.Name + "-" + industry_name + "-client-" + td_version.version + suffix_package_name
    else:
        # if(tdCustomer.Name != "TDengine"):
        #     logging.error("Failed! OEM can not support community version!")
        #     raise Exception("Failed! OEM can not support community version!")
        install_info.packagServerName = tdCustomer.Name + "-server-" + td_version.version + suffix_package_name
        install_info.packagClientName = tdCustomer.Name + "-client-" + td_version.version + suffix_package_name

    logging.info("packagServerName: {0}".format(install_info.packagServerName))
    logging.info("packagClientName: {0}".format(install_info.packagClientName))


def set_release_path():
    logging.info("set_release_path...")

    global install_info

    if td_version.verType != "community":
        install_info.release_dir = os.path.join(internal_dir, "debug")
    else:
        install_info.release_dir = os.path.join(community_dir, "debug")

    if not os.path.exists(install_info.release_dir):
        os.mkdir(install_info.release_dir)
        logging.info(f"mkdir: {install_info.release_dir}")

    logging.info(f"release_dir: {install_info.release_dir}")


def parse_arguments():
    global tdCustomer, td_version, install_info, test_process, script_dir, upload, check_commit, industry_name, only_client

    parser = argparse.ArgumentParser(description='Release TDengine on Windows')
    parser.add_argument('-D', '--directory', type=str, help='set packaging script directory (default: D:\\workspace)',
                        default="D:\\workspace")
    parser.add_argument('-B', '--branch', type=str, help='set branch of the package (default: main)', default="main")
    parser.add_argument('-v', '--version', type=str, help='Set version type (default: community)', default="enterprise")
    parser.add_argument('-n', '--number', type=str, help='Set version number (default: 1.0.0)', default="")
    parser.add_argument('-N', '--customer-name', type=str, help='Set customer name', default="TDengine")
    parser.add_argument('-M', '--customer-email', type=str, help='Set customer email', default="support@taosdata.com")
    parser.add_argument('-P', '--customer-prompt', type=str, help='Set customer prompt', default="taos")
    parser.add_argument('-G', '--grant-value', type=int, help='Set grant value in days (default: 10)', default=10)
    parser.add_argument('-t', '--test_process', help='test single process(pi,opc,taosx, package)', default="")
    parser.add_argument('-I', '--industry-name', type=str, help='Set industry name', default="Power")
    parser.add_argument('-u', '--upload', action="store_false", help='upload package to nas server', default=False)
    parser.add_argument('-c', '--check_commit', action="store_false", help='check commit id', default=False)
    parser.add_argument('-oc', '--only_client', action="store_true", help='only genreate windows client', default=False)

    version_pattern = re.compile('^[0-9]+\\.([0-9]+\\.){1,3}[0-9]+$')
    args = parser.parse_args()
    if args.number == "":
        logging.error("Failed! Version number is empty, please specify version number")
        raise Exception("Version number is empty, please specify version number")
    elif len(args.number) >= 16:
        logging.error("Failed! Length of the version number should be less than 16")
        raise Exception("Length of the version number should be less than 16")
    elif version_pattern.match(args.number) == None:
        logging.error(
            "Failed! Only digits and dots are allowed in the version number, alpha characters are not allowed")
        raise Exception("Only digits and dots are allowed in the version number, alpha characters are not allowed")

    td_version.verType = args.version
    td_version.version = args.number

    tdCustomer.Name = args.customer_name
    tdCustomer.Email = args.customer_email
    tdCustomer.Prompt = args.customer_prompt
    tdCustomer.grantValue = args.grant_value

    install_info.directory = args.directory
    install_info.branch = args.branch
    install_info.install_dir = "C:\\TDengine"
    install_info.internal_dir = os.path.join(args.directory, args.branch, "TDinternal")
    install_info.community_dir = os.path.join(install_info.internal_dir, "community")
    script_dir = os.path.join(install_info.internal_dir, "enterprise", "packaging")
    test_process = args.test_process
    upload = args.upload
    check_commit = args.check_commit
    industry_name = args.industry_name
    only_client = args.only_client


def git_pull(repo_dir, source_branch, target):
    logging.info(f"pull code for {repo_dir}...")
    os.chdir(repo_dir)
    logging.info(f"change directory to {repo_dir}")

    repo = git.Repo(repo_dir)

    try:
        repo.git.checkout("--", ".")
        logging.info(f"{repo_dir}: restore changed files")
        repo.git.reset("--hard")
        logging.info(f"{repo_dir}: reset to HEAD done")

        os.system("git remote prune origin")
        repo.git.checkout(source_branch)
        repo.git.checkout(source_branch)
        logging.info(f"{repo_dir}: checkout {source_branch} done")

        os.system("git gc --prune=now")
        repo.git.pull()
        logging.info(f"{repo_dir}: pull latest code done")
    except Exception as e:
        logging.info(f"ERROR: {repo_dir} pull latest code failed")
        sys.exit(1)
    try:
        repo.git.tag('-d', target)
        logging.info(f"{repo_dir}: delete old tag {target} done")
    except Exception as e:
        logging.info(f"ERROR: {repo_dir} git tag -d {target} failed")
    try:
        if target != "main" and target != "3.1" and target != "3.0":
            repo.git.branch('-D', target)
            logging.info(f"{repo_dir}: delete old branch {target} done")
    except Exception as e:
        logging.info(f"ERROR: {repo_dir} git branch -D {target} failed")
    for i in range(5):
        try:
            repo.git.fetch("--all")
            repo.git.checkout(target)
            break
        except Exception as e:
            logging.error(f"ERROR:{repo_dir} fetch or checkout failed")
        time.sleep(1)


def get_latest_code():
    # pull internal
    git_pull(install_info.internal_dir, install_info.branch, f"release/ver-{td_version.version}")

    # pull grant_lib
    grant_lib_dir = os.path.join(install_info.internal_dir, "enterprise", "contrib", "grant-lib")
    git_pull(grant_lib_dir, install_info.branch, f"ver-{td_version.version}")

    # pull community
    git_pull(install_info.community_dir, install_info.branch, f"release/ver-{td_version.version}")

    # pull taos-tools
    # Comment out the following two lines, as the taos-tools repository has been migrated to the TDengine repository. 2025-02-11
    # tools_dir = os.path.join(install_info.community_dir, "tools", "taos-tools")
    # git_pull(tools_dir, install_info.branch, f"ver-{td_version.version}")

    # # pull taosadapter
    # taosadapter_dir = os.path.join(install_info.community_dir, "tools", "taosadapter")
    # git_pull(taosadapter_dir, install_info.branch, f"ver-{td_version.version}")

    # # pull taosws
    # taosws_dir = os.path.join(install_info.community_dir, "tools", "taosws-rs")
    # git_pull(taosws_dir, "main", f"ver-{td_version.version}")


def init_release_dir():
    logging.info(f"init release directory {install_info.release_dir} ...")
    if os.path.exists(install_info.release_dir):
        os.system(f"rm -rf {install_info.release_dir} ")
        # shutil.rmtree(install_info.release_dir)
    os.mkdir(install_info.release_dir)
    logging.info(f"init release directory {install_info.release_dir} done")


def industry_options():
    if industry_name == "Power":
        return (" -DTD_FUNC_STREAM=false "
                "-DTD_FUNC_SUBSCRIPTION=false -DTD_FUNC_AUDIT=false -DTD_FUNC_CSV=false -DTD_FUNC_VIEW=false "
                "-DTD_FUNC_MULTI_TIER_STORAGE=false -DTD_FUNC_DATA_BAK_RESTORE=false -DTD_FUNC_SHARED_STORAGE=false "
                "-DTD_FUNC_ACTIVE_ACTIVE=false -DTD_FUNC_DUAL_REPLICA_HA=false -DTD_FUNC_DB_ENCRYPTION=false "
                "-DTD_DATAIN_OPC_DA=false -DTD_DATAIN_OPC_UA=false -DTD_DATAIN_PI=false -DTD_DATAIN_KAFKA=false "
                "-DTD_DATAIN_INFLUXDB=false -DTD_DATAIN_MQTT=false -DTD_DATAIN_AVEVAHISTORIAN=false "
                "-DTD_DATAIN_OPENTSDB=false -DTD_DATAIN_TDENGINE_2_6=false -DTD_DATAIN_TDENGINE_3_0=false "
                "-DTD_DATAIN_MYSQL=false -DTD_DATAIN_POSTGRES=false -DTD_DATAIN_ORACLE=false -DTD_DATAIN_MONGODB=false")
    elif industry_name == "Powerfull":
        return (" -DTD_FUNC_STREAM=true "
                "-DTD_FUNC_SUBSCRIPTION=true -DTD_FUNC_AUDIT=true -DTD_FUNC_CSV=true -DTD_FUNC_VIEW=true "
                "-DTD_FUNC_MULTI_TIER_STORAGE=true -DTD_FUNC_DATA_BAK_RESTORE=true -DTD_FUNC_SHARED_STORAGE=true "
                "-DTD_FUNC_ACTIVE_ACTIVE=true -DTD_FUNC_DUAL_REPLICA_HA=true -DTD_FUNC_DB_ENCRYPTION=true "
                "-DTD_DATAIN_OPC_DA=true -DTD_DATAIN_OPC_UA=true -DTD_DATAIN_PI=true -DTD_DATAIN_KAFKA=true "
                "-DTD_DATAIN_INFLUXDB=true -DTD_DATAIN_MQTT=true -DTD_DATAIN_AVEVAHISTORIAN=true "
                "-DTD_DATAIN_OPENTSDB=true -DTD_DATAIN_TDENGINE_2_6=true -DTD_DATAIN_TDENGINE_3_0=true "
                "-DTD_DATAIN_MYSQL=true -DTD_DATAIN_POSTGRES=true -DTD_DATAIN_ORACLE=true -DTD_DATAIN_MONGODB=true")
    elif industry_name == "Powerlite":
        return (" -DTD_FUNC_STREAM=false "
                "-DTD_FUNC_SUBSCRIPTION=false -DTD_FUNC_AUDIT=false -DTD_FUNC_CSV=false -DTD_FUNC_VIEW=false "
                "-DTD_FUNC_MULTI_TIER_STORAGE=false -DTD_FUNC_DATA_BAK_RESTORE=false -DTD_FUNC_SHARED_STORAGE=false "
                "-DTD_FUNC_ACTIVE_ACTIVE=false -DTD_FUNC_DUAL_REPLICA_HA=false -DTD_FUNC_DB_ENCRYPTION=false "
                "-DTD_DATAIN_OPC_DA=false -DTD_DATAIN_OPC_UA=false -DTD_DATAIN_PI=false -DTD_DATAIN_KAFKA=false "
                "-DTD_DATAIN_INFLUXDB=false -DTD_DATAIN_MQTT=false -DTD_DATAIN_AVEVAHISTORIAN=false "
                "-DTD_DATAIN_OPENTSDB=false -DTD_DATAIN_TDENGINE_2_6=false -DTD_DATAIN_TDENGINE_3_0=false "
                "-DTD_DATAIN_MYSQL=false -DTD_DATAIN_POSTGRES=false -DTD_DATAIN_ORACLE=false -DTD_DATAIN_MONGODB=false")
    else:
        return ""


def process_cmake():
    os.chdir(install_info.release_dir)
    logging.info("start cmake...")
    logging.info(f"current path: {os.getcwd()}")
    base_cmd = (f'cmake .. -G "NMake Makefiles JOM" '
                f'-DCMAKE_MAKE_PROGRAM=jom -DCMAKE_BUILD_TYPE=Release -DBUILD_TOOLS=true '
                f'-DWEBSOCKET=true -DBUILD_TEST=false '
                f'-DVERNUMBER={td_version.version} -DCPUTYPE=x64')
    if td_version.verType == 'community':
        cmd = base_cmd + ' -DBUILD_HTTP=false -DBUILD_KEEPER=true'
    else:
        if td_version.verType == "industry":
            TD_INDUSTRY_NAME = "TDengine " + industry_name + " Edition"
            cmd += ' -DTD_INDUSTRY=true' + industry_options()
        else:
            TD_INDUSTRY_NAME = f"{tdCustomer.Name} Enterprise Edition"
        cmd = base_cmd + (f' -DBUILD_EXPLORER=false -DBUILD_TAOSX=false -DBUILD_KEEPER=internal '
                          f'-DASSERT_NOT_CORE=true -DBUILD_HTTP=internal -DCUS_NAME={tdCustomer.Name} '
                          f'-DCUS_PROMPT={tdCustomer.Prompt} -DCUS_EMAIL={tdCustomer.Email} -DTD_PRODUCT_NAME=\"{TD_INDUSTRY_NAME}\" '
                          f'-DGRANT_VALUE={tdCustomer.grantValue} -DJEMALLOC_ENABLED=false ')
    logging.info(cmd)
    try:
        subprocess.check_call(cmd, shell=True)
    except Exception as e:
        logging.error("cmake failed")
        thread1_exception = e
        sys.exit(1)
    os.chdir(script_dir)


def process_build():
    os.chdir(install_info.release_dir)
    logging.info("current dir: {0}".format(os.getcwd()))
    logging.info("start building ....")
    try:
        subprocess.check_call("jom -j8 ", shell=True)
    except Exception as e:
        logging.error(f"ERROR: build failed: {e}")
        sys.exit(1)
    logging.info("build done")
    os.chdir(script_dir)


def process_install():
    os.chdir(install_info.release_dir)
    if os.path.exists(install_info.install_dir):
        shutil.rmtree(install_info.install_dir)
    if not os.path.exists(install_info.install_dir):
        try:
            os.makedirs(install_info.install_dir)
            logging.info(f"install directory {install_info.install_dir} created successfully!")
        except OSError as error:
            logging.error(f"install directory {install_info.install_dir} directory: {error}")
            raise Exception(f"install directory {install_info.install_dir} directory: {error}")
    logging.info("start jom install ....")
    subprocess.check_call("jom -j9 install", shell=True)
    logging.info("make install done")
    bin_dir = os.path.join(install_info.release_dir, "build", "bin")
    verify_commit_id(bin_dir, "taosd.exe", community_dir)

    taosadapter_dir = os.path.join(install_info.community_dir, "tools", "taosadapter")
    verify_commit_id(bin_dir, "taosadapter.exe", taosadapter_dir)

    taos_tools_dir = os.path.join(install_info.community_dir, "tools", "taos-tools")
    verify_commit_id(bin_dir, "taosBenchmark.exe", taos_tools_dir)
    verify_commit_id(bin_dir, "taosdump.exe", taos_tools_dir)

    os.chdir(script_dir)


def process_build_TD():
    get_latest_code()
    process_cmake()
    process_build()


def process_build_taosx():
    if td_version.verType != "community":
        taosx_dir = os.path.join(install_info.directory, install_info.branch, "taosx")
        git_pull(taosx_dir, "main", f"ver-{td_version.version}")

        # remove unnecessary files
        taosx_release_dir = os.path.join(taosx_dir, "release", f"{tdCustomer.Prompt}x")
        if os.path.exists(taosx_release_dir):
            shutil.rmtree(taosx_release_dir)

        if os.path.isfile(f"{taosx_dir}\\target\\taosx.dev.db"):
            os.remove(f"{taosx_dir}\\target\\taosx.dev.db")
        if os.path.isfile(f"{taosx_dir}\\.new"):
            os.remove(f"{taosx_dir}\\.new")

        taosx_packaging_dir = os.path.join(taosx_dir, "packaging")
        os.chdir(taosx_packaging_dir)
        try:
            if td_version.verType == "industry":
                logging.info(f"set VUE_APP_INDUSTRY={industry_name}")
                os.environ["VUE_APP_INDUSTRY"] = industry_name

            subprocess.check_call(
                f"python release.py -ob -vn {td_version.version} -cn {tdCustomer.Name} -cp {tdCustomer.Prompt} -ce {tdCustomer.Email}")
        except Exception as e:
            logging.error(f"ERROR: taosx build failed: {e}")
            sys.exit(1)

        if not os.path.exists(f"{taosx_dir}\\release\\{tdCustomer.Prompt}x"):
            logging.error(f"{tdCustomer.Prompt}x build failed")
            sys.exit(1)

        verify_commit_id(f"{taosx_dir}\\release\\{tdCustomer.Prompt}x\\bin", f"{tdCustomer.Prompt}sx.exe", taosx_dir)
        verify_commit_id(f"{taosx_dir}\\release\\{tdCustomer.Prompt}x\\bin", f"{tdCustomer.Prompt}-explorer.exe", taosx_dir)


def process_build_keeper():
    if td_version.verType != "community":
        keeper_dir = os.path.join(install_info.directory, install_info.branch, "taoskeeperinternal")
        keeper_repo_url = 'github.com/taosdata/taoskeeperinternal'
    else:
        keeper_dir = os.path.join(install_info.directory, install_info.branch, "taoskeeper")
        keeper_repo_url = 'github.com/taosdata/taoskeeper'

    git_pull(keeper_dir, install_info.branch, f"ver-{td_version.version}")

    build_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repo = git.Repo(keeper_dir)
    gitinfo = repo.head.object.hexsha
    buildInfo = "Windows-x64 " + build_time

    os.chdir(keeper_dir)
    try:
        if td_version.verType != "community":
            cmd = f"go build -ldflags=\"-s -w -X 'github.com/taosdata/taoskeeperinternal/version.CUS_NAME={tdCustomer.Name}' -X 'github.com/taosdata/taoskeeperinternal/version.CUS_EMAIL={tdCustomer.Email}' -X 'github.com/taosdata/taoskeeperinternal/version.CUS_PROMPT={tdCustomer.Prompt}' -X 'github.com/taosdata/taoskeeper/version.CUS_NAME={tdCustomer.Name}' -X 'github.com/taosdata/taoskeeper/version.CUS_EMAIL={tdCustomer.Email}l' -X 'github.com/taosdata/taoskeeper/version.CUS_PROMPT={tdCustomer.Prompt}' -X 'github.com/taosdata/taoskeeperinternal/version.Version={td_version.version}' -X 'github.com/taosdata/taoskeeperinternal/version.Gitinfo={gitinfo}' -X 'github.com/taosdata/taoskeeperinternal/version.BuildInfo={buildInfo}'\" -o taoskeeper.exe main.go"
            logging.info(cmd)
            subprocess.call(cmd)
        else:
            subprocess.call(
                f"go build -ldflags=\"-s -w -X '{keeper_repo_url}/version.Version={td_version.version}' -X '{keeper_repo_url}/version.Gitinfo={gitinfo}' -X '{keeper_repo_url}/version.BuildInfo={buildInfo}'\" -o taoskeeper.exe main.go")
    except Exception as e:
        logging.error(f"ERROR: keeper build failed: {e}")
        sys.exit(1)

    verify_commit_id(keeper_dir, "taoskeeper.exe", keeper_dir)


def process_build_taosws_32bit():
    taosws_dir = os.path.join(install_info.release_dir, "community", "tools", "ext_taosws-prefix", "src", "ext_taosws")
    os.chdir(taosws_dir)
    build_dir = os.path.join(taosws_dir, "target", "i686-pc-windows-msvc")

    if (os.path.exists(build_dir)):
        shutil.rmtree(build_dir)

    subprocess.check_call("rustup toolchain install stable-i686", shell=True)
    subprocess.check_call("rustup target add i686-pc-windows-msvc", shell=True)
    subprocess.check_call("cargo build --target=i686-pc-windows-msvc --release -p taos-ws-sys --features rustls",
                          shell=True)

    dll_dir = os.path.join(build_dir, "release")
    x86_target_lib_dir = os.path.join(install_info.install_dir, "taos_odbc", "x86", "lib")
    if not os.path.exists(x86_target_lib_dir):
        os.makedirs(x86_target_lib_dir)
    x86_target_bin_dir = os.path.join(install_info.install_dir, "taos_odbc", "x86", "bin")
    logging.info(f"x86 target bin dir:  {x86_target_bin_dir}")
    if not os.path.exists(x86_target_bin_dir):
        os.makedirs(x86_target_bin_dir)

    logging.info("copy {}\\taosws.dll.lib to {}\\taosws.lib".format(dll_dir, x86_target_lib_dir))
    os.system("copy /Y {}\\taosws.dll.lib {}\\taosws.lib".format(dll_dir, x86_target_lib_dir))
    logging.info("copy  {}\\taosws.dll to {}".format(dll_dir, x86_target_bin_dir))
    os.system("copy /Y {}\\taosws.dll {}".format(dll_dir, x86_target_bin_dir))

    logging.info("32bit taosws build done")


def clone_repository(repo_url, target_dir):
    """
    克隆指定的 Git 仓库到目标目录。

    :param repo_url: 仓库的 URL。
    :param target_dir: 克隆到的目标目录。
    """
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, target_dir],
            check=True,  # 如果命令失败，抛出异常
            stdout=subprocess.PIPE,  # 捕获标准输出
            stderr=subprocess.PIPE,  # 捕获标准错误
            text=True  # 将输出解码为字符串
        )
        logging.info(f"Successfully cloned repository: {repo_url} to {target_dir}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to clone repository: {repo_url}. Error: {e.stderr}")
        raise

def process_build_odbc():
    # only directory/main/3.0  has  taos-connector-odbc
    odbc_dir = os.path.join(directory, branch, "taos-connector-odbc")
    odbc_repo_url = "https://github.com/taosdata/taos-connector-odbc"
    if not os.path.exists(odbc_dir):
        clone_repository(odbc_repo_url, odbc_dir)
        
    os.chdir(odbc_dir)
    os.system("git checkout main")
    os.system("git pull")
    logging.info("current path: {0}".format(os.getcwd()))

    # build 32 bit ODBC
    os.system(f'"{vcvarsall_path}" amd64_x86')
    logging.info("vcvarsall.bat amd64_x86")
    os.system("rm -rf .externals && rm -rf build32")
    os.system(
        '''cmake --no-warn-unused-cli -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DWS_FOR_TEST:STRING=127.0.0.1:6041 -DSERVER_FOR_TEST:STRING=127.0.0.1:6030  -B build32 -G "Visual Studio 17 2022" -A Win32''')
    os.system(f"cmake --build build32 --config {odbc_build_type} -j 8")

    x86_dll_dir = os.path.join(odbc_dir, "build32", "src", f"{odbc_build_type}")
    x86_target_lib_dir = os.path.join(install_info.install_dir, "taos_odbc", "x86", "lib")
    x86_target_bin_dir = os.path.join(install_info.install_dir, "taos_odbc", "x86", "bin")
    os.system("xcopy /YS {}\\taos_odbc.dll {}".format(x86_dll_dir, x86_target_bin_dir))
    os.system("xcopy /YS {}\\taos_odbc.lib {}".format(x86_dll_dir, x86_target_lib_dir))

    x86_template_dir = os.path.join(odbc_dir, "build32", "templates")
    os.system(
        "xcopy /YS {}\\win_odbc_install.ini {}\\taos_odbc\\x86".format(x86_template_dir, install_info.install_dir))

    # build 64 bit ODBC

    driver_dir = os.path.join(install_info.install_dir, "driver")
    x64_target_lib_dir = os.path.join(install_info.install_dir, "taos_odbc", "x64", "lib")
    if not os.path.exists(x64_target_lib_dir):
        os.makedirs(x64_target_lib_dir)
    os.system("copy /Y {}\\taosws.lib {}\\taosws.lib".format(driver_dir, x64_target_lib_dir))
    os.system("copy /Y {}\\taos.lib {}\\taos.lib".format(driver_dir, x64_target_lib_dir))

    os.system("vcvarsall.bat x64")
    os.system("rm .externals -rf && rm build64 -rf")
    subprocess.call(
        "cmake --no-warn-unused-cli -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DWS_FOR_TEST:STRING=127.0.0.1:6041 -DSERVER_FOR_TEST:STRING=127.0.0.1:6030 -B build64 -G \"Visual Studio 17 2022\" -A x64")
    subprocess.call(f"cmake --build build64 --config {odbc_build_type} -j 8")

    x64_dll_dir = os.path.join(odbc_dir, "build64", "src", f"{odbc_build_type}")
    x64_target_bin_dir = os.path.join(install_info.install_dir, "taos_odbc", "x64", "bin")
    if not os.path.exists(x64_target_bin_dir):
        os.makedirs(x64_target_bin_dir)
    os.system("xcopy /YS {}\\taos_odbc.dll {}".format(x64_dll_dir, x64_target_bin_dir))
    os.system("xcopy /YS {}\\taos_odbc.lib {}".format(x64_dll_dir, x64_target_lib_dir))

    x64_template_dir = os.path.join(odbc_dir, "build64", "templates")
    os.system(
        "xcopy /YS {}\\win_odbc_install.ini {}\\taos_odbc\\x64".format(x64_template_dir, install_info.install_dir))


def process_download_odbc():
    subprocess.call(
        "powershell -command \"Start-BitsTransfer -Source https://github.com/taosdata/taos_odbc/releases/download/v1.0.0.0/taos_odbc_install_files.zip -Destination taos_odbc_install_files.zip\"")
    subprocess.call(f"powershell -command \"Expand-Archive taos_odbc_install_files.zip {install_info.install_dir}\"")


def process_add_enterprise_extent():
    connector_install_dir = os.path.join(install_info.install_dir, "connector")

    if os.path.exists(connector_install_dir):
        shutil.rmtree(connector_install_dir)
    os.makedirs(connector_install_dir)
    logging.info("start download connectors ...")
    jdbc_connector_url = "https://github.com/taosdata/taos-connector-jdbc"
    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-jdbc {}\\JDBC" \
                          .format(connector_install_dir), shell=True)
    
    os.chdir(f"{connector_install_dir}\\JDBC")
    os.system("mvn clean package -Dmaven.test.skip=true")
    os.system("mv target/*.jar {}/".format(connector_install_dir))
    os.chdir(f"{connector_install_dir}")
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'JDBC')}")

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/driver-go {}\\go" \
                          .format(connector_install_dir), shell=True)
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'go', '.git')}")

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-python {}\\python" \
                          .format(connector_install_dir), shell=True)
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'python', '.git')}")

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-node {}\\nodejs" \
                          .format(connector_install_dir), shell=True)
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'nodejs', '.git')}")

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-dotnet {}\\dotnet" \
                          .format(connector_install_dir), shell=True)
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'dotnet', '.git')}")

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-rust {}\\rust" \
                          .format(connector_install_dir), shell=True)
    os.system(f"rmdir /s /q {os.path.join(connector_install_dir, 'rust', '.git')}")

    logging.info("download connectors done")

    logging.info("start copy examples ...")
    examples_install_dir = os.path.join(install_info.install_dir, "examples")
    os.system("md {}".format(examples_install_dir))
    examples_dir = os.path.join(install_info.community_dir, "examples")
    os.system('echo "xcopy {} to {}"'.format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\c\\* {}\\c\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\JDBC\\* {}\\JDBC\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\matlab\\* {}\\matlab\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\python\\* {}\\python\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\R\\* {}\\R\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\go\\* {}\\go\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\nodejs\\* {}\\nodejs\\".format(examples_dir, examples_install_dir))
    os.system("xcopy /S {}\\C#\\* {}\\C#\\".format(examples_dir, examples_install_dir))
    os.system("md {}\\taosbenchmark-json".format(examples_install_dir))
    os.system("xcopy /S {}\\tools\\taos-tools\\example\\* {}\\taosbenchmark-json\\" \
              .format(install_info.community_dir, examples_install_dir))
    logging.info("copy examples done")

    logging.info("start copy dll ...")
    dll_dir = os.path.join(install_info.community_dir, "tools", "taos-tools", "packaging", "win")
    os.system("xcopy {}\\*.dll {}".format(dll_dir, install_info.install_dir))
    logging.info("copy dll done")


def copy_taosx_files():
    if td_version.verType != "community":
        taosx_source_dir = os.path.join(install_info.directory, install_info.branch, "taosx", "release", f"{tdCustomer.Prompt}x")

        os.system(f"xcopy /S /E {taosx_source_dir}\\plugins\\* {install_info.install_dir}\\plugins\\*")
        os.system(f"xcopy /S {taosx_source_dir}\\bin {install_info.install_dir}\\*")
        os.system(f"xcopy /S {taosx_source_dir}\\append {install_info.install_dir}\\append\\*")
        os.system(f"xcopy /S {taosx_source_dir}\\config {install_info.install_dir}\\cfg\\*")


# get keeper  from tdinternal repo not single repo so comment this function
# def copy_keeper_files():
#     if td_version.verType != "community":
#         keeper_dir = os.path.join(install_info.directory, install_info.branch, "taoskeeperinternal")
#     else:
#         keeper_dir = os.path.join(install_info.directory, install_info.branch, "taoskeeper")

#     os.system(f"xcopy {keeper_dir}\\taoskeeper.exe {install_info.install_dir}\\")
#     os.system(f"xcopy {keeper_dir}\\config\\taoskeeper.toml {install_info.install_dir}\\cfg\\")


def write_server_install_file():
    """
    This function creates a text file with installation instructions for OEM Server on Windows operating system.
    """
    with open(f"{install_info.internal_dir}\\enterprise\\packaging\\windows\\windows_before_install.txt", "w") as f:
        f.write(f"{tdCustomer.Name} will be installed under C:\\{tdCustomer.Name}, "
                f"users can modify configuration file C:\\{tdCustomer.Name}\\cfg\\{tdCustomer.Name}.cfg, "
                f"set the log file path or other parameters.\n")
        f.write(
            f"- To start/stop {tdCustomer.Name} with administrator privileges:  sc.exe start/stop {tdCustomer.Prompt}d\n")
        f.write(
            f"- To start/stop{tdCustomer.Name}Adapter with administrator privileges: sc.exe start/stop {tdCustomer.Name}adapter\n")
        f.write(f"- To access {tdCustomer.Name} from your local machine, run {tdCustomer.Prompt}\n")
        f.write(f"- Please manually remove C:\\{tdCustomer.Name} from your system PATH environment "
                f"after you remove {tdCustomer.Name} software.")


def write_client_install_file():
    """
    This function creates a text file with installation instructions for OEM client on Windows operating system.
    """
    with open(f"{install_info.community_dir}\\packaging\\tools\\windows_before_install.txt", "w") as f:
        f.write("Once it's installed, please take the steps below:\n")
        f.write("1: Open Terminal on your local machine.\n")
        f.write(f"2: To connect to a {tdCustomer.Name} server using the default settings "
                f"and credentials, run the {tdCustomer.Prompt} command.\n")
        f.write(f"3: To connect to a {tdCustomer.Name} server using custom settings or credentials, "
                f"run {tdCustomer.Prompt} --help for more information.")


def write_client_OEM_install_file():
    """
    This function creates a text file with installation instructions for OEM client on Windows operating system.
    """
    with open(f"{install_info.community_dir}\\packaging\\tools\\windows_before_install.txt", "w") as f:
        f.write("Once it's installed, please take the steps below:\n")
        f.write("1: Open Terminal on your local machine.\n")
        f.write(f"2: To connect to a {tdCustomer.Name} server using the default settings "
                f"and credentials, run the {tdCustomer.Prompt} command.\n")
        f.write(f"3: To connect to a {tdCustomer.Name} server using custom settings or credentials, "
                f"run {tdCustomer.Prompt} --help for more information.")


def process_package_server():
    if td_version.verType == "community":
        iss_path = os.path.join(install_info.community_dir, "packaging", "tools", "tdengine.iss")
        ico_path = os.path.join(install_info.community_dir, "packaging", "tools", 'favicon.ico')
    else:
        if tdCustomer.Name == "TDengine":
            iss_path = os.path.join(install_info.internal_dir, "enterprise", "packaging", "windows", "tdengine.iss")
            ico_path = os.path.join(install_info.internal_dir, "enterprise", "packaging", "windows", 'favicon.ico')
        else:
            iss_path = os.path.join(install_info.internal_dir, "enterprise", "packaging", "oem_release_cfg",
                                    "server_oem.iss")
            ico_path = os.path.join(install_info.internal_dir, "enterprise", "packaging", "oem_release_cfg",
                                    f'{tdCustomer.Prompt}.ico')

    logging.info(f"packaging {install_info.packagServerName} server...")
    write_server_install_file()

    logging.info("OEM name is :" + tdCustomer.Name)
    logging.info("OEM prompt is :" + tdCustomer.Prompt)
    logging.info("OEM email is :" + tdCustomer.Email)

    # keep same method with old 3.1 code
    if install_info.branch == "3.1" and tdCustomer.Name == "TDengine":
        odbc_dir = os.path.join(install_info.install_dir, "taos_odbc")
        if os.path.exists(odbc_dir):
            shutil.rmtree(odbc_dir)

    try:
        package_process_cmd = f"iscc /DMyAppInstallName=\"{install_info.packagServerName}\" \
                /DMyAppIco=\"{ico_path}\" \
                /DMyAppInstallDir=\"C:\\{tdCustomer.Name}\" \
                /DMyAppVersion=\"{td_version.version}\" \
                /DMyAppExcludeSource=\"tmq*.exe,tsim.exe, create_table.exe, runUdf.exe, dumper.exe\" \
                /DCusName=\"{tdCustomer.Name}\" \
                /DCusPrompt=\"{tdCustomer.Prompt}\" \
                {iss_path} /O{install_info.directory}\\{install_info.branch}\\release"
        logging.info(f"start to package process:\n{package_process_cmd}")

        subprocess.check_call(package_process_cmd, shell=True)
        logging.info(f"packaging {install_info.packagServerName} server done")
    except Exception as e:
        logging.error(f"ERROR: packaging {install_info.packagServerName} server failed: {e}")
        sys.exit(1)

    if upload is True:
        file = os.path.join(install_info.directory, install_info.branch, "release",
                            f"{install_info.packagServerName}.exe")
        subprocess.check_call(
            f"ssh root@192.168.1.213 \"mkdir -p /nas/TDengine/v{td_version.version}/{td_version.verType}\" ",
            shell=True)
        subprocess.check_call(f"scp {file} root@192.168.1.213:/nas/TDengine/v{td_version.version}/{td_version.verType}",
                              shell=True)


def process_package_client():
    iss_path = os.path.join(install_info.community_dir, "packaging", "tools", "tdengine.iss")
    ico_path = os.path.join(install_info.community_dir, "packaging", "tools", 'favicon.ico')

    logging.info(f"packaging {install_info.packagClientName} ...")
    write_client_install_file()
    logging.info(ico_path)

    try:
        subprocess.check_call(f"iscc /DMyAppInstallName=\"{install_info.packagClientName}\" \
              /DMyAppIco=\"{ico_path}\" \
              /DMyAppVersion=\"{td_version.version}\" \
              /DMyAppExcludeSource=\"taosd.exe,tmq*.exe,tsim.exe, create_table.exe, runUdf.exe, dumper.exe, taosudf.exe, taoskeeper.exe, taosadapter.exe\" \
              /DCusName=\"{tdCustomer.Name}\" \
              /DCusPrompt=\"{tdCustomer.Prompt}\" \
              {iss_path} /O{install_info.directory}\\{install_info.branch}\\release", shell=True)
    except Exception as e:
        logging.error(f"ERROR: packaging {install_info.packagClientName} client failed: {e}")
        sys.exit(1)

    if upload is True:
        file = os.path.join(install_info.directory, install_info.branch, "release",
                            f"{install_info.packagClientName}.exe")
        subprocess.check_call(
            f"ssh root@192.168.1.213 \"mkdir -p /nas/TDengine/v{td_version.version}/{td_version.verType}\" ",
            shell=True)
        subprocess.check_call(f"scp {file} root@192.168.1.213:/nas/TDengine/v{td_version.version}/{td_version.verType}",
                              shell=True)


def process_OEM_client_rename_process():
    # change file
    os.chdir(install_info.install_dir)
    cfg_dir = os.path.join(install_info.install_dir, "cfg")
    os.chdir(cfg_dir)
    for filename in os.listdir(cfg_dir):
        replace_in_file(filename)

    # iss_path = os.path.join(script_dir,"oem_release_cfg","oem.iss")
    # replace_iss_in_file(iss_path)

    # add new client.bat replacing  for oem
    start_dir = os.path.join(script_dir, "oem_release_cfg")
    os.chdir(start_dir)
    replace_in_file("client.bat")


def process_package_OEM_client():
    iss_path = os.path.join(script_dir, "oem_release_cfg", "oem.iss")
    # ico_path = os.path.join(script_dir,"oem_release_cfg",f"{tdCustomer.Prompt}.ico")
    ico_path = f"{tdCustomer.Prompt}.ico"

    logging.info(f"packaging OEM({install_info.packagClientName}) client...")
    write_client_install_file()
    os.system(f"iscc /DMyAppInstallName=\"{install_info.packagClientName}\" \
                /DMyAppIco=\"{ico_path}\" \
                /DMyAppInstallDir=\"C:\\{tdCustomer.Name}\" \
                /DMyAppVersion=\"{td_version.version}\" \
                /DMyAppExcludeSource=\"taosd.exe,tmq*.exe,tsim.exe,taosadapter.exe,taoskeeper.exe,create_table.exe,runUdf.exe,taosudf.exe,dumper.exe\" \
                /DCusName=\"{tdCustomer.Name}\" \
                /DCusPrompt=\"{tdCustomer.Prompt}\" \
                {iss_path} /O{install_info.directory}\\{install_info.branch}\\release")
    logging.info(f"packaging {install_info.packagClientName}  done")


def rename_files_in_directory(directory):
    # Loop through all files in the given directory
    for filename in os.listdir(directory):
        # Check if the filename starts with 'taos'
        if filename.startswith('taos'):
            new_filename = tdCustomer.Prompt + filename[4:]  # Skip 'taos'
            # Get full file paths
            old_file = os.path.join(directory, filename)
            new_file = os.path.join(directory, new_filename)
            # Rename the file
            os.rename(old_file, new_file)
            logging.info(f'Renamed: {filename} -> {new_filename}')


def replace_in_file(file_path):
    # Read in the file
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace('/taos', f'/{tdCustomer.Prompt}')
    filedata = filedata.replace('taosx', f'{tdCustomer.Prompt}x')
    filedata = filedata.replace('taosX', f'{tdCustomer.Prompt}X')
    filedata = filedata.replace('taosd', f'{tdCustomer.Prompt}d')
    filedata = filedata.replace('taosadapter', f'{tdCustomer.Prompt}adapter')
    filedata = filedata.replace('taoskeeper', f'{tdCustomer.Prompt}keeper')
    filedata = filedata.replace('taos-', f'{tdCustomer.Prompt}-')
    filedata = filedata.replace('taos_odbc\\', f'{tdCustomer.Prompt}_odbc\\')
    upper_name = tdCustomer.Prompt.upper()
    filedata = filedata.replace('TAOS_', f'{upper_name}_')
    filedata = filedata.replace('TDengine', tdCustomer.Name)

    # Write the file out again
    with open(file_path, 'w') as file:
        file.write(filedata)


def replace_iss_in_file(file_path):
    # Read in the file
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        filedata = file.read()

    # Replace the target string
    # define oem icon
    filedata = filedata.replace('oemdefine', tdCustomer.Prompt)
    # Write the file out again
    with open(file_path, 'w') as file:
        file.write(filedata)


def process_OEM_rename_process():
    # change file and directory names
    os.chdir(install_info.install_dir)
    odbc_dir = os.path.join(install_info.install_dir, "taos_odbc")
    if os.path.exists(odbc_dir):
        os.rename(odbc_dir, f"{tdCustomer.Prompt}_odbc")

    rename_files_in_directory(install_info.install_dir)
    cfg_dir = os.path.join(install_info.install_dir, "cfg")
    rename_files_in_directory(cfg_dir)

    pi_dir = os.path.join(install_info.install_dir, "plugins", "pi")
    rename_files_in_directory(pi_dir)

    # change file content
    os.chdir(install_info.install_dir)
    if os.path.exists(f"{tdCustomer.Prompt}x-srv.xml"):
        logging.info(f'replace content: {tdCustomer.Prompt}x-srv.xml')
        replace_in_file(f"{tdCustomer.Prompt}x-srv.xml")
    else:
        logging.info(f'{tdCustomer.Prompt}x-srv.xml not found')
    if os.path.exists(f"{tdCustomer.Prompt}-explorer-srv.xml"):
        logging.info(f'replace content: {tdCustomer.Prompt}-explorer-srv.xml')
        replace_in_file(f"{tdCustomer.Prompt}-explorer-srv.xml")
    else:
        logging.info(f'{tdCustomer.Prompt}-explorer-srv.xml not found')

    os.chdir(cfg_dir)
    for filename in os.listdir(cfg_dir):
        replace_in_file(filename)

    start_dir = os.path.join(install_info.internal_dir, "enterprise", "packaging", "windows")
    os.chdir(start_dir)
    replace_in_file("start-all.bat")
    replace_in_file("stop-all.bat")
    replace_in_file("taos.bat")

    # not sure oem need this 
    if tdCustomer.Name == "TDengine":
        odbc_x86_dir = os.path.join(install_info.install_dir, f"{tdCustomer.Prompt}_odbc", "x86")
        os.chdir(odbc_x86_dir)
        replace_in_file("win_odbc_install.ini")

        odbc_x64_dir = os.path.join(install_info.install_dir, f"{tdCustomer.Prompt}_odbc", "x64")
        os.chdir(odbc_x64_dir)
        replace_in_file("win_odbc_install.ini")


def process_package():
    process_package_server()


def get_commit_id(repo_dir):
    repo = git.Repo(repo_dir)
    return repo.head.object.hexsha


def verify_commit_id(bin_dir, bin_name, repo_dir):
    if check_commit is False:
        return

    if not os.path.exists(f"{bin_dir}/{bin_name}"):
        logging.error(f"{bin_dir}/{bin_name} not found")
        sys.exit(1)

    if bin_name == "taosd.exe" and td_version.verType != "community":
        except_internal_commit_id = get_commit_id(internal_dir)
        except_community_commit_id = get_commit_id(community_dir)
        cmd = [f'{bin_dir}/{bin_name} -V | findstr "git"']
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            actual_community_commit_id = result.stdout.splitlines()[0].split(" ")[1]
            actual_internal_commit_id = result.stdout.splitlines()[1].split(" ")[1]
        except Exception as e:
            logging.error(f"ERROR: get commit id failed: {e}")
            sys.exit(1)

        length_community_commit_id = len(except_community_commit_id)
        length_internal_commit_id = len(except_internal_commit_id)

        if actual_community_commit_id[:length_community_commit_id] != except_community_commit_id:
            logging.error(
                f"community commit id not match, except: {except_community_commit_id}, actual: {actual_community_commit_id}")
            sys.exit(1)
        if actual_internal_commit_id[:length_internal_commit_id] != except_internal_commit_id:
            logging.error(
                f"internal commit id not match, except: {except_internal_commit_id}, actual: {actual_internal_commit_id}")
            sys.exit(1)
    else:
        except_commit_id = get_commit_id(repo_dir)
        cmd = [f'{bin_dir}/{bin_name} -V | findstr "git"']
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            actual_commit_id = result.stdout.split()[1]
        except Exception as e:
            logging.error(f"ERROR: get commit id failed: {e}")
            sys.exit(1)

        length_commit_id = len(except_commit_id)
        if actual_commit_id[:length_commit_id] != except_commit_id:
            logging.error(f"commit id not match, except: {except_commit_id}, actual: {actual_commit_id}")
            sys.exit(1)

    if bin_name == "taosd.exe" or bin_name == f"{tdCustomer.Prompt}x.exe" or bin_name == f"{tdCustomer.Prompt}-explorer.exe":
        index = 2
    else:
        index = 1

    cmd = [f'{bin_dir}/{bin_name} -V | findstr "version"']
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        actual_version = result.stdout.split(" ")[index]
    except Exception as e:
        logging.error(f"ERROR: get version failed: {e}")
        sys.exit(1)

    if actual_version != td_version.version:
        logging.error(f"version not match, except: {td_version.version}, actual: {actual_version}")
        sys.exit(1)


def testHanle(process):
    logging.info("Process Test:", process)
    if process == "init":
        logging.info("Calling init...")
        init_release_dir()
    elif process == "cmake":
        logging.info("Calling cmake...")
        process_cmake()
    elif process == "build":
        logging.info("Calling cmake build...")
        process_build()
    elif process == "install":
        logging.info("Calling install...")
        process_install()
    elif process == "extent":
        logging.info("Calling install extent...")
        process_add_enterprise_extent()
    elif process == "package":
        logging.info("Calling package...")
        process_package()
    elif process == "packages":
        logging.info("Calling package server...")
        process_package_server()
    elif process == "packagec":
        logging.info("Calling package client...")
        process_package_client()
    elif process == "packageoem":
        logging.info("Calling package oem client...")
        process_OEM_client_rename_process()
        process_package_OEM_client()
    elif process == "packageoems":
        logging.info("Calling package oem server...")
        copy_taosx_files()
        process_OEM_rename_process()
        process_package_server()
    else:
        logging.info("Invalid input. Please enter valid input.")


def set_win_dev_env(vcvarsall_path):
    """
    设置 Windows 开发环境的环境变量。

    :param vcvarsall_path: 必选，指定 vcvarsall.bat 的路径。
    """

    # 检查路径是否存在
    if not os.path.exists(vcvarsall_path):
        logging.error(f"vcvarsall.bat not found at: {vcvarsall_path}")
        raise FileNotFoundError(f"vcvarsall.bat not found at: {vcvarsall_path}")

    try:
        # 调用 vcvarsall.bat 并捕获输出
        result = subprocess.run(
            f'"{vcvarsall_path}" x64 && set',  # 调用 vcvarsall.bat 并设置环境变量
            shell=True,  # 允许使用 shell 命令
            check=True,  # 如果命令失败，抛出异常
            stdout=subprocess.PIPE,  # 捕获标准输出
            stderr=subprocess.PIPE,  # 捕获标准错误
            text=True  # 将输出解码为字符串
        )

        # 解析输出并设置环境变量
        output = result.stdout
        for line in output.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value
        logging.info("Windows development environment variables set successfully.")
    except subprocess.CalledProcessError as e:
        # 捕获命令执行错误并打印详细信息
        logging.error(f"Error executing vcvarsall.bat: {e.stderr}")
        raise


def os_check():
    if current_os != 'Windows':
        logging.info("Failed! This script only for windows!")
        sys.exit(1)
    else:
        set_win_dev_env(vcvarsall_path)


if __name__ == "__main__":
    start_time = time.time()
    os_check()
    logging.info("Release tdengine on windows start...")
    parse_arguments()
    set_package_name()

    set_release_path()
    print_param()

    if test_process != '':
        testHanle(test_process)
        sys.exit(1)

    init_release_dir()

    # p1 = Process(target=process_build_TD)
    # p2 = Process(target=process_build_taosx)
    # p3 = Process(target=process_build_keeper)

    # # p1.start()
    # # p2.start()
    # # p3.start()

    # # p1.join()
    # # p2.join()
    # # p3.join()
    try:
        logging.info("start to build taosd and taosx")
        process_build_TD()
        process_build_taosx()
        logging.info("finish  building taosd nad taosx ")

        # # add -DBUILD_KEEPER=true  and comment process_build_keeper
        # process_build_keeper()
        process_install()
        if install_info.branch != "3.1":
            if tdCustomer.Name == "TDengine":
                process_build_taosws_32bit()
                process_build_odbc()
                process_add_enterprise_extent()
                process_package_client()
            else:
                process_OEM_client_rename_process()
                process_package_OEM_client()
        else:
            process_download_odbc()
            process_add_enterprise_extent()
            process_package_client()

        if only_client is False:
            if td_version.verType != "community":
                copy_taosx_files()
                if tdCustomer.Name != "TDengine":
                    process_OEM_rename_process()
            process_package_server()
    except Exception as e:
        logging.error(f"ERROR: build or package on Windows failed: {e}")
        sys.exit(1)

    end_time = time.time()
    excute_time = (end_time - start_time) / 60
    logging.info(f"Total time: {excute_time} mins")
