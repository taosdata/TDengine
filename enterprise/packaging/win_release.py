import os
import platform
import sys
import argparse
import shutil
import subprocess
import logging
import requests

current_os = platform.system()
test_process = ''
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

class Customer:
    def __init__(self, name, email, prompt, grantValue):
        self.Name = name
        self.Email = email
        self.Prompt = prompt
        self.grantValue = grantValue
tdCustomer = Customer("TDengine", "support@taosdata.com", "taos", 60)

class TDVersion:
    def __init__(self, verType, version):
        self.verType = verType
        self.version = version
td_version = TDVersion("cluster", "1.0.0")

class InstallInfo:
    def __init__(self, internal_dir, community_dir, packaging_dir, install_dir):
        self.internal_dir = internal_dir
        self.community_dir = community_dir
        self.packaging_dir = packaging_dir
        self.install_dir = install_dir
        self.release_dir = ""
        self.packagServerName = ""
        self.packagClientName = ""
install_info = InstallInfo(os.path.abspath(os.path.join(os.getcwd(), "..", "..")), 
                           os.path.abspath(os.path.join(os.getcwd(), "..", "..", "community")), 
                           os.getcwd(), 
                           "C:\\TDengine")

        
def print_param():
    print("Version Type:", td_version.verType)
    print("Version Number:", td_version.version)
    
    print("Customer Name:", tdCustomer.Name)
    print("Customer Email:", tdCustomer.Email)
    print("Customer Prompt:", tdCustomer.Prompt)
    print("Grant Value: ", tdCustomer.grantValue)
    
    print("internal_dir:", install_info.internal_dir)
    print("community_dir: ", install_info.community_dir)
    print("package_dir:", install_info.packaging_dir)
    print("install_dir: ", install_info.install_dir)
    print("release_dir: ", install_info.release_dir)
    print("packagServerName:", install_info.packagServerName)
    print("packagClientName: ", install_info.packagClientName)
       
def set_package_name():
    logging.info("set_package_name...")
    global install_info
    suffix_package_name = ""
    if current_os == 'Windows':
        suffix_package_name = "-Windows-x64"
    if td_version.verType == "cluster":
        install_info.packagServerName = tdCustomer.Name + "-enterprise-server-" + td_version.version + suffix_package_name
        install_info.packagClientName = tdCustomer.Name + "-enterprise-client-" + td_version.version + suffix_package_name
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
    if td_version.verType == "cluster":
        install_info.release_dir = os.path.join(install_info.internal_dir, "debug")
    else:
        install_info.release_dir = os.path.join(install_info.community_dir, "debug")
    if not os.path.exists(install_info.release_dir):
        os.mkdir(install_info.release_dir)
        logging.info("mkdir:", install_info.release_dir)

    install_info.release_dir = os.path.join(install_info.release_dir, f"ver-{td_version.version}-x64")
    logging.info("release_dir: {0}".format(install_info.release_dir))
    
def parse_arguments():
    global tdCustomer, td_version, install_info, test_process
    
    parser = argparse.ArgumentParser(description='Release TDengine on Windows')
    parser.add_argument('-v', '--version', type=str, help='Set version type (default: community)', default="cluster")
    parser.add_argument('-n', '--number', type=str, help='Set version number (default: 1.0.0)', default="1.0.0")
    parser.add_argument('-N', '--customer-name', type=str, help='Set customer name', default="TDengine")
    parser.add_argument('-M', '--customer-email', type=str, help='Set customer email', default="support@taosdata.com")
    parser.add_argument('-P', '--customer-prompt', type=str, help='Set customer prompt', default="taos")
    parser.add_argument('-G', '--grant-value', type=int, help='Set grant value in days (default: 60)', default=60)
    parser.add_argument('-t', '--test_process', help='test single process(pi,opc,taosx, package)', default="")
        
    args = parser.parse_args()
    td_version.verType = args.version
    td_version.version = args.number
    tdCustomer.Name = args.customer_name
    tdCustomer.Email = args.customer_email
    tdCustomer.Prompt = args.customer_prompt
    tdCustomer.grantValue = args.grant_value
    test_process = args.test_process
   
def generate_community_dir():
    logging.info("generate community package directory ...")
    if not os.path.exists(install_info.community_dir):
        os.chdir(install_info.internal_dir)
        if os.path.exists('debug'):
            shutil.rmtree('debug')
        os.mkdir("debug")
        os.chdir("debug")
        # os.system("cmake ..")
        # subprocess.check_call("cmake", shell=True)
        # logging.info("cmake success")

def init_release_dir():
    logging.info(f"init release directory {install_info.release_dir} ...")
    if os.path.exists(install_info.release_dir):
        shutil.rmtree(install_info.release_dir)
    os.mkdir(install_info.release_dir)
    logging.info(f"init release directory done")
    

def process_cmake():
    os.chdir(install_info.release_dir)
    logging.info("start cmake...")
    logging.info("current path: {0}".format(os.getcwd()))
     
    if td_version.verType == 'cluster':
        cmd = (f'cmake ../../ -G "NMake Makefiles JOM" '
            f'-DCMAKE_MAKE_PROGRAM=jom -DBUILD_TOOLS=true '
            f'-DBUILD_EXPLORER=true -DBUILD_TAOSX=false -DWEBSOCKET=true '
            f'-DBUILD_HTTP=internal -DBUILD_TEST=false -DVERNUMBER={td_version.version} '
            f'-DCPUTYPE=x64 -DCUS_NAME={tdCustomer.Name} -DCUS_PROMPT={tdCustomer.Prompt} '
            f'-DCUS_EMAIL={tdCustomer.Email} -DGRANT_VALUE={tdCustomer.grantValue}')
    else:
        cmd = (f'cmake ../../ -G "NMake Makefiles JOM" '
            f'-DCMAKE_MAKE_PROGRAM=jom -DBUILD_TOOLS=true '
            f'-DWEBSOCKET=true -DBUILD_HTTP=false -DBUILD_TEST=false '
            f'-DVERNUMBER={td_version.version} -DCPUTYPE=x64')

    logging.info(cmd)
    subprocess.check_call(cmd, shell=True)

    
def process_build():
    os.chdir(install_info.release_dir)
    logging.info("current dir: {0}".format(os.getcwd()))
    logging.info("start building ....")
    subprocess.check_call("cmake --build .", shell=True)
    logging.info("build done")
    
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
    logging.info("start make install ....")
    subprocess.check_call("cmake --install .", shell=True)
    logging.info("make install done")

def process_add_enterprice_extent():
    connector_install_dir = os.path.join(install_info.install_dir, "connector")
    
    jdbc_links=[
        'https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.1/taos-jdbcdriver-3.2.1-dist.jar',
        'https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.1/taos-jdbcdriver-3.2.1-sources.jar',
        'https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.1/taos-jdbcdriver-3.2.1.jar'
    ]
    logging.info("start download connectors ...")
    for link in jdbc_links:
        subprocess.check_call(['wget',link,'-P',connector_install_dir])
    
    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/driver-go {}/go"\
                   .format(connector_install_dir), shell=True)
      
    subprocess.check_call("md {}".format(connector_install_dir), shell=True)
    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/driver-go {}/go"\
                   .format(connector_install_dir), shell=True)
    subprocess.check_call("rm -rf {}/go/.git*".format(connector_install_dir), shell=True)

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-python {}/python"\
                   .format(connector_install_dir), shell=True)
    subprocess.check_call("rm -rf {}/python/.git*".format(connector_install_dir), shell=True)

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-node {}/nodejs"\
                   .format(connector_install_dir), shell=True)
    subprocess.check_call("rm -rf {}/nodejs/.git*".format(connector_install_dir), shell=True)

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-dotnet {}/dotnet"\
                   .format(connector_install_dir), shell=True)
    subprocess.check_call("rm -rf {}/dotnet/.git*".format(connector_install_dir), shell=True)

    subprocess.check_call("git clone --depth 1 https://github.com/taosdata/taos-connector-rust {}/rust"\
                   .format(connector_install_dir), shell=True)
    subprocess.check_call("rm -rf {}/rust/.git*".format(connector_install_dir), shell=True)
    
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
    os.system("xcopy /S {}\\tools\\taos-tools\\example\\* {}\\taosbenchmark-json\\"\
                   .format(install_info.community_dir, examples_install_dir))
    logging.info("copy examples done")
    
def write_TDengine_server_install_file():
    """
    This function creates a text file with installation instructions for TDengine Server on Windows operating system.
    """
    with open(f"{install_info.community_dir}\\packaging\\tools\\windows_before_install.txt", "w") as f:
        f.write(f"{tdCustomer.Name} is an open-source, cloud-native time-series database "
                f"optimized for Internet of Things (IoT), Connected Cars, and Industrial IoT. "
                f"With its built-in caching, stream processing, and data subscription capabilities, "
                f"TDengine offers a simplified solution for time-series data processing.\n\n")
        f.write(f"{tdCustomer.Name} will be installed under {install_info.install_dir}, "
                f"users can modify configuration file {install_info.install_dir}\\cfg\\taos.cfg, "
                f"set the log file path or other parameters.\n")
        f.write(f"- To start/stop {tdCustomer.Name} with administrator privileges: run sc start/stop taosd\n")
        f.write("- To start/stop taosAdapter with administrator privileges: run sc start/stop taosadapter\n")
        f.write(f"- To access{tdCustomer.Name} from your local machine, run {tdCustomer.Prompt}\n")
        f.write(f"- Please manually remove {install_info.install_dir} "
                f"from your system PATH environment after you remove {tdCustomer.Name} software.")
  
def write_TDengine_client_install_file():
    """
    This function creates a text file with installation instructions for TDengine client on Windows operating system.
    """
    with open(f"{install_info.community_dir}\\packaging\\tools\\windows_before_install.txt", "w") as f:
        f.write("TDengine is an open-source, cloud-native time-series database "
                f"optimized for Internet of Things (IoT), Connected Cars, and Industrial IoT. "
                f"With its built-in caching, stream processing, and data subscription capabilities, "
                f"TDengine offers a simplified solution for time-series data processing.\n\n")
        f.write("After the installation process is complete, perform the following steps to start using TDengine:\n")
        f.write("1: Open Terminal on your local machine.\n")
        f.write("2: To connect to a TDengine server using the default settings and credentials, run the taos command.\n")
        f.write("3: To connect to a TDengine server using custom settings or credentials, run taos --help for more information.\n")
        f.write("4: To connect to TDengine Cloud, follow the instructions on the Tools - TDengine CLI page in your TDengine Cloud account.") 

def write_server_install_file():
    """
    This function creates a text file with installation instructions for OEM Server on Windows operating system.
    """
    with open(f"{install_info.community_dir}\\packaging\\tools\\windows_before_install.txt", "w") as f:
        f.write(f"{tdCustomer.Name} will be installed under {install_info.install_dir}, "
                f"users can modify configuration file {install_info.install_dir}\\cfg\\taos.cfg, "
                f"set the log file path or other parameters.\n")
        f.write(f"- To start/stop {tdCustomer.Name} with administrator privileges:  sc start/stop {tdCustomer.Prompt}d\n")
        f.write("- To start/stop taosAdapter with administrator privileges: sc start/stop taosadapter\n")
        f.write(f"- To access {tdCustomer.Name} from your local machine, run {tdCustomer.Prompt}\n")
        f.write(f"- Please manually remove {install_info.install_dir} from your system PATH environment "
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

def process_package_server():
    iss_path = os.path.join(install_info.community_dir, "packaging", "tools", "tdengine.iss")
    
    if tdCustomer.Name == "TDengine":
        logging.info("packaging TDengine server...")
        write_TDengine_server_install_file()
        os.system(f"iscc /DMyAppInstallName=\"{install_info.packagServerName}\" \
                  /DMyAppVersion=\"{td_version.version}\" \
                  /DMyAppExcludeSource=\"\" \
                  /DCusName=\"{tdCustomer.Name}\" \
                  /DCusPrompt=\"{tdCustomer.Prompt}\" \
                  {iss_path} /O..\\release")
    else:
        logging.info(f"packaging {install_info.packagServerName} server...")
        write_server_install_file()
        os.system(f"iscc /DMyAppInstallName=\"{install_info.packagServerName}\" \
                  /DMyAppVersion=\"{td_version.version}\" \
                  /DMyAppExcludeSource=\"\" \
                  /DCusName=\"{tdCustomer.Name}\" \
                  /DCusPrompt=\"{tdCustomer.Prompt}\" \
                  {iss_path} /O..\\release")
        logging.info(f"packaging {install_info.packagServerName} server done")
    # to do remove something
    # if os.system("echo %errorlevel%") != 0:
    #     logging.error(f"package {install_info.packagServerName} failed")
    #     raise Exception(f"package {install_info.packagServerName} failed")

def process_package_client():
    iss_path = os.path.join(install_info.community_dir, "packaging", "tools", "tdengine.iss")
    if tdCustomer.Name == "TDengine":
        write_TDengine_client_install_file()
        os.system(f"iscc /DMyAppInstallName=\"{install_info.packagClientName}\" \
                  /DMyAppVersion=\"{td_version.version}\" \
                  /DMyAppExcludeSource=\"taosd.exe,tmq*.exe,tsim.exe\" \
                  /DCusName=\"{tdCustomer.Name}\" \
                  /DCusPrompt=\"{tdCustomer.Prompt}\" \
                  {iss_path} /O..\\release")
    else:
        logging.info(f"packaging {install_info.packagClientName} server...")
        write_client_install_file()
        os.system(f"iscc /DMyAppInstallName=\"{install_info.packagClientName}\" \
                  /DMyAppVersion=\"{td_version.version}\" \
                  /DMyAppExcludeSource=\"taosd.exe,tmq*.exe,tsim.exe\" \
                  /DCusName=\"{tdCustomer.Name}\" \
                  /DCusPrompt=\"{tdCustomer.Prompt}\" \
                  {iss_path} /O..\\release")
        logging.info("packaging {install_info.packagClientName} server done")
    # if os.system("echo %errorlevel%") != 0:
    #     print(f"package {install_info.packagClientName} failed")
    #     exit(1)

def process_package():
    process_package_server()
    process_package_client()

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
        process_add_enterprice_extent()
    elif process == "package":
        logging.info("Calling package...")
        process_package()
    elif process == "packages":
        logging.info("Calling package server...")
        process_package_server()
    elif process == "packagec":
        logging.info("Calling package client...")
        process_package_client()
    else:
        logging.info("Invalid input. Please enter valid input.")

def set_win_dev_env():
    output = os.popen('vcvarsall.bat x64 && set').read()

    for line in output.splitlines():
        pair = line.split("=", 1)
        if(len(pair) >= 2):
            os.environ[pair[0]] = pair[1]

def os_check():
    if current_os != 'Windows':
        logging.info("Failed! This script only for windows!")
        sys.exit()
    else:
        set_win_dev_env()

if __name__ == "__main__":
    set_win_dev_env()
   # os_check()
    logging.info("Release tdengine on windows start...")
    parse_arguments()
    set_package_name()
    set_release_path()
    print_param()

    generate_community_dir()

    if test_process != '':
        testHanle(test_process)
        sys.exit()

    init_release_dir()
    
    process_cmake()
    process_build()
    process_install()
    if td_version.verType == "cluster":
        process_add_enterprice_extent()
    process_package()
 
