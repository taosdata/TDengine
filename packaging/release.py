import argparse
import sys
import shutil
import platform
import os
import subprocess
import toml

cus_name = "TDengine"
taosx_agent_name = "taosx-agent"
connector_array = []
version = ''
install_path = ''
packagServerName = ''
current_os = platform.system()
script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)
release_path = os.path.join(script_dir, "..", "release")
taosx_dir = os.path.abspath(os.path.join(script_dir, ".."))

test_process = ''

def getTaosxVersion():
    global version
    cargo_toml_path = os.path.join(taosx_dir, "Cargo.toml")
    print("cargo_toml_path:", cargo_toml_path)
    with open(cargo_toml_path, 'r') as f:
        cargo_toml = toml.load(f)
        version = cargo_toml['package']['version']

def getTaosxAgentOutputName():
    if current_os == 'Windows':  # Windows操作系统
        return taosx_agent_name + ".exe"
    else:
        return taosx_agent_name

def setDefaultParam():
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

def readArgs():
    global connector_array, packagServerName, test_process
    getTaosxVersion()
    parser = argparse.ArgumentParser()

    # 添加 -c 参数，connector集合
    parser.add_argument('-c', '--connector_array', nargs='+', help='An array of strings')
    parser.add_argument('-t', '--test_process', help='test single process(pi,opc,mqtt,taosx, package)')

    args, unknown_args = parser.parse_known_args()
    if args.connector_array:
        connector_array = args.connector_array
    if args.test_process:
        test_process = args.test_process
    if unknown_args:
        print(f"Unknown args: {unknown_args}")

    result = "".join([f"-{elem}" for elem in connector_array])

    if current_os == 'Windows':
        packagServerName = f'{taosx_agent_name}-v{version}{result}-installer'
    elif current_os == 'Darwin':
        packagServerName = f'{taosx_agent_name}-v{version}{result}-Mac-installer'
    elif current_os == 'Linux':
        packagServerName = f'{taosx_agent_name}-v{version}{result}-Linux-installer'
    else:
        print('Unknown operating system:', current_os)
        sys.exit()   


def printParam():      
    print('PARAM connector_array', connector_array)
    print('PARAM version:', version)
    print('PARAM install_path:', install_path)
    print('PARAM release_path:', release_path)
    print('PARAM packagServerName:', packagServerName)
    print('')

def buildAndInstallOPCOnWindows():
    print("buildAndInstallOPC on windows start...")
    opc_connector_path = os.path.join(taosx_dir, "plugins", "opc")
    os.chdir(opc_connector_path)

    # 32位编译
    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "386"
    opc_app_name = "opc-collector_windows_386.exe"
    os.system(f"go build -o dist/{opc_app_name}")
    
    opc_install_path = os.path.join(install_path, "xplugins", "opc")
    initDirectory(opc_install_path)
    opc_path = os.path.join(opc_connector_path, "dist", opc_app_name)
    try:
        shutil.copy2(opc_path, opc_install_path)
    except FileNotFoundError as e:
        print("Build OPC failed: ", e.strerror)
        sys.exit()

def buildAndInstallMQTTOnWindows():
    print("buildAndInstallMQTT on windows start...")
    mqtt_connector_path = os.path.join(taosx_dir, "plugins", "mqtt")
    os.chdir(mqtt_connector_path)

    os.environ["GOOS"] = "windows"
    os.environ["GOARCH"] = "amd64"
    mqtt_app_name = "taosmqtt.exe"
    os.system(f"go build -o dist/{mqtt_app_name}")

    mqtt_install_path = os.path.join(install_path, "xplugins", "mqtt")
    initDirectory(mqtt_install_path)
    mqtt_path = os.path.join(mqtt_connector_path, "dist", mqtt_app_name)
    try:
        shutil.copy2(mqtt_path, mqtt_install_path)
    except FileNotFoundError as e:
        print("Build MQTT failed: ", e.strerror)
        sys.exit()

def buildAndInstallOPC():
    if current_os == 'Windows':
        buildAndInstallOPCOnWindows()
    else:
        print('buildAndInstallOPC not supported on operating system:', current_os)
        sys.exit()

def buildAndInstallMQTT():
    if current_os == 'Windows':
        buildAndInstallMQTTOnWindows()
    else:
        print('buildAndInstallMQTT not supported on operating system:', current_os)
        sys.exit()


def buildAndInstallPI():
    if current_os != 'Windows':
        print(" PI Connector is only compatible with the Windows operating system.")
        sys.exit()
    print("buildAndInstallPI start...")
    pi_connector_path = os.path.join(taosx_dir, "plugins", "pi", "src", "TDPIConnector")
    os.chdir(pi_connector_path)
    print("solution clean...")
    os.system('devenv TDPIConnector.sln /clean')
    print("solution build...")
    build_result = os.system('devenv TDPIConnector.sln /build Release')
    if build_result == 0:
        print("PI Connector Solution built successfully.")
    else:
        print("Error building PI Connector solution.")
        sys.exit()

    pi_install_path = os.path.join(install_path, "xplugins", "pi")
    initDirectory(pi_install_path)

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

def copyTaosAgentServiceFile(taosx_install_path):
    taosx_agent_path = os.path.join(taosx_dir, "taosx-agent", "bin")
    for filename in os.listdir(taosx_agent_path):
        if filename.startswith("taosx-agent-srv"):
            filepath = os.path.join(taosx_agent_path, filename)
            if os.path.isfile(filepath):
                shutil.copy2(filepath, taosx_install_path)

def copyTaosAgentCfg(taos_cfg_path):
    taosx_agent_cfg = os.path.join(taosx_dir, "taosx-agent", "examples", "agent.example.toml")
    try:
        shutil.copy2(taosx_agent_cfg, taos_cfg_path)
    except FileNotFoundError as e:
        print("Copy TaosX Agent cfg from {} to {} failed: {}".format(taosx_agent_cfg, taos_cfg_path,  e.strerror))
        sys.exit()

def buildAndInstallTaosX():
    print("buildAndInstallTaosX Agent start...")
    os.chdir(taosx_dir)
    os.system('cargo build --release --package taosx-agent')

    taox_install_path = os.path.join(install_path, "bin")
    checkDirectory(taox_install_path)
    taosx_agent_path = os.path.join(taosx_dir, "target", "release", getTaosxAgentOutputName())
    try:
        shutil.copy2(taosx_agent_path, taox_install_path)
    except FileNotFoundError as e:
        print("Copy TaosX to {} failed: {}".format(taosx_agent_path,  e.strerror))
        sys.exit()

    copyTaosAgentServiceFile(taox_install_path)
    taos_cfg_path = os.path.join(install_path, "cfg")
    copyTaosAgentCfg(taos_cfg_path)
   
def packageOnWindows():
    os.chdir(script_dir)
    result = subprocess.run(f'iscc /F"{packagServerName}" /DMyAppVersion="{version}" '
                            f'/DMyAppSourceDir="{install_path}" '
                            f'/DCusName="{cus_name}" '
                            f'/DTaosXAgentName="{taosx_agent_name}" '
                            f'{script_dir}/taosx.iss /O{taosx_dir}/release', shell=True)
    if result.returncode != 0:
        print(f'package {packagServerName} failed')
        sys.exit(1)

def package():
    if current_os == 'Windows':
        packageOnWindows()
    else:
        print('packaging not supported on operating system:', current_os)
        sys.exit()      

   
def initDirectory(path):
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
 
def checkDirectory(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as e:
        print('Error:', e)
        sys.exit()
        
def initInstallDirectory():
    print("initInstallDirectory {}...".format(install_path))
    checkDirectory(install_path)
    checkDirectory(os.path.join(install_path, "cfg"))

    opc_install_path = os.path.join(install_path, "xplugins", "opc")
    initDirectory(opc_install_path)

    pi_install_path = os.path.join(install_path, "xplugins", "pi")
    initDirectory(pi_install_path)

    mqtt_install_path = os.path.join(install_path, "xplugins", "mqtt")
    initDirectory(mqtt_install_path)


def initReleaseDirectory():
    print("initReleaseDirectory {}...".format(release_path))
    checkDirectory(release_path)

def testHanle(process):
    if process == "pi":
        print("Calling PI function...")
        buildAndInstallPI()
    elif process == "opc":
        print("Calling OPC function...")
        buildAndInstallOPC()
    elif process == 'mqtt':
        print("Calling MQTT function...")
        buildAndInstallMQTT()
    elif process == "package":
        print("Calling Package function...")
        package()
    elif process == "taosx":
        print("Calling Taosx function...")
        buildAndInstallTaosX()
    else:
        print("Invalid input. Please enter valid input.")
  
if __name__ == '__main__':
    setDefaultParam()
    readArgs()
    printParam()
    if test_process != '':
        testHanle(test_process)
        sys.exit()
    initInstallDirectory()

    buildAndInstallTaosX()
    if "pi" in connector_array:
        buildAndInstallPI()
    if "opc" in connector_array:
        buildAndInstallOPC()
    if "mqtt" in connector_array:
        buildAndInstallMQTT()
        
    initReleaseDirectory()
    package()
