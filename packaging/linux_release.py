import os;
import logging
import shutil



script_dir = os.path.abspath(os.getcwd())
top_dir = os.path.abspath(os.path.join(script_dir, ".."))
release_dir = os.path.abspath(os.path.join(top_dir, "release"))
opc_dir = os.path.abspath(os.path.join(top_dir, "plugins","opc"))
mqtt_dir = os.path.abspath(os.path.join(top_dir, "plugins","mqtt"))
influxdb_dir = os.path.abspath(os.path.join(top_dir, "plugins","influxdb"))
explore_dir = os.path.abspath(os.path.join(top_dir, "..","explorer"))

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def release(release_info,build_info):
    
    logging.info("release_info: {0}".format(release_info.__dict__))
    
    init_release_dir(release_info)
    for info in build_info:
        logging.info("build_info: {0}".format(info.__dict__))
        if info.Name =='opc':
            build_and_install_opc_on_linux(release_info,info.VersionMode)
        if info.Name =='mqtt':
            build_and_install_mqtt_on_linux(release_info,info.VersionMode)
        if info.Name =='influxdb':
            build_and_install_influxdb_on_linux(info.VersionMode)
        if info.Name =='taosx':
            build_and_install_taosx_on_linux(info.VersionMode)
        if info.Name =='taosx-agent':
            build_and_install_taosx_agent_on_linux(info.VersionMode)
        if info.Name =='taos-explorer':
            install_taos_explorer_on_linux(info.VersionMode)
    make_tar_package(release_info)
    logging.info("release successfully")
            

def init_release_dir(release_info):
    logging.info("init_release_dir")
    global release_dir
    release_dir = os.path.join(top_dir,"release","taosX-{0}-Linux-x64".format(release_info.TaosXVersion))
    if os.path.exists(release_dir):
        logging.info("release_dir %s already exists" % release_dir)
    else:
        logging.info("release_dir %s does not exist, create it" % release_dir)
        os.system("mkdir -p %s" % release_dir)          
        
def build_and_install_opc_on_linux(release_info,mode='release'):
    logging.info("building taosx-opc")
    platform = "linux"
    arch = "amd64"
    verMode = "release"
    
    dst_dir = os.path.join(release_dir,"plugins","opc")
    binary_file = os.path.join(opc_dir,"taosx-opc")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)
        
    os.chdir(opc_dir)
    if mode.lower() == 'release':
        os.system(f"go build -ldflags "
                  f"\"-s -w -X 'collector/version.BuildAt={release_info.BuildTime}' "
                  f"-X 'collector/version.CommitID={release_info.Commit}'\" "
                  f"-o {binary_file}")
        logging.info("taox-opc built successfully")
    else:
        os.system(f"go build -ldflags "
                  f"\"-s -w -X 'collector/version.BuildAt={release_info.BuildTime}' "
                  f"-X 'collector/version.CommitID={release_info.Commit}'\" "
                  f"-o {binary_file}")
        logging.info("taox-opc built successfully")
    
    shutil.copyfile(binary_file,os.path.join(dst_dir,'taosx-opc'))
    logging.info("taox-opc copied to {release_dir}".format(release_dir=release_dir))
    
    os.chdir(script_dir)  
    
def build_and_install_mqtt_on_linux(release_info,mode='release'):
    logging.info("building taosx-mqtt")
    platform = "linux"
    arch = "amd64"
    verMode = "release"
    dst_dir = os.path.join(release_dir,"plugins","mqtt")
    binary_file = os.path.join(mqtt_dir,"dist","taosx-mqtt")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)
        
    os.chdir(mqtt_dir)
    if mode.lower() == 'release':
        build_command = f"go build -o {binary_file} -ldflags \"" \
                 f"-X github.com/taosdata/taosx/plugins/mqtt/version.Commit={release_info.Commit} " \
                 f"-X \'github.com/taosdata/taosx/plugins/mqtt/version.BuildTime={release_info.BuildTime}\'\""
    else:
        build_command = f"go build -o {binary_file} -ldflags \"" \
                 f"-X github.com/taosdata/taosx/plugins/mqtt/version.Commit={release_info.Commit} " \
                 f"-X \'github.com/taosdata/taosx/plugins/mqtt/version.BuildTime={release_info.BuildTime}\'\""
    
    logging.info(f"build_command: {build_command}")                 
    os.system(build_command)
    logging.info("taox-mqtt built successfully")
    
    shutil.copyfile(binary_file,os.path.join(dst_dir,'taosx-mqtt'))
    logging.info("taox-mqtt copied to {release_dir}".format(release_dir=dst_dir))
    
    os.chdir(script_dir)
    
def build_and_install_influxdb_on_linux(mode='release'):
    logging.info("build_and_install taosx-influxdb under linux")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"plugins","influxdb")
    binary_file = os.path.join(influxdb_dir,"target","taosx-influxdb.jar")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)
        
    os.chdir(influxdb_dir)
    if mode.lower() == 'release':
        build_command = "mvn clean package"
    else:
        # debug mode
        build_command = "mvn clean package"
    logging.info(f"build_command: {build_command}")                 
    os.system(build_command)
    logging.info("taox-influxdb built successfully")
    
    shutil.copyfile(binary_file,os.path.join(release_dir,"plugins","influxdb","taosx-influxdb.jar"))
    logging.info("taox-influxdb copied to {release_dir}".format(release_dir=dst_dir))
    
    os.chdir(script_dir)
    
def build_and_install_taosx_on_linux(mode='release'):
    logging.info("build_and_install taosx under linux...")

    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"bin")
    binary_file = os.path.join(top_dir,"target",mode.lower(),"taosx")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)     
    os.chdir(top_dir)
    
    if mode.lower() == 'release':
        os.system(f'cargo build --release --no-default-features --features optin --features rustls')
    else:
        os.system(f'cargo build --no-default-features --features optin --features rustls')
    logging.info("taox built successfully")
    
    shutil.copy(binary_file,dst_dir)
    logging.info("taox copied to {release_dir}".format(release_dir=dst_dir))
    
    if not os.path.exists(os.path.join(release_dir,"scripts")):
        os.system("mkdir -p %s" % os.path.join(release_dir,"scripts"))
    else:
        logging.info(f"{os.path.join(release_dir,'scripts')} already exists")
    
    shutil.copy(os.path.join(top_dir,"target","taosx.service"),os.path.join(release_dir,"scripts"))
    os.chdir(script_dir)

def install_taos_explorer_on_linux(mode='release'):
    logging.info("install taosx-explore under linux...")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"bin")
    binary_file = os.path.join(explore_dir,"target",mode.lower(),"taos-explorer")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)
        
    os.chdir(explore_dir)
    
    # if mode.lower() == 'release':
    #     os.system('yarn build:bin')
    # else:
    #     os.system('yarn build:debug')
        
    # logging.info("taos-explorer built successfully")
    
    shutil.copy(binary_file,dst_dir)
    logging.info("taox-agent copied to {release_dir}".format(release_dir=dst_dir))
    
    if not os.path.exists(os.path.join(release_dir,"scripts")):
        os.system("mkdir -p %s" % os.path.join(release_dir,"scripts"))
    else:
        logging.info(f"{os.path.join(release_dir,'scripts')} already exists")
    
    shutil.copyfile(os.path.join("target","taos-explorer.service"),os.path.join(release_dir,"scripts","taos-explorer.service"))
    
    if not os.path.exists(os.path.join(release_dir,"config")):
        os.system("mkdir -p %s" % os.path.join(release_dir,"config"))
    else:
        logging.info(f"{os.path.join(release_dir,'config')} already exists")
    shutil.copyfile(os.path.join("server","examples","explorer.toml"),os.path.join(release_dir,"config","explorer.toml"))

    os.chdir(script_dir)

def build_and_install_taosx_agent_on_linux(mode='release'):
    logging.info("build_and_install taosx-agent under linux...")
    platform = "linux"
    arch = "amd64"
    dst_dir = os.path.join(release_dir,"bin")
    binary_file = os.path.join(top_dir,"target",mode.lower(),"taosx-agent")
    if os.path.exists(dst_dir):
        logging.info(f"{dst_dir} already exists")
    else:
        logging.info(f"{dst_dir} does not exist, create it")
        os.system("mkdir -p %s" % dst_dir)
        
    os.chdir(top_dir)
    
    if mode.lower() == 'release':
        os.system('cargo build --release --package taosx-agent')
    else:
        os.system('cargo build --package taosx-agent')
        
    logging.info("taox-agent built successfully")
    
    shutil.copy(binary_file,dst_dir)
    logging.info("taox-agent copied to {release_dir}".format(release_dir=dst_dir))
    
    if not os.path.exists(os.path.join(release_dir,"scripts")):
        os.system("mkdir -p %s" % os.path.join(release_dir,"scripts"))
    else:
        logging.info(f"{os.path.join(release_dir,'scripts')} already exists")
    
    shutil.copyfile(os.path.join(top_dir,"target","taosx-agent.service"),os.path.join(release_dir,"scripts","taosx-agent.service"))
    
    if not os.path.exists(os.path.join(release_dir,"config")):
        os.system("mkdir -p %s" % os.path.join(release_dir,"config"))
    else:
        logging.info(f"{os.path.join(release_dir,'config')} already exists")
    shutil.copyfile(os.path.join(top_dir,"taosx-agent","examples","agent.toml"),os.path.join(release_dir,"config","agent.toml"))

    os.chdir(script_dir)


def make_tar_package(release_info):
    logging.info("making tar package")
    shutil.copy(os.path.join(script_dir,"rmtaosX.sh"),release_dir)
    shutil.copy(os.path.join(script_dir,"install.sh"),release_dir)
    
    os.chmod(os.path.join(release_dir,"rmtaosX.sh"),0o755)
    os.chmod(os.path.join(release_dir,"install.sh"),0o755)
    os.chmod(os.path.join(release_dir,"bin"),0o755)
    os.chmod(os.path.join(release_dir,"plugins"),0o755)
    
    os.chdir(os.path.join(release_dir,".."))
    code = os.system("tar -czvf taosX-{0}-Linux-x64.tar.gz $(basename {1}) --remove-files".format(release_info.TaosXVersion,release_dir))
    if code != 0:
        raise Exception("packaging {0} failed".format(release_info.TaosXVersion))
    else:    
        logging.info("packaging {0} successfully".format(release_info.TaosXVersion))
        os.chdir(script_dir) 
