import yaml
import os
import logging
import subprocess
import platform
import shutil
import re
import time
import argparse
import mkpkg
import multiprocessing

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s ',
                    level=logging.INFO,filename='draftlog.txt')
# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# formatter = logging.Formatter('%(levelname)-8s %(message)s')
# console.setFormatter(formatter)
# logging.getLogger('').addHandler(console)

def setLogLevel(level):
    if level == 'debug':
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

def getGitHead(gitPath, repo):
    os.chdir(gitPath)
    logging.info(f"gitPath:{gitPath}")
    code, output = subprocess.getstatusoutput('git rev-parse --verify HEAD')
    if code == 0:
        return output
    else:
        raise Exception(
            "get repo {0} git info failed, reason {1}".format(repo, output))

cpuCount = multiprocessing.cpu_count()
# TDinternal/enterprise/packaging
scriptDir = os.path.abspath('.')

# TDinternal
topDir = os.path.join(scriptDir, '..', '..')

# TDinternal/community
communityDir = os.path.join(topDir, 'community')

# TDinternal/enterprise
enterpriseDir = os.path.join(topDir, 'enterprise')

# TDinternal/community/debug
compileDir = os.path.join(communityDir,'debug')

buildTime = time.strftime("%Y-%m-%d %H:%M", time.localtime())

internalGitInfo = getGitHead(topDir, "TDinternal")
communityGitInfo = getGitHead(communityDir, "Community(TDengine)")


def readOption(filecontent,option):
    options = yamlfile[option]
    logging.debug(options)
    return options

def getTaosToolVersion():
    code, output = subprocess.getstatusoutput(
        'git tag |grep -v taos | sort | tail -1')
    if code == 0:
        return output
    else:
        raise Exception(
            "get taosTools's version failed, reason {1}".format(output))

def executeCommand(command):
    logging.info(">>>>>>>>>>> {0}".format(command))
    ret = subprocess.run(command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,shell=True, encoding='utf-8')
    logging.info(ret.stdout)
    ret.check_returncode()
    logging.info(">>>>>>>>>>> {0} done".format(command))

def executeExpensiveCommand(command):
    logging.info(">>>>>>>>>>> {0}".format(command))
    ret = subprocess.run(command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
    logging.info(ret.stdout)
    logging.info(">>>>>>>>>>>")
    ret.check_returncode()
    
def validVersion(version,comVersion):
    # version = options['buildOptions']['VERNUMBER']
    # comVersion = options['buildOptions']['VERCOMPATIBLE']
    if len(version) == 0:
        logging.error('invalid empty version number')
        raise Exception('invalid empty version number')
    if len(comVersion) == 0:
        logging.error('invalid empty compatible verion number')
        raise Exception('invalid empty compatible version number')
    verReg = re.search('^([0-9]+\.){3,4}(\*|[0-9]+)$', version)
    verComReg = re.search('^([0-9]+\.){3,4}(\*|[0-9]+)$', comVersion)
 
    if not verComReg or not verReg or checkVersionCompatible(version, comVersion) == 2:
        logging.error("invalid version number :{0} or version uncompatible with {1}".format(
            version, comVersion))
        raise Exception("invalid version number :{0} or version uncompatible with {1}".format(
            version, comVersion))
    else:
        logging.info("release version number:{0} , compatible version number:{1}".format(
            version, comVersion))

def initOEMParameter(options):
    if len(options['OEMFLAG']['CUS_NAME'])>0:
        options['buildOptions']['CUS_NAME'] = options['OEMFLAG']['CUS_NAME']
    else:
        logging.error(f"OEM cust name can't be an empty string")
        raise Exception("OEM cust name can't be an empty string")
    
    if len(options['OEMFLAG']['CUS_PROMPT'])>0:
        options['buildOptions']['CUS_PROMPT'] = options['OEMFLAG']['CUS_PROMPT']
    else:
        logging.error(f"OEM cust promt can't be an empty string")
        raise Exception("OEM cust promt can't be an empty string")
    
    if len(options['OEMFLAG']['CUS_EMAIL'])>0:
        options['buildOptions']['CUS_EMAIL'] = options['OEMFLAG']['CUS_EMAIL']
    else:
        logging.error("OEM cust email can't be an empty string")
        raise Exception("OEM cust email can't be an empty string")
    
    logging.info(f"OEM Rlease info custName:{options['OEMFLAG']['CUS_NAME']} custPromt:{options['OEMFLAG']['CUS_PROMPT']} custEmail:{options['OEMFLAG']['CUS_EMAIL']}")

def checkAndInitInput(options,version):
    logging.info("checking input paramerters")
    currentOS = platform.system()

    # check version
    if version is not None and version != options['buildOptions']['VERNUMBER']:
        logging.warning(f"CLI input version:{version} not euqal to buildOption[VERNUMBER]:{options['buildOptions']['VERNUMBER']},will overwrite buildOption[VERNUMBER]")
        options['buildOptions']['VERNUMBER'] = version
        logging.info(f"current release version:{options['buildOptions']['VERNUMBER']}")
    else:
        logging.info(f"current release version:{options['buildOptions']['VERNUMBER']}")
    
    # check version valid
    validVersion(options['buildOptions']['VERNUMBER'],options['buildOptions']['VERCOMPATIBLE'])
        
    # check OS
    if currentOS != options['buildOptions']['OSTYPE']:
        logging.warning(
            "current os is {0} but expect {1}".format(currentOS, options['buildOptions']['OSTYPE']))
    else:
        logging.info("currentOS:{0}, targetOS:{1}".format(currentOS, options['buildOptions']['OSTYPE']))

    # check verMode
    if options['verMode'] not in ['edge', 'cluster', 'cloud']:
        logging.error(
            f"verMode only accept ['edge','cluster','cloud'],unsupport vermode:{options['verMode']}")
        raise Exception(
            f"verMode only accept ['edge','cluster','cloud'],unsupport vermode:{options['verMode']}")
    else:
        logging.info(f"verMode:{options['verMode']}")
        
    if options['verMode'] == 'cluster' and options['OEMFLAG']['enable'] == True:
        logging.info(f"This is a OEM release, initing OEM parameters...")
        initOEMParameter(options)
    
    # check pagMode
    logging.debug(f"{options}")
    if options['pagMode'] not in ['full', 'lite']:
        logging.error(
            f"pagMode only accept ['full', 'lite'],unsupport pagMode:{options['pagMode']}")
        raise Exception(
             f"pagMode only accept ['full', 'lite'],unsupport pagMode:{options['pagMode']}")
    elif options['pagMode']!=options['buildOptions']['PAGMODE']:
        logging.warning(f"pageMode on the top: {options['pagMode']} is not same as build option PAGMODE:{options['buildOptions']['PAGMODE']}, PAGMODE will be overrWritted")
        options['buildOptions']['PAGMODE'] = options['pagMode']
        logging.info(f"current BuilOptions of PAGMODE is {options['buildOptions']['PAGMODE']}")
    else:
        logging.info(f"pagMode:{options['pagMode']}")

# this may be removable.
def generateLatestDocker():
    if options['dockerMode'] == 'latest' and options['verMode'] == 'cluster':
        logging.info("generate_docker_enterprise.sh will be running...")
    elif options['dockerMode'] == 'latest':
        logging.info("generate_docker.sh will be running...")


def generateEnterpriseDocker():
    shutil.copy('test.txt', os.path.join(scriptDir, 'docker'))
    logging.info("generate_docker_enterprise.sh will be running.")


def generateCommunityDocker():
    logging.info("generate_docker.sh will be running...")


def checkVersionCompatible(ver1, ver2):

    if ver1 == ver2:
        return 0
    ver1 = list(map(int, ver1.split('.')))
    ver2 = list(map(int, ver2.split('.')))

    # fill empty fields in ver1 with zeros
    for i in range(len(ver1), len(ver2)):
        ver1.append(0)

    for i in range(len(ver1)):
        # fill empty fields in ver2 with zeros
        if i >= len(ver2):
            ver2.append(0)
        if ver1[i] > ver2[i]:
            return 1
        elif ver1[i] < ver2[i]:
            return 2


def prepareDir():
    packageDir = os.path.join(communityDir, 'release')
    rpmsDir = os.path.join(communityDir, 'rpms')
    debsDir = os.path.join(communityDir, 'debs')
    logging.info("Prepare the release directories.")
    
    # check dir clear path
    
    if os.path.exists(communityDir):
        os.chdir(communityDir)
        if not os.path.exists(compileDir):
            logging.info("mkdir {0}".format(compileDir))
            os.mkdir(compileDir)
    else:
        raise Exception("community dir not found")

    # mkdir release
    if os.path.exists(packageDir):
        shutil.rmtree(packageDir)
        os.mkdir(packageDir)
    else:
        os.mkdir(packageDir)
    logging.info(f"make release dir:{packageDir}")
    
     # mkdir for rmp package
    if options['TDenginePackages']['rpm']['enable'] == True:
        logging.info(f"make rpm release dir:{rpmsDir}")
        if os.path.exists(rpmsDir):
            shutil.rmtree(rpmsDir)
            logging.info("remove exist rpm dir")
            os.mkdir(rpmsDir)
            logging.info("mkdir {0}".format(rpmsDir))
        else:
            logging.info("mkdir dir:{0}".format(rpmsDir))
            os.mkdir(rpmsDir)

    # mkdir for deb package
    if options['TDenginePackages']['rpm']['enable'] == True:
        logging.info(f"make deb release dir:{debsDir}")
        
        if os.path.exists(debsDir):
            shutil.rmtree(debsDir)
            os.mkdir(debsDir)
        else:
            os.mkdir(debsDir)

def generateCmakeCommand(buildOptions,verMode)->str:
    command = ''
    if verMode == 'edge':
        command = 'cmake ../'
    elif verMode == 'cloud':
        command = 'cmake ../../'
    elif verMode == 'cluster':
        command = 'cmake ../../'
    else:
        logging.error("verMode is not supported")
    # 1
    if buildOptions['CPUTYPE'] in ['x64','arm64','mips64']:
        command += f" -DCPUTYPE={buildOptions['CPUTYPE']} "
    else:
        logging.error(f"{buildOptions['CPUTYPE']} is not supported")
        raise Exception(f"{buildOptions['CPUTYPE']} is not supported")
    # 2    
    if buildOptions['OSTYPE'] in ['Linux','Darwin','Windows']:
        command += f" -DOSTYPE={buildOptions['OSTYPE']} "
    else:
        logging.error(f"{buildOptions['OSTYPE']} is not supported")
        raise Exception(f"{buildOptions['OSTYPE']} is not supported")
    # 3
    if buildOptions['WEBSOCKET'] == True:
        command += f" -DWEBSOCKET=true "
    else:
        command += f" -DWEBSOCKET=false "
        logging.info("WEBSOCKET is false, libtaows will not be built")
    # 4
    if buildOptions['SOMODE'] in ['dynamic','static']:
        command += f" -DSOMODE={buildOptions['SOMODE']} "
    else:
        logging.error(f"{buildOptions['SOMODE']} is not supported")
    # 5
    if len(buildOptions['DBNAME']) > 0 :
        command += f" -DDBNAME={buildOptions['DBNAME']} "
    else:
        logging.error("DBNAME is empty")
    # 6
    if  buildOptions['VERTYPE']  in ['stable','beta']:
        command += f" -DVERTYPE={buildOptions['VERTYPE']} "
    else:
        logging.error(f"{buildOptions['VERTYPE']} is not supported")
    
    # 7
    command += f" -DVERDATE='{buildTime}' "
    
    # 8
    command += f" -DGITINFO={communityGitInfo} "
    
    # 9
    command += f" -DGITINFOI={internalGitInfo} "
    
    # 10
    if len(buildOptions['VERNUMBER']) > 0:
        command += f" -DVERNUMBER={buildOptions['VERNUMBER']} "
    else:
        raise Exception("VERNUMBER is empty")

    # 11
    if len(buildOptions['VERCOMPATIBLE']) > 0:
        command += f" -DVERCOMPATIBLE={buildOptions['VERCOMPATIBLE']} "
    else:
        raise Exception("VERCOMPATIBLE is empty")
    # 12
    if buildOptions['BUILD_HTTP']==True:
        command += f" -DBUILD_HTTP=true "
    else:
        command += f" -DBUILD_HTTP=false "
        logging.info("BUILD_HTTP is false, taosAdapter will not be built")
    
    # 13
    if buildOptions['BUILD_TOOLS']==True:
        command += f" -DBUILD_TOOLS=true "
    else:
        command += f" -DBUILD_TOOLS=false "
        logging.INFO("BUILD_TOOLS is false, tools will not be built")
    
    # 14
    if buildOptions['JEMALLOC_ENABLED']==True:
        if f"{buildOptions['OSTYPE']}-{buildOptions['CPUTYPE']}" == 'Linux-x64':
            command += f" -DJEMALLOC_ENABLED=true "
        else:
            logging.warning("jemalloc is not supported on this platform")
    else:
        command += f" -DJEMALLOC_ENABLED=false "
        logging.INFO("JEMALLOC_ENABLED is false, jemalloc will not be built")
    
    # 20
    if buildOptions['BUILD_EXPLORER'] == False:
        command += f" -DBUILD_EXPLORER=false "
        logging.info("BUILD_EXPLORER is false, explorer will not be built")
    elif verMode in ['cluster','cloud'] and buildOptions['BUILD_EXPLORER'] == False:
        command += f" -DBUILD_EXPLORER=false "
        logging.warning("BUILD_EXPLORER is true, but verMode is not cluster or cloud")
    elif verMode in ['cluster','cloud','cluster'] and buildOptions['BUILD_EXPLORER'] == True:
        command += f" -DBUILD_EXPLORER=true "
    else:
        logging.error("BUILD_EXPLORER is not supported for this verMode:{}".format(verMode))
        raise Exception("BUILD_EXPLORER is not supported for this verMode:{}".format(verMode))    
        
    # 15
    if buildOptions['BUILD_TAOSX'] == True :
        if verMode in ['cluster','cloud']:
            command += f" -DBUILD_TAOSX=true "
        else:
            command += f" -DBUILD_TAOSX=true "
            logging.warning("will build taosx in community mode")
    else:
        command += f" -DBUILD_TAOSX=false "
        logging.info("BUILD_TAOSX is false, taosx will not be built")
    
    # 16
    if buildOptions['BUILD_CLOUD'] == True:
        if verMode == 'cloud':
            command += f" -DBUILD_CLOUD=true "
        else:
            logging.error("BUILD_CLOUD is true, cloud release")
    else:
        command += f" -DBUILD_CLOUD=false "
        logging.info("BUILD_CLOUD is false, not for cloud release.")
    

    if verMode == 'cluster':
    # 17
        command += f" -DCUS_NAME={buildOptions['CUS_NAME']} "
    # 18
        command += f" -DCUS_PROMPT={buildOptions['CUS_PROMPT']} "    
    # 19
        command += f" -DCUS_EMAIL={buildOptions['CUS_EMAIL']} "    
    logging.info(f"cmake command is {command}")
    return command

def buildInstall(buildOptions,verMode):
    prepareDir()
    os.chdir(compileDir)
    logging.info(f"{buildOptions['OSTYPE']}-{buildOptions['CPUTYPE']} of {'community' if options['verMode']=='edge' else 'enterprise'} will be build.")
    logging.info("compile Dir:{0}".format(os))
    command = generateCmakeCommand(buildOptions,verMode)
    executeCommand(command)

    logging.info(f"cmake -j {cpuCount} ")
    executeCommand(f"make -j {cpuCount}")
    
    logging.info(f"making install")
    executeCommand(f"make install")
    os.chdir(scriptDir)

def makeTDenginePackages(packages,buildOptions,verMode):
    # pack TDengine.tar.gz
    if packages['tar.gz']['enable'] == True:
        
        if packages['tar.gz']['server']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }-tar.gz")
            mkpkg.mkServerTar(packages['tar.gz'],options['buildOptions'],options['verMode'])
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }-tar.gz")

        if packages['tar.gz']['client']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }-tar.gz")
            mkpkg.makeTarClient(packages['tar.gz'],options['buildOptions'],options['verMode'])
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }-tar.gz")    
    else:
        logging.warning(f"Skip packing TDengine-{options['verMode']}-{options['buildOptions']['VERNUMBER'] } tar.gz packages")
    
    logging.debug(packages)
    
    # pack TDengine.rpm
    if packages['rpm']['enable'] == True:
        
        if packages['rpm']['server']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }.rpm")
            os.chdir(os.path.join(communityDir,"packaging",'deb'))
            mkpkg.makeServeRpm(buildOptions,verMode)
            os.chdir(scriptDir)            
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }.rpm")
        
        if packages['rpm']['client']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }.rpm")
            os.chdir(os.path.join(communityDir,"packaging",'rpm'))
            mkpkg.makeClientRpm(buildOptions)
            os.chdir(scriptDir)
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }.rpm")    
    else:
        logging.warning(f"Skip packing TDengine-{options['verMode']}-{options['buildOptions']['VERNUMBER'] } rpm packages")
    
    # pack TDengine.deb
    if packages['deb']['enable'] == True:
        
        if packages['deb']['server']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }.deb")
            os.chdir(os.path.join(communityDir,"packaging",'deb'))
            mkpkg.makeServerDeb(buildOptions,verMode)
            os.chdir(scriptDir)
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-server-{options['buildOptions']['VERNUMBER'] }.deb")
        
        if packages['deb']['client']['enable'] == True:
            logging.info(f"making TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }.deb")
            os.chdir(os.path.join(communityDir,"packaging",'deb'))
            mkpkg.makeClientDeb(buildOptions)
            os.chdir(scriptDir)
        else:
            logging.warning(f"Skip packing TDengine-{options['verMode']}-client-{options['buildOptions']['VERNUMBER'] }.deb")    
    else:
        logging.warning(f"Skip packing TDengine-{options['verMode']}-{options['buildOptions']['VERNUMBER'] } deb packages")
    
def makeTaosToolsPackages(taosToolsPackages,buildOptions,verMode):
    # pack taosTools.tar.gz
    if taosToolsPackages['enable'] == True:
        if taosToolsPackages['tar.gz']['enable'] == True:
            logging.info(f"making taosToolstar.gz")
            mkpkg.makeTaosToolsTar(taosToolsPackages['tar.gz'],options['buildOptions'],options['verMode'])
        else:
            logging.warning(f"Skip packing taosTools.tar.gz")
        
        if taosToolsPackages['rpm']['enable'] == True:
            logging.info(f"making taosTools-{getTaosToolVersion()}.rpm")
            mkpkg.makeTaosToolsRpm(buildOptions,verMode)
        else:
            logging.warning(f"Skip packing taosTools-{getTaosToolVersion()}.rpm")
        
        if taosToolsPackages['deb']['enable'] == True:
            logging.info(f"making taosTools-{getTaosToolVersion()}.deb")
            mkpkg.makeTaosToolsDeb(buildOptions,verMode)
    else:
        logging.info("Skip making taosTools packages")
        
def makePackages(options,buildOptions,verMode):
    # =====================for TDengine =========================
    if options['TDenginePackages']['enable'] == True:
        logging.info("making TDengine packages like TDengine-server or TDengine-client") 
        makeTDenginePackages(options['TDenginePackages'],buildOptions,verMode)
    else:
        logging.warning("Skip making TDengine packages")
    
    # ========= for taos tools ==========
    if options['taosToolsPackages']['enable'] == True:
        makeTaosToolsPackages(options['taosToolsPackages'],buildOptions,verMode)
    else:
        logging.warning("Skip making taosTools packages")
    
    logging.info("will do post acotions.")
    logging.info("Release done")


def doActions(parePareOptions):
    actions = parePareOptions['actions']
    os.chdir(scriptDir)
    for action in actions:
        logging.debug('action info:{0}'.format(action))
        keys = action.keys()
        for key in keys:
            values = action[key]
            logging.info('action name:{0} command:{1} will be executed'.format(key,' '.join(values)))
            executeCommand(' '.join(values))
            
if __name__ == "__main__":
    parse = argparse.ArgumentParser(description='manual to this script')
    parse.add_argument('--file', type=str, default='enterprise.yaml',help='Config file name,default enterprise.yaml',required=True)
    parse.add_argument('--version', type=str, default=None,help='Release version',required=False)
    parse.add_argument('--vermode', type=str, default='cluster',help='Release version mode,edge or cluster',required=False)
    parse.add_argument('--loglevel', type=str, default='info',required=False)

    args = parse.parse_args()
    
    setLogLevel(args.loglevel)
    
    logging.info(
        f"base on config yaml {args.file} release for version:{args.version}")

    with open(os.path.join(scriptDir, args.file), 'r') as file:
        yamlfile = yaml.safe_load(file)
    options = readOption(yamlfile,'parameters')

    targets = []
    if options['TDenginePackages']['tar.gz']['enable'] == True:
        targets.append('tar.gz')
    if options['TDenginePackages']['rpm']['enable'] == True:
        targets.append('rpm')
    if options['TDenginePackages']['tar.gz']['enable'] == True:
        targets.append('deb')

    logging.info("{0}-{1}-{2} packages [{4}] version {3} will be released".format(
        options['verMode'],
        options['buildOptions']['OSTYPE'],
        options['buildOptions']['CPUTYPE'],
        options['buildOptions']['VERNUMBER'],
        ','.join(targets))
    )

    checkAndInitInput(options,args.version)
    
    # ================do parepare acotions 
    if  yamlfile['prepareActions']['enable'] == True:
        logging.info("will do parepare acotions.")
        doActions(readOption(yamlfile,'prepareActions'))
        logging.info("parepare acotions done.")
    else:
        logging.info("No post acotions.")    

    # build && install and packaging
    
    # ================= build and install 
    
    buildInstall(options['buildOptions'],options['verMode'])
    
    # ================ making packages 
    makePackages(options,options['buildOptions'],options['verMode'])
    
    # ================do post acotions
    if  yamlfile['postActions']['enable'] == True:
        logging.info("will do post acotions.")
        doActions(readOption(yamlfile,'postActions'))
        logging.info("post acotions done.")
    else:
        logging.info("No post acotions.")
    
    

