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
import sys


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
compileDir = os.path.join(communityDir, 'debug')

buildTime = time.strftime("%Y-%m-%d %H:%M", time.localtime())

# internalGitInfo = getGitHead(topDir, "TDinternal")
# communityGitInfo = getGitHead(communityDir, "Community(TDengine)")

logfile = f'release_py_{buildTime}.txt'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s ',
                    level=logging.INFO, filename=logfile)

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
        os.chdir(scriptDir)
        return output
    else:
        raise Exception(
            "get repo {0} git info failed, reason {1}".format(repo, output))


def readOption(fileContent, option):
    options = fileContent[option]
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
                         stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
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


def validVersion(version, comVersion):
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
    if len(options['OEMFLAG']['CUS_NAME']) > 0:
        options['buildOptions']['CUS_NAME'] = options['OEMFLAG']['CUS_NAME']
    else:
        logging.error(f"OEM cust name can't be an empty string")
        raise Exception("OEM cust name can't be an empty string")

    if len(options['OEMFLAG']['CUS_PROMPT']) > 0:
        options['buildOptions']['CUS_PROMPT'] = options['OEMFLAG']['CUS_PROMPT']
    else:
        logging.error(f"OEM cust promt can't be an empty string")
        raise Exception("OEM cust promt can't be an empty string")

    if len(options['OEMFLAG']['CUS_EMAIL']) > 0:
        options['buildOptions']['CUS_EMAIL'] = options['OEMFLAG']['CUS_EMAIL']
    else:
        logging.error("OEM cust email can't be an empty string")
        raise Exception("OEM cust email can't be an empty string")

    logging.info(
        f"OEM Rlease info custName:{options['OEMFLAG']['CUS_NAME']} custPromt:{options['OEMFLAG']['CUS_PROMPT']} custEmail:{options['OEMFLAG']['CUS_EMAIL']}")


def checkAndInitInput(options, version):
    logging.info("checking input paramerters")
    currentOS = platform.system()

    # check version
    if version is not None and version != options['buildOptions']['VERNUMBER']:
        logging.warning(
            f"CLI input version:{version} not euqal to buildOption[VERNUMBER]:{options['buildOptions']['VERNUMBER']},will overwrite buildOption[VERNUMBER]")
        options['buildOptions']['VERNUMBER'] = version
        logging.info(
            f"current release version:{options['buildOptions']['VERNUMBER']}")
    else:
        logging.info(
            f"current release version:{options['buildOptions']['VERNUMBER']}")

    # check version valid
    validVersion(options['buildOptions']['VERNUMBER'],
                 options['buildOptions']['VERCOMPATIBLE'])

    # check OS
    if currentOS != options['buildOptions']['OSTYPE']:
        logging.warning(
            "current os is {0} but expect {1}".format(currentOS, options['buildOptions']['OSTYPE']))
    else:
        logging.info("currentOS:{0}, targetOS:{1}".format(
            currentOS, options['buildOptions']['OSTYPE']))

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
    elif options['buildOptions'].get('PAGMODE') is not None and options['pagMode'] != options['buildOptions']['PAGMODE']:
        logging.warning(
            f"pageMode on the top: {options['pagMode']} is not same as build option PAGMODE:{options['buildOptions']['PAGMODE']}, PAGMODE will be overrWritted")
        options['buildOptions']['PAGMODE'] = options['pagMode']
        logging.info(
            f"current BuilOptions of PAGMODE is {options['buildOptions']['PAGMODE']}")
    else:
        logging.info(f"pagMode:{options['pagMode']}")

def generateLatestDocker(options):
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


def prepareDir(options):
    packageDir = os.path.join(communityDir, 'release')
    rpmsDir = os.path.join(communityDir, 'rpms')
    debsDir = os.path.join(communityDir, 'debs')
    logging.info("Prepare the release directories.")

    # check dir clear path
    if os.path.exists(communityDir):
        if not os.path.exists(compileDir):
            logging.info("mkdir {0}".format(compileDir))
            os.mkdir(compileDir)
        else:
            shutil.rmtree(compileDir)
            os.mkdir(compileDir)
            logging.info("clean exist compile dir")
    else:
        logging.error("community dir not found")
        raise Exception("community dir not found")

    # mkdir release
    if os.path.exists(packageDir):
        # shutil.rmtree(packageDir)
        # os.mkdir(packageDir)
        logging.info(f"{packageDir} exists")
    else:
        os.mkdir(packageDir)
    logging.info(f"make release dir:{packageDir}")

    # mkdir for rmp package
    if options['TDenginePackages'].get('rpm') is not None and options['TDenginePackages']['rpm']['enable'] == True:
        logging.info(f"make rpm release dir:{rpmsDir}")
        if os.path.exists(rpmsDir):
            # shutil.rmtree(rpmsDir)
            logging.info("remove exist rpm dir")
            # os.mkdir(rpmsDir)
            # logging.info("mkdir {0}".format(rpmsDir))
        else:
            logging.info("mkdir dir:{0}".format(rpmsDir))
            os.mkdir(rpmsDir)

    # mkdir for deb package
    if options['TDenginePackages'].get('deb') is not None and options['TDenginePackages']['deb']['enable'] == True:
        logging.info(f"make deb release dir:{debsDir}")

        if os.path.exists(debsDir):
            # shutil.rmtree(debsDir)
            # os.mkdir(debsDir)
            logging.info(f"{debsDir} exists")
        else:
            os.mkdir(debsDir)


def generateCmakeCommand(buildOptions, verMode) -> str:
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
    if buildOptions.get('CPUTYPE') is not None and buildOptions['CPUTYPE'] in ['x64', 'arm64', 'mips64']:
        command += f" -DCPUTYPE={buildOptions['CPUTYPE']} "
    else:
        logging.error("buildOptions['CPUTYPE'] is not supported or not set")
        raise Exception("buildOptions['CPUTYPE'] is not supported or not set")
    # 2
    if buildOptions.get('OSTYPE') is not None and buildOptions['OSTYPE'] in ['Linux', 'Darwin', 'Windows']:
        command += f" -DOSTYPE={buildOptions['OSTYPE']} "
    else:
        logging.error("buildOptions['OSTYPE'] is not supported or set")
        raise Exception("buildOptions['OSTYPE'] is not supported or set")
    # 3
    if buildOptions.get('WEBSOCKET') is not None and buildOptions['WEBSOCKET'] == True:
        command += f" -DWEBSOCKET=true "
    else:
        command += f" -DWEBSOCKET=false "
        logging.info("libtaows will not be built")
    # 4
    if buildOptions.get('SOMODE') is not None and buildOptions['SOMODE'] in ['dynamic', 'static']:
        command += f" -DSOMODE={buildOptions['SOMODE']} "
    else:
        logging.error("buildOptions['SOMODE'] is not supported or not set")
    # 5
    if buildOptions.get('DBNAME') is not None and len(buildOptions['DBNAME']) > 0:
        command += f" -DDBNAME={buildOptions['DBNAME']} "
    else:
        logging.error("buildOptions('DBNAME') is empty or not set")
        raise Exception("buildOptions('DBNAME') is empty or not set")
    # 6
    if buildOptions.get('DBNAME') is not None and buildOptions['VERTYPE'] in ['stable', 'beta']:
        command += f" -DVERTYPE={buildOptions['VERTYPE']} "
    else:
        logging.error("buildOptions['VERTYPE'] is not supported")

    # 7
    command += f" -DVERDATE='{buildTime}' "

    # 8
    communityGitInfo = getGitHead(communityDir, "Community(TDengine)")
    command += f" -DGITINFO={communityGitInfo} "

    # 9
    if verMode == 'cluster' or verMode == 'cloud':
        internalGitInfo = getGitHead(topDir, "TDinternal")
        command += f" -DGITINFOI={internalGitInfo} "
    else:
        command += f" -DGITINFOI=NULL "

    # 10
    if buildOptions.get('VERNUMBER') is not None and len(buildOptions['VERNUMBER']) > 0:
        command += f" -DVERNUMBER={buildOptions['VERNUMBER']} "
    else:
        raise Exception("buildOptions[VERNUMBER] is empty or not set")

    # 11
    if buildOptions.get('VERCOMPATIBLE') is not None and len(buildOptions['VERCOMPATIBLE']) > 0:
        command += f" -DVERCOMPATIBLE={buildOptions['VERCOMPATIBLE']} "
    else:
        raise Exception("buildOptions[VERCOMPATIBLE] is empty")
    # 12
    if buildOptions.get('BUILD_HTTP') is not None:
        if buildOptions['BUILD_HTTP'] == True:
            command += f" -DBUILD_HTTP=true "
            logging.info("BUILD_HTTP is true, taosAdapter will be built")
        elif buildOptions['BUILD_HTTP'] == False:
            command += f" -DBUILD_HTTP=false "
            logging.info("BUILD_HTTP is false, taosAdapter will not be built")
        else:
            command += f" -DBUILD_HTTP={buildOptions['BUILD_HTTP']} "
            logging.info(f"BUILD_HTTP is {buildOptions['BUILD_HTTP']}")
    else:
        logging.error("buildOptions['BUILD_HTTP'] is not set")
        raise Exception("buildOptions['BUILD_HTTP'] is not set")
    # 13
    if buildOptions.get('BUILD_TOOLS') is not None and buildOptions['BUILD_TOOLS'] == True:
        command += f" -DBUILD_TOOLS=true "
    else:
        command += f" -DBUILD_TOOLS=false "
        logging.info("taoTools will not be built")

    # 14
    if buildOptions.get('JEMALLOC_ENABLED') is not None and buildOptions['JEMALLOC_ENABLED'] == True:
        if buildOptions.get('PAGMODE') is not None:
            platform = f"{buildOptions['OSTYPE']}-{buildOptions['CPUTYPE']}-{buildOptions['PAGMODE']}"
        else:
            platform = f"{buildOptions['OSTYPE']}-{buildOptions['CPUTYPE']}-full"
        if platform == 'Linux-x64-full':
            command += f" -DJEMALLOC_ENABLED=true "
        else:
            logging.warning(f"jemalloc is not supported on this {platform}")
    else:
        logging.info("Jemalloc build is not enabled")

    # 15
    if buildOptions.get('BUILD_TAOSX') is not None:
        if buildOptions['BUILD_TAOSX'] == True:
            if verMode in ['cluster', 'cloud']:
                command += f" -DBUILD_TAOSX=true "
            else:
                command += f" -DBUILD_TAOSX=true "
                logging.warning("will build taosx in community mode")
        else:
            command += f" -DBUILD_TAOSX=false "
            logging.info("BUILD_TAOSX is false, taosx will not be built")
    else:
        logging.info("BUILD_TAOSX is not set, will not build taosx")

    # 16
    if buildOptions.get('BUILD_CLOUD') is not None:
        if buildOptions['BUILD_CLOUD'] == True:
            if verMode == 'cloud':
                command += f" -DBUILD_CLOUD=true "
            else:
                logging.error("BUILD_CLOUD is true, cloud release")
        else:
            command += f" -DBUILD_CLOUD=false "
            logging.info("BUILD_CLOUD is false, not for cloud release.")
    else:
        logging.info("BUILD_CLOUD is not set, will not build cloud release.")

    if verMode == 'cluster':
        # 17
        command += f" -DCUS_NAME={buildOptions['CUS_NAME']} "
    # 18
        command += f" -DCUS_PROMPT={buildOptions['CUS_PROMPT']} "
    # 19
        command += f" -DCUS_EMAIL={buildOptions['CUS_EMAIL']} "

    # 20
    if buildOptions.get('BUILD_EXPLORER') is not None:
        if buildOptions['BUILD_EXPLORER'] == False:
            command += f" -DBUILD_EXPLORER=false "
            logging.info("BUILD_EXPLORER is false, explorer will not be built")
        elif verMode in ['cluster', 'cloud'] and buildOptions['BUILD_EXPLORER'] == False:
            command += f" -DBUILD_EXPLORER=false "
            logging.warning(
                "BUILD_EXPLORER is true, but verMode is not cluster or cloud")
        elif verMode in ['cluster', 'cloud', 'cluster'] and buildOptions['BUILD_EXPLORER'] == True:
            command += f" -DBUILD_EXPLORER=true "
        else:
            logging.error(
                "BUILD_EXPLORER is not supported for this verMode:{}".format(verMode))
            raise Exception(
                "BUILD_EXPLORER is not supported for this verMode:{}".format(verMode))
    else:
        logging.info("BUILD_EXPLORER is not set, will not build explorer")

    # 21
    if buildOptions.get('PAGMODE') is not None:
        if buildOptions['PAGMODE'] in ['full', 'lite']:
            command += f" -DPAGMODE={buildOptions['PAGMODE']} "
        else:
            logging.error(
                f"PAGMODE:{buildOptions['PAGMODE']} is not supported")
            raise Exception(
                f"PAGMODE:{buildOptions['PAGMODE']} is not supported")
    else:
        logging.info("PAGMODE is not setted")

    # 22
    if buildOptions.get('GRANT_VALUE') is not None and type(buildOptions['GRANT_VALUE']) == int:
        command += f" -DGRANT_VALUE={buildOptions['GRANT_VALUE']} "
    else:
        logging.info("GRANT_VALUE is not setted")

    logging.info(f"cmake command is {command}")
    return command


def buildInstall(options, verMode):
    buildOptions = options['buildOptions']
    prepareDir(options)

    logging.info(
        f"{buildOptions['OSTYPE']}-{buildOptions['CPUTYPE']} of {'community' if options['verMode']=='edge' else 'enterprise'} will be build.")
    command = generateCmakeCommand(buildOptions, verMode)

    os.chdir(compileDir)
    logging.info("compile Dir:{0}".format(compileDir))
    executeCommand(command)

    logging.info(f"cmake -j {cpuCount} ")
    executeCommand(f"make -j {cpuCount}")
    # executeCommand(f"make")

    logging.info(f"making install")
    executeCommand(f"make install")
    os.chdir(scriptDir)


def makeTDenginePackages(packages, buildOptions, verMode):
    # pack TDengine.tar.gz
    if packages.get('tar.gz') is not None and packages['tar.gz']['enable'] == True:

        if packages['tar.gz']['server']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-server-{buildOptions['VERNUMBER'] }-tar.gz")
            mkpkg.makeServerTar(
                packages['tar.gz'], buildOptions, verMode)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-server-{buildOptions['VERNUMBER'] }-tar.gz")

        if packages['tar.gz']['client']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-client-{buildOptions['VERNUMBER'] }-tar.gz")
            mkpkg.makeTarClient(
                packages['tar.gz'], buildOptions, verMode)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-client-{buildOptions['VERNUMBER'] }-tar.gz")
    else:
        logging.warning(
            f"Skip packing TDengine-{verMode}-{buildOptions['VERNUMBER'] } tar.gz packages")

    logging.debug(packages)

    # pack TDengine.rpm
    if packages.get('rpm') is not None and packages['rpm']['enable'] == True:

        if packages['rpm']['server']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-server-{buildOptions['VERNUMBER']}.rpm")
            os.chdir(os.path.join(communityDir, "packaging", 'deb'))
            mkpkg.makeServeRpm(buildOptions, verMode)
            os.chdir(scriptDir)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-server-{buildOptions['VERNUMBER']}.rpm")

        if packages['rpm']['client']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-client-{buildOptions['VERNUMBER']}.rpm")
            os.chdir(os.path.join(communityDir, "packaging", 'rpm'))
            mkpkg.makeClientRpm(buildOptions)
            os.chdir(scriptDir)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-client-{buildOptions['VERNUMBER'] }.rpm")
    else:
        logging.warning(
            f"Skip packing TDengine-{verMode}-{buildOptions['VERNUMBER'] } rpm packages")

    # pack TDengine.deb
    if packages.get('deb') is not None and packages['deb']['enable'] == True:

        if packages['deb']['server']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-server-{buildOptions['VERNUMBER'] }.deb")
            os.chdir(os.path.join(communityDir, "packaging", 'deb'))
            mkpkg.makeServerDeb(buildOptions, verMode)
            os.chdir(scriptDir)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-server-{buildOptions['VERNUMBER'] }.deb")

        if packages['deb']['client']['enable'] == True:
            logging.info(
                f"making TDengine-{verMode}-client-{buildOptions['VERNUMBER'] }.deb")
            os.chdir(os.path.join(communityDir, "packaging", 'deb'))
            mkpkg.makeClientDeb(buildOptions)
            os.chdir(scriptDir)
        else:
            logging.warning(
                f"Skip packing TDengine-{verMode}-client-{buildOptions['VERNUMBER'] }.deb")
    else:
        logging.warning(
            f"Skip packing TDengine-{verMode}-{buildOptions['VERNUMBER'] } deb packages")


def makeTaosToolsPackages(taosToolsPackages, buildOptions, verMode):
    # pack taosTools.tar.gz
    if taosToolsPackages['enable'] == True:
        if taosToolsPackages['tar.gz']['enable'] == True:
            logging.info(f"making taosToolstar.gz")
            mkpkg.makeTaosToolsTar(
                taosToolsPackages['tar.gz'], options['buildOptions'], options['verMode'])
        else:
            logging.warning(f"Skip packing taosTools.tar.gz")

        if taosToolsPackages['rpm']['enable'] == True:
            logging.info(f"making taosTools-{getTaosToolVersion()}.rpm")
            mkpkg.makeTaosToolsRpm(buildOptions, verMode)
        else:
            logging.warning(
                f"Skip packing taosTools-{getTaosToolVersion()}.rpm")

        if taosToolsPackages['deb']['enable'] == True:
            logging.info(f"making taosTools-{getTaosToolVersion()}.deb")
            mkpkg.makeTaosToolsDeb(buildOptions, verMode)
    else:
        logging.info("Skip making taosTools packages")


def makePackages(options, buildOptions, verMode):
    # =====================for TDengine =========================
    if options.get('TDenginePackages') is not None and options['TDenginePackages']['enable'] == True:
        logging.info(
            "making TDengine packages like TDengine-server or TDengine-client")
        mkpkg.makePackages(options['TDenginePackages'], buildOptions, verMode)
    else:
        logging.warning("Skip making TDengine packages")

    # ========= for taos tools ==========
    if options.get('taosToolsPackages') is not None and options['taosToolsPackages']['enable'] == True:
        mkpkg.makeTaosToolPackages(
            options['taosToolsPackages'], buildOptions, verMode)
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
            logging.info('action name:{0} command:{1} will be executed'.format(
                key, ' '.join(values)))
            executeCommand(' '.join(values))

def doWindowsRelease(options):
    logging.info("windows release ...")
    
    
    if options.get('CPU') is not None:
        cpuType = options['CPU']
    else: 
        cpuType = 'x64'
        
    if options.get('verMode') is not None:
        verMode = options['verMode']
    else: 
        raise Exception("verMode is not set")
    
    if options.get('version') is not None:
        verNumber = options['version']
    else: 
        raise Exception("verNumber is not set")
    
    if options.get('CUSTOMER_NAME') is not None:
        custName = options['CUSTOMER_NAME']
    else: 
        custName = 'TDengine'
    
    if options.get('CUSTOMER_PROMPT') is not None:
        custPrompt = options['CUSTOMER_PROMPT']
    else: 
        custPrompt = 'taos'
    
    if options.get('CUSTOMER_EMAIL') is not None:
        custEmail = options['CPU']
    else: 
        custEmail = 'support@taosdata.com'
    
    if options.get('GRANT_VALUE') is not None:
        grant = options['GRANT_VALUE']
    else: 
        grant = '60' 
    
    
    command = f'python win_release.py --version {verMode} --number {verNumber} -N {custName} -M {custEmail} -P {custPrompt}'
    logging.info(f'release command {command}')
    
    os.system(command)

def doMacosRelease(options):
     logging.info("macos release ...")
     if options.get('CPU') is not None and  options['CPU'] in ['x64','arm64']:
        cpuType = options['CPU']
     else:
         raise Exception("CPU is not set or not supported")
     
     if options.get('verMode') is not None and options['verMode'] == 'edge':
         verMode = options['verMode']
     else:
         raise Exception("verMode is not set or not supported")
    
     if options.get('version') is not None:
         verNumber = options['version']
     else:
         raise Exception("verNumber is not set")
        
     command = f'./release_mac.sh -b 3.0 -c {cpuType} -n {verNumber} -l full -v {verMode} -V stable -d no'
     logging.info("mac-{1} release command {0}".format(command,cpuType))
     
     os.system(command)

def doLinuxRelease(options,args):
    logging.info("linux release ...")
    targets = []
    if options['TDenginePackages'].get('tar.gz') is not None and options['TDenginePackages']['tar.gz']['enable'] == True:
        targets.append('tar.gz')
    if options['TDenginePackages'].get('rpm') is not None and options['TDenginePackages']['rpm']['enable'] == True:
        targets.append('rpm')
    if options['TDenginePackages'].get('deb') is not None and options['TDenginePackages']['deb']['enable'] == True:
        targets.append('deb')
    logging.info("{0}-{1}-{2} packages [{4}] version {3} will be released".format(
        options['verMode'],
        options['buildOptions']['OSTYPE'],
        options['buildOptions']['CPUTYPE'],
        options['buildOptions']['VERNUMBER'],
        ','.join(targets))
    )
    checkAndInitInput(options, args.version)
    # ================= build and install
    buildInstall(options, options['verMode'])
    # ================ making packages
    makePackages(options, options['buildOptions'], options['verMode'])
     

def doRelease(options, preActions, postActions, args):
    
    # ================do parepare acotions
    logging.info(preActions)
    if preActions['enable'] == True:
        logging.info("will do parepare acotions.")
        doActions(readOption(options, 'prepareActions'))
        logging.info("parepare acotions done.")
    else:
        logging.info("No post acotions.")


    # ================do release acotions
    if options.get('target') is not None and options['target'] == 'windows':
        doWindowsRelease(options)
    elif options.get('target') is not None and options['target'] == 'macos':
        doMacosRelease(options)
    elif options.get('target') is None or options['target'] == 'linux':
        doLinuxRelease(options,args)
    else:
        logging.error("unsupport target:{0}".format(options['target']))
        raise Exception("unsupport target:{0}".format(options['target']))
    
    # ================do post acotions
    if postActions['enable'] == True:
        logging.info("will do post acotions.")
        doActions(readOption(options, 'postActions'))
        logging.info("post acotions done.")
    else:
        logging.info("No post acotions.")


def readConfigFile(configFile, args):
    with open(os.path.join(scriptDir, configFile), 'r') as file:
        yamlfile = yaml.safe_load(file)

    if yamlfile.get('name') is not None:
        logging.info(f"{yamlfile['name']}")
    else:
        logging.warning(f"{configFile} name is empty")

    if yamlfile.get('type') is not None:
        if yamlfile['type'] == 'workflow':
            logging.info("This is a workflow config file")
            if yamlfile.get('tasks') is not None:

                for task in yamlfile['tasks']:
                    logging.info(f"task:{task}")
                    readConfigFile(task['task'], args)
            else:
                logging.warning("tasks are empty")
                return
        elif yamlfile['type'] == 'feature':
            logging.info(
                f"config file:{configFile} verMode:{yamlfile['parameters']['verMode']}")
            doRelease(
                yamlfile['parameters'], yamlfile['prepareActions'], yamlfile['postActions'], args)
        else:
            logging.error(
                f"unsupport config {configFile}'s type {yamlfile['type']}")
            sys.exit(1)
    else:
        logging.error("config file type is empty")
        sys.exit(1)


if __name__ == "__main__":
    parse = argparse.ArgumentParser(description='manual to this script')
    parse.add_argument('--file', type=str, default='enterprise.yaml',
                       help='Config file name,default enterprise.yaml', required=True)
    parse.add_argument('--version', type=str, default=None,
                       help='Release version, if empty will use version number define in config file.', required=False)
    parse.add_argument('--vermode', type=str, default='enterprise',
                       help='Release version mode enterprise or community, default enterprise.', required=False)
    parse.add_argument('--loglevel', type=str, default='info',
                       help="default info, extra support debug", required=False)

    args = parse.parse_args()

    setLogLevel(args.loglevel)

    logging.info(
        f"base on config yaml {args.file}")

    # with open(os.path.join(scriptDir, args.file), 'r') as file:
    #     yamlfile = yaml.safe_load(file)
    # options = readOption(yamlfile,'parameters')
    readConfigFile(args.file, args)
