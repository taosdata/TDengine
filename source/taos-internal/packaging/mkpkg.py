import os
import logging
import subprocess
import shutil
import requests
import tempfile


# TDinternal/enterprise/packaging/
scriptDir = os.path.dirname(os.path.realpath(__file__))
# TDinternal
topDir = os.path.join(scriptDir, '..', '..')
communityDir = os.path.join(topDir, 'community')
compileDir = os.path.join(communityDir, 'debug')
releaseDir = os.path.join(communityDir, 'release')
buildDir = os.path.join(compileDir, 'build')
communityScriptDir = os.path.join(communityDir, 'packaging', 'tools')
taosToolsDir = os.path.join(communityDir, 'tools', 'taos-tools')
webDir = os.path.join(topDir, 'enterprise', 'src', 'plugins', 'web')
communityCfgDir = os.path.join(communityDir, 'packaging', 'cfg')
# for server
tarName = 'package.tar.gz'
packagingDir = ''
installDir = ''

# for client
clientInstallDir = ''

# for taos-tools
taosToolsInstallDir = ''

# restore file path
binFiles = []
taosToolsBinFiles = []
taosxBinFile = []
tdinsightCaches = []
libFiles = []
headerFiles = []
initFileDeb = []
initFileRpm = []
cfgFiles = []
verMode = ''

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


# ======== common util function =========
def executeCommand(command, ifCheck=True):
    logging.info("execute >>>>>>>>>>> {0}".format(command))
    ret = subprocess.run(command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
    logging.info(ret.stdout)
    ret.check_returncode()
    logging.info("done <<<<<<<<<<<< {0}".format(command))


def getTaosToolVersion():
    os.chdir(os.path.join(communityDir, 'tools',
             'taos-tools', 'packaging', 'deb'))
    code, output = subprocess.getstatusoutput(
        'git for-each-ref --sort=taggerdate --format \'%(tag)\' refs/tags|grep -v taos | tail -1')
    if code == 0:
        os.chdir(scriptDir)
        return output
    else:
        raise Exception(
            "get taosTools's version failed, reason {1}".format(output))


def utilCopyFile(srcFile, dst):
    if os.path.isfile(srcFile):
        logging.info(f'copy {srcFile} into {dst}')
        shutil.copy(srcFile, dst)
    else:
        logging.error(f'{srcFile} doesn\'t exists')
        raise Exception(f'{srcFile} doesn\'t exists')


def utilCopyTree(srcDir, dst):
    if os.path.isdir(srcDir):
        logging.info(f'copy {srcDir} into {dst}')
        shutil.copytree(srcDir, dst)
    else:
        logging.error(f'{srcDir} doesn\'t exists')
        raise Exception(f'{srcDir} doesn\'t exists')


def downloadTDinsight():
    url = "https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh"
    dstFile = os.path.join(buildDir, 'bin', 'TDinsight.sh')

    logging.info('Trying to download TDinsight.sh')

    # Download TDinsight.sh
    response = requests.get(url)
    if response.status_code == 200:
        with open(dstFile, 'wb') as f:
            f.write(response.content)
        logging.info("TDinsight.sh downloaded!")
    else:
        logging.error("failed to download TDinsight.sh")
        raise Exception("failed to download TDinsight.sh")

    # # download TDinsight caches
    os.chdir(os.path.join(buildDir, 'bin'))
    os.chmod(dstFile, 0o755)
    subprocess.check_output(["./TDinsight.sh", "--download-only"])

    os.chdir(scriptDir)


def buildTaosKeeper(verMode, cpuType):
    os.chdir(releaseDir)
    logging.info("build taoskeeper for {0}".format(verMode))

    if cpuType in ["x64", "x86_64", "amd64"]:
        arch = "amd64"
    elif cpuType in ["x32", "i386", "i686"]:
        arch = "386"
    elif cpuType in ["arm", "aarch32"]:
        arch = "arm"
    elif cpuType in ["arm64", "aarch64"]:
        arch = "arm64"
    else:
        arch = cpuType

    if verMode == "edge" or verMode == "all":
        logging.info("build taoskeeper for community")
        command = f" bash {os.path.join(scriptDir)}/build_taoskeeper.sh -r {arch} -e taoskeeper"

    elif verMode == "cluster" or verMode == "all":
        logging.info("build taoskeeper for enterprise")
        command = f" bash {os.path.join(scriptDir)}/build_taoskeeper.sh -r {arch} -e taoskeeperinternal"
    else:
        logging.error("invalid vermode:{0}", verMode)
        raise Exception("invalid vermode")
    executeCommand(command)
    os.chdir(scriptDir)


def copyJemllocFiles(packageComponent, type):
    logging.info("packing Jemalloc files.")
    logging.debug(packageComponent)
    global installDir, clientInstallDir
    if type == 'server':
        baseDir = installDir
    elif type == 'client':
        baseDir = clientInstallDir
    else:
        logging.error("invalid type:{0}", type)
        raise Exception("invalid type")

    if packageComponent['package.tar.gz']['jemalloc'] == True:
        if os.path.isfile(os.path.join(buildDir, 'bin', 'jemalloc-config')):
            # make dir
            dirNames = ['bin', 'lib', 'lib/pkgconfig',
                        'include', 'include/emalloc', 'share/doc', 'share/man/man3']
            for dir in dirNames:
                if dir == 'bin':
                    os.makedirs(os.path.join(baseDir, 'jemalloc',
                                dir), exist_ok=True, mode=0o755)
                else:
                    os.makedirs(os.path.join(
                        baseDir, 'jemalloc', dir), exist_ok=True)
                logging.info(f"mkdir {os.path.join(baseDir,'jemalloc',dir)}")
            # copy files
            utilCopyFile(os.path.join(buildDir, 'bin', 'jemalloc-config'),
                         os.path.join(baseDir, 'jemalloc', 'bin'))
            utilCopyFile(os.path.join(buildDir, 'bin', 'jemalloc.sh'),
                         os.path.join(baseDir, 'jemalloc', 'bin'))
            utilCopyFile(os.path.join(buildDir, 'bin', 'jeprof'),
                         os.path.join(baseDir, 'jemalloc', 'bin'))
            utilCopyFile(os.path.join(buildDir, 'include', 'jemalloc', 'jemalloc.h'), os.path.join(
                baseDir, 'jemalloc', 'include', 'jemalloc'))
            utilCopyFile(os.path.join(buildDir, 'lib', 'libjemalloc.so.2'),
                         os.path.join(baseDir, 'jemalloc', 'lib'))
            # utilCopyFile(os.path.join(buildDir, 'lib', 'libjemalloc.a'),
            #              os.path.join(baseDir, 'jemalloc', 'lib'))
            # utilCopyFile(os.path.join(buildDir, 'lib', 'libjemalloc_pic.a'), os.path.join(
            #     baseDir, 'jemalloc', 'lib'))
            utilCopyFile(os.path.join(buildDir, 'lib', 'pkgconfig', 'jemalloc.pc'), os.path.join(
                baseDir, 'jemalloc', 'lib', 'pkgconfig'))
            utilCopyFile(os.path.join(buildDir, 'share', 'doc', 'jemalloc', 'jemalloc.html'), os.path.join(
                baseDir, 'jemalloc', 'share', 'doc', 'jemalloc'))
            utilCopyFile(os.path.join(buildDir, 'share', 'man', 'man3', 'jemalloc.3'), os.path.join(
                baseDir, 'jemalloc', 'share', 'man', 'man3'))

        else:
            logging.error(
                "debug/build/bin/jemalloc-config not exist, build failed.")
            raise Exception(
                "build with jemalloc failed, can't found debug/build/bin/jemalloc-config")
    else:
        logging.warning('package.tar.gz will not contain dictory jemalloc')

    logging.info("copy Jemalloc files.")


def makePackageDirs(dirs):
    logging.info(
        f"mkdir {packagingDir} packaging steps will be done in this dir.")
    os.makedirs(packagingDir, exist_ok=True)

    for dir in dirs:
        if dir == 'bin':
            os.makedirs(os.path.join(packagingDir, 'bin'),
                        exist_ok=True, mode=0o755)
        else:
            os.makedirs(os.path.join(packagingDir, dir), exist_ok=True)


def makePackageName(buildOptions, verMode, type) -> any:
    if buildOptions.get('PAGMODE') is None:
        tmpPagMode = 'full'
    else:
        tmpPagMode = buildOptions['PAGMODE']
    
    
    if type == 'server':
        if verMode == 'cluster' and tmpPagMode == 'full':
            # TDengine-enterprise-server-3.0.3.1.20230327-Linux-x64
            pkgName = '{0}-{1}-server-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-server-{2}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'])
        elif verMode == 'cluster' and tmpPagMode == 'lite':
            pkgName = '{0}-{1}-server-{2}-{6}{3}-{4}-{5}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                                 buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-server-{2}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'])
        elif verMode == 'cloud' and tmpPagMode == 'full':
            pkgName = '{0}-{1}-server-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-server-{2}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'])
        elif verMode == 'edge' and tmpPagMode == 'full':
            pkgName = '{0}-server-{1}-{4}{2}-{3}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                         buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-server-{1}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'])
        elif verMode == 'edge' and tmpPagMode == 'lite':
            pkgName = '{0}-server-{1}-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-server-{1}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'])
        else:
            logging.error("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
            raise Exception("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
    elif type == 'client':
        if verMode == 'cluster' and tmpPagMode == 'full':
            # TDengine-enterprise-client-3.0.3.1.20230327-Linux-x64
            pkgName = '{0}-{1}-client-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-client-{2}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'])
        elif verMode == 'cluster' and tmpPagMode == 'lite':
            pkgName = '{0}-{1}-client-{2}-{3}-{6}{4}-{5}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                                 buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-client-{2}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'])
        elif verMode == 'cloud' and tmpPagMode == 'full':
            pkgName = '{0}-{1}-client-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-{1}-client-{2}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'])
        elif verMode == 'edge' and tmpPagMode == 'full':
            pkgName = '{0}-client-{1}-{4}{2}-{3}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                         buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-client-{1}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'])
        elif verMode == 'edge' and tmpPagMode == 'lite':
            pkgName = '{0}-client-{1}-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
            folderName = '{0}-client-{1}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'])
        else:
            logging.error("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
            raise Exception("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
    elif type == 'taosTools':
        # taosTools-2.4.10-Linux-x64-comp3
        compVersion = buildOptions['VERCOMPATIBLE'].split('.')[0]
        pkgName = '{0}Tools-{1}-{5}{2}-{3}-comp{4}'.format(buildOptions['CUS_PROMPT'], getTaosToolVersion(
        ), buildOptions['OSTYPE'], buildOptions['CPUTYPE'], compVersion, '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        folderName = '{0}Tools-{1}'.format(buildOptions['CUS_PROMPT'], getTaosToolVersion())
    else:
        logging.error("unsupported type:{0}".format(type))
        raise Exception("unsupported type:{0}".format(type))
    return {'name': pkgName, 'folder': folderName}


def copyDriver(buildOption):
    global packagingDir, libFiles
    logging.info("copying driver")
    os.makedirs(os.path.join(packagingDir, 'driver'), exist_ok=True)

    for lib in libFiles:
        if lib == 'vercomp.txt':
            # write into vercomp.txt
            logging.info("writing vercomp.txt")
            with open(os.path.join(packagingDir, 'driver', 'vercomp.txt'), 'w') as f:
                f.write(buildOption['VERCOMPATIBLE'])
        else:
            utilCopyFile(os.path.join(lib),
                         os.path.join(packagingDir, 'driver'))


def copyJDBC(url, tag='3.2.1'):
    logging.info("building JDBC")
    global packagingDir
    repoPath = os.path.join(packagingDir, 'connector', 'jdbc')
    os.system(f"git clone -b {tag} --depth 1 {url} {repoPath}")

    os.chdir(os.path.join(packagingDir, 'connector', 'jdbc'))
    executeCommand('mvn clean package -Dmaven.test.skip=true')
    utilCopyFile(os.path.join(repoPath, 'target',
                 'taos-jdbcdriver-{0}-dist.jar'.format(tag)), os.path.join(packagingDir, 'connector'))
    utilCopyFile(os.path.join(repoPath, 'target',
                 'taos-jdbcdriver-{0}.jar'.format(tag)), os.path.join(packagingDir, 'connector'))
    utilCopyFile(os.path.join(repoPath, 'target',
                 'taos-jdbcdriver-{0}-sources.jar'.format(tag)), os.path.join(packagingDir, 'connector'))
    os.chdir(scriptDir)

    shutil.rmtree(repoPath)
    logging.info("building and copy JDBC success")


def copyExamples(packageComponent, buildOptions):
    logging.info("copying examples")
    global packagingDir, taosToolsDir
    os.chdir(packagingDir)
    if buildOptions['DBNAME'] == 'taos':
        dstExampleDir = os.path.join(packagingDir, 'examples')
        srcExampleDir = os.path.join(communityDir, 'examples')
        os.makedirs(dstExampleDir, exist_ok=True)
        
        logging.debug(
                f"copy examples from {srcExampleDir} to {dstExampleDir}")
        for example in packageComponent['examples']:
            if example == 'taosbenchmark-json':
                utilCopyTree(os.path.join(taosToolsDir, 'example'),
                             os.path.join(dstExampleDir, 'taosbenchmark-json'))
            else:
                utilCopyTree(os.path.join(srcExampleDir, example),
                                 os.path.join(dstExampleDir, example))
            logging.info("copy examples of {0} into {1} success".format(
                    example, dstExampleDir))

        if buildOptions.get('PAGMODE') is not None and buildOptions['PAGMODE'] != 'lite' and buildOptions['CPUTYPE'] != 'aarch32':
            targetToRemove = [
                os.path.join(dstExampleDir, 'examples', 'JDBC',
                             'connectionPools', 'target'),
                os.path.join(dstExampleDir, 'examples',
                             'JDBC', 'JDBCDemo', 'target'),
                os.path.join(dstExampleDir, 'examples', 'JDBC',
                             'mybatisplus-demo', 'target'),
                os.path.join(dstExampleDir, 'examples', 'JDBC',
                             'springbootdemo', 'target'),
                os.path.join(dstExampleDir, 'examples', 'JDBC',
                             'SpringJdbcTemplate', 'target'),
                os.path.join(dstExampleDir, 'examples',
                             'JDBC', 'taosdemo', 'target'),
            ]
            for target in targetToRemove:
                if os.path.exists(target):
                    shutil.rmtree(target)
            logging.info(
                "copy examples into {0} success".format(dstExampleDir))
    else:
        logging.warning("skip copying examples, since DBNAME:{0} is not taos".format(
            buildOptions['DBNAME']))
    os.chdir(scriptDir)


def copyConnectors(packageComponent):
    gitRepos = {
        "go": "https://github.com/taosdata/driver-go",
        "python": "https://github.com/taosdata/taos-connector-python",
        "nodejs": "https://github.com/taosdata/taos-connector-node",
        "dotnet": "https://github.com/taosdata/taos-connector-dotnet",
        "rust": "https://github.com/taosdata/libtaos-rs",
        "jdbc": "https://github.com/taosdata/taos-connector-jdbc.git"
    }
    global packagingDir
    logging.info("downloading and copy connectors")

    os.makedirs(os.path.join(packagingDir, 'connector'), exist_ok=True)
    for connector in packageComponent['connector']:
        url = gitRepos[connector]
        repoPath = os.path.join(packagingDir, 'connector', connector)
        logging.info("downloading {0} from {1}".format(connector, url))
        if connector == 'jdbc':
            copyJDBC(url)
        else:
            logging.info("cloning {0} to {1}".format(connector, repoPath))
            os.system(f"git clone --depth 1 {url} {repoPath}")
            shutil.rmtree(os.path.join(repoPath, '.git'))
    logging.info("copy connectors success")


def copyInstallScript(packageComponent, buildOption, verMode, type):
    logging.info("copying install script")
    logging.debug(packageComponent)
    logging.info(buildOption)
    global installDir, clientInstallDir
    if type == 'client':
        path = packageComponent['install_client.sh'].split('/')
        baseDir = globals()[path[0]]
        path[0] = baseDir
        utilCopyFile(os.path.join(*path), clientInstallDir)
        if buildOption['OSTYPE'] == 'Darwin':
            with open(os.path.join(clientInstallDir, 'install_client.sh')) as f_in, open(os.path.join(clientInstallDir, 'install_client_temp.sh'), 'w') as f_out:
                # 将 Linux 替换为 Darwin
                for line in f_in:
                    f_out.write(line.replace('osType=Linux', 'osType=Darwin'))
                    os.replace(os.path.join(clientInstallDir, 'install_client_temp.sh'), os.path.join(
                        clientInstallDir, 'install_client.sh'))

        if verMode == "cluster":
            shutil.copyfile(os.path.join(clientInstallDir, 'install_client.sh'), os.path.join(
                clientInstallDir, 'install_client_tmp.sh'))
            with open(os.path.join(clientInstallDir, 'install_client_tmp.sh')) as f_in:
                file_data = f_in.read()
            # 将 edge 替换为 cluster
            file_data = file_data.replace('verMode=edge', 'verMode=cluster')
            file_data = file_data.replace(
                'serverName2=\"taosd\"', 'serverName2=\"{0}d\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'configFile=\"taos.cfg\"', 'configFile=\"{0}.cfg\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOption['CUS_NAME']))
            file_data = file_data.replace('emailName2=\"taosdata.com\"', 'emailName2=\"{0}\"'.format(
                buildOption['CUS_EMAIL'].split('@')[1]))
            logging.debug(file_data)
            with open(os.path.join(clientInstallDir, 'install_client.sh'), 'w') as f_out:
                f_out.write(file_data)
            os.chmod(os.path.join(clientInstallDir, 'install_client.sh'), 0o755)
            os.remove(os.path.join(clientInstallDir, 'install_client_tmp.sh'))

        if verMode == "cloud":
            with open(os.path.join(clientInstallDir, 'install_client.sh')) as f_in, open(os.path.join(clientInstallDir, 'install_client_temp.sh'), 'w') as f_out:
                for line in f_in:
                    f_out.write(line.replace('pagMode=edge', 'pagMode=cloud'))
            os.replace(os.path.join(clientInstallDir, 'install_client_temp.sh'), os.path.join(
                clientInstallDir, 'install_client.sh'))
            os.chmod(os.path.join(clientInstallDir, 'install.sh'), 0o755)

        if buildOption['PAGMODE'] == "lite":
            with open(os.path.join(clientInstallDir, 'install_client.sh')) as f_in, open(os.path.join(clientInstallDir, 'install_client_temp.sh'), 'w') as f_out:
                # 将 full 替换为 lite
                for line in f_in:
                    f_out.write(line.replace('pagMode=full', 'pagMode=lite'))
            os.replace(os.path.join(clientInstallDir, 'install_client_temp.sh'), os.path.join(
                clientInstallDir, 'install_client.sh'))
            os.chmod(os.path.join(clientInstallDir, 'install.sh'), 0o755)
        logging.info("copy client_install.sh success")

    elif type == 'server':
        path = packageComponent['install.sh'].split('/')
        baseDir = globals()[path[0]]
        path[0] = baseDir
        utilCopyFile(os.path.join(*path), installDir)
        if verMode == 'cluster':
            # copy install_taosKeeper.sh
            logging.info("copying install_taoskeeper.sh")
            with open(os.path.join(scriptDir, 'install_taoskeeper.sh'), 'r') as f_out:
                with open(os.path.join(installDir, 'install.sh'), 'a') as f_in:
                    f_in.write(f_out.read())
            # update install.sh to replace clust and OEM info
            shutil.copyfile(os.path.join(installDir, 'install.sh'), os.path.join(
                installDir, 'install_tmp.sh'))
            with open(os.path.join(installDir, 'install_tmp.sh'), 'r') as f:
                file_data = f.read()
            file_date = file_data.replace('verMode=edge', 'verMode=cluster')
            file_data = file_date.replace(
                'serverName2=\"taosd\"', 'serverName2=\"{0}d\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'configFile2=\"taos.cfg\"', 'configFile2=\"{0}.cfg\"'.format(buildOption['CUS_PROMPT']))
            file_data = file_data.replace(
                'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOption['CUS_NAME']))
            file_data = file_data.replace('emailName2=\"taosdata.com\"', 'emailName2=\"{0}\"'.format(
                buildOption['CUS_EMAIL'].split('@')[1]))

            with open(os.path.join(installDir, 'install.sh'), 'w') as f_in:
                f_in.write(file_data)
            os.chmod(os.path.join(installDir, 'install.sh'), 0o755)
            os.remove(os.path.join(installDir, 'install_tmp.sh'))

        if buildOption['PAGMODE'] == "cloud":
            with open(os.path.join(installDir, 'install.sh'), 'r') as f_in,\
                    tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
                for line in f_in:
                    f_out.write(line.replace('pagMode=edge', 'pagMode=cloud'))
            os.replace(f_out.name, os.path.join(installDir, 'install.sh'))
            os.chmod(os.path.join(installDir, 'install.sh'), 0o755)

        if buildOption['PAGMODE'] == "lite":
            with open(os.path.join(installDir, 'install.sh'), 'r') as f_in,\
                    tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
                for line in f_in:
                    f_out.write(line.replace('pagMode=full', 'pagMode=lite'))
            os.replace(f_out.name, os.path.join(installDir, 'install.sh'))
            os.chmod(os.path.join(installDir, 'install.sh'), 0o755)

        logging.info("copy install.sh success")
    else:
        logging.error(
            "fail to copy install.sh, since type:{0} is not client or server".format(type))
        raise Exception(
            "fail to copy install.sh, since type:{0} is not client or server".format(type))


def copyShares(packageComponent):
    logging.info("copying web files")
    global packagingDir, webDir
    logging.debug(packageComponent)
    items = packageComponent['share']
    for item in items:
        for k in item:   
            dstDir = os.path.join(packagingDir, 'share', *(k.split('/')))
            if not os.path.exists(dstDir):
                os.makedirs(dstDir, exist_ok=True)
            else:
                shutil.rmtree(dstDir)
                os.makedirs(dstDir, exist_ok=True)

            tmpDir = item[k]['src'].split('/')
            tmp = globals()[tmpDir[0]]
            tmpDir[0] = tmp
            
            srcDir = os.path.join(*(tmpDir))
            
            logging.info("copy {0} to {1}".format (srcDir,dstDir))
            if item[k].get('folder') is not None and item[k]['folder'] == True:
                shutil.copytree(srcDir, dstDir, symlinks=True, copy_function=shutil.copy2, dirs_exist_ok=True)
            else:
                utilCopyFile(srcDir, dstDir)

def initBinFiles(packageComponent, buildOptions, verMode):
    global binFiles
    binFiles.clear()
    logging.debug("packageComponent:{0}".format(packageComponent))
    for bin in packageComponent['package.tar.gz']['bin']:
        tmp = bin.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        if tmp[-1] == 'taoskeeper':
            buildTaosKeeper(verMode, buildOptions['CPUTYPE'])
        if tmp[-1] == 'tdengine-datasource.zip':
            downloadTDinsight()
        binFiles.append(os.path.join(*(tmp)))
    logging.info(
        "binFiles:{0} will be packed in this time release".format(binFiles))


def initLibFiles(packageComponent, buildOptions):
    global libFiles
    libFiles.clear()

    for lib in packageComponent['driver']:
        if lib == 'vercomp.txt':
            tmp = 'vercomp.txt'
        else:
            tmp = lib.split('/')
            baseDir = globals()[tmp[0]]
            tmp[0] = baseDir
        logging.debug("tmp:{0}".format(tmp))
        if tmp[-1] == 'libtaos.so.':
            libname = tmp[-1]+buildOptions['VERNUMBER']
            tmp[-1] = libname
            libFiles.append(os.path.join(*(tmp)))
        elif tmp[-1] == 'libtaosws.so':
            libFiles.append(os.path.join(*(tmp)))
        elif tmp == 'vercomp.txt':
            libFiles.append('vercomp.txt')
        else:
            logging.error("unsupported lib:{0}".format(lib))
            raise Exception("unsupported lib:{0}".format(lib))
        logging.info(
            "libFiles:{0} will be packed in this time release".format(libFiles))


def initHeadFiles(packageComponent):
    global headerFiles
    headerFiles.clear()
    for header in packageComponent['package.tar.gz']['inc']:
        tmp = header.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        headerFiles.append(os.path.join(*(tmp)))
    logging.info(
        "headerFiles:{0} will be packed in this time release".format(headerFiles))


def initConfigFiles(packageComponent):
    global cfgFiles
    cfgFiles.clear()
    for cfg in packageComponent['package.tar.gz']['cfg']:
        tmp = cfg.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        cfgFiles.append(os.path.join(*(tmp)))
    logging.info(
        "cfgFiles:{0} will be packed in this time release".format(cfgFiles))


def initdFiles(packageComponent):
    global initFileDeb, initFileRpm
    initFileRpm.clear()
    initFileRpm.clear()

    for init in packageComponent['package.tar.gz']['init.d']:
        tmp = init.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        if tmp[-2] == 'deb':
            initFileDeb.append(os.path.join(*(tmp)))
            logging.info(
                "initFileDeb:{0} will be packed in this time release".format(initFileDeb))
        elif tmp[-2] == 'rpm':
            initFileRpm.append(os.path.join(*(tmp)))
            logging.info(
                "initFileRpm:{0} will be packed in this time release".format(initFileRpm))
        else:
            logging.error(f"{tmp[-1]} is not supported")
            raise Exception(f"{tmp[-1]} is not supported")


def initTaosToolsBinFiles(packageComponent):
    global taosToolsBinFiles
    taosToolsBinFiles.clear()
    for tool in packageComponent['bin']:
        tmp = tool.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        if tmp[-1] == 'tdengine-datasource_zip':
            downloadTDinsight()
        taosToolsBinFiles.append(os.path.join(*(tmp)))


def initRemoveServerScript(verMode, buildOptions):
    logging.info("init remove.sh ...")
    if verMode not in ['edge', 'cluster', 'cloud']:
        raise Exception("unsupported verMode:{0}".format(verMode))

    if buildOptions.get('PAGMODE') is not None and buildOptions['PAGMODE'] not in ['full', 'lite']:
        raise Exception("unsupported pagMode:{0}".format(
            buildOptions['PAGMODE']))

    if os.path.exists(os.path.join(releaseDir, 'build-taoskeeper')):
        logging.info("appending remove_taoskeeper.sh into bin/remove.sh")
        with open(os.path.join(packagingDir, 'bin', 'remove.sh'), 'a') as f_out:
            with open(os.path.join(scriptDir, 'remove_taoskeeper.sh'), 'r') as f_in:
                f_out.write(f_in.read())
            logging.info(
                "appending remove_taoskeeper.sh into bin/remove.sh done")
    else:
        logging.warning(
            "taoskeeper is not built, skip appending remove_taoskeeper.sh into bin/remove.sh")

    if verMode == 'cluster':
        shutil.copyfile(os.path.join(packagingDir, 'bin', 'remove.sh'),
                        os.path.join(packagingDir, 'bin', 'remove_tmp.sh'))
        # modify content of remove.sh
        with open(os.path.join(packagingDir, 'bin', 'remove_tmp.sh'), 'r') as f_in:
            file_data = f_in.read()
        file_data = file_data.replace('verMode=edge', 'verMode=cluster')
        file_data = file_data.replace(
            'serverName2=\"taosd\"', 'clientName2=\"{0}d\"'.format(buildOptions['CUS_PROMPT']))
        file_data = file_data.replace(
            'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOptions['CUS_PROMPT']))
        file_data = file_data.replace(
            'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOptions['CUS_NAME']))

        with open(os.path.join(packagingDir, 'bin', 'remove.sh'), 'w') as f_out:
            f_out.write(file_data)
        os.chmod(os.path.join(packagingDir, 'bin', 'remove.sh'), 0o755)
        os.remove(os.path.join(packagingDir, 'bin', 'remove_tmp.sh'))
        logging.info("modify content of remove.sh done")
    elif verMode == 'cloud':
        with open(os.path.join(releaseDir, 'bin', 'remove.sh'), 'r') as f_in,\
                tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
            for line in f_in:
                f_out.write(line.replace('verMode=edge', 'verMode=cloud'))
        os.replace(f_out.name, os.path.join(releaseDir, 'bin', 'remove.sh'))
        os.chmod(os.path.join(releaseDir, 'bin', 'remove.sh'), 0o755)
        logging.info("modify content of cloud remove.sh done")
    else:
        logging.info("verMode is edge, modify bin/remove.sh")


def initRemoveClientScript(verMode, buildOptions):
    logging.info("init remove_client.sh ...")
    if verMode not in ['edge', 'cluster', 'cloud']:
        raise Exception("unsupported verMode:{0}".format(verMode))

    if buildOptions.get('PAGMODE') is not None and buildOptions['PAGMODE'] not in ['full', 'lite']:
        raise Exception("unsupported pagMode:{0}".format(
            buildOptions['PAGMODE']))
    logging.info("nothing need modify with remove_client.sh")


def copyInstallServerScript(packageComponent, buildOption, verMode):
    logging.info("init install.sh")
    logging.debug(packageComponent)
    path = packageComponent['install.sh'].split('/')
    baseDir = globals()[path[0]]
    path[0] = baseDir
    utilCopyFile(os.path.join(*path), packagingDir)

    # copy install_taosKeeper.sh
    logging.info("copying install_taoskeeper.sh")
    if os.path.exists(os.path.join(releaseDir, 'build-taoskeeper')):
        with open(os.path.join(scriptDir, 'install_taoskeeper.sh'), 'r') as f_out:
            with open(os.path.join(packagingDir, 'install.sh'), 'a') as f_in:
                f_in.write(f_out.read())
    else:
        logging.warning(
            "taoskeeper is not built, skip appending install_taoskeeper.sh into bin/install.sh")
    # update install.sh to replace clust and OEM info
    if verMode == 'cluster':
        shutil.copyfile(os.path.join(packagingDir, 'install.sh'), os.path.join(
            packagingDir, 'install_tmp.sh'))
        with open(os.path.join(packagingDir, 'install_tmp.sh'), 'r') as f:
            file_data = f.read()
        file_date = file_data.replace('verMode=edge', 'verMode=cluster')
        file_data = file_date.replace(
            'serverName2=\"taosd\"', 'serverName2=\"{0}d\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'configFile2=\"taos.cfg\"', 'configFile2=\"{0}.cfg\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOption['CUS_NAME']))
        file_data = file_data.replace('emailName2=\"taosdata.com\"', 'emailName2=\"{0}\"'.format(
            buildOption['CUS_EMAIL'].split('@')[1]))

        with open(os.path.join(packagingDir, 'install.sh'), 'w') as f_in:
            f_in.write(file_data)
        os.chmod(os.path.join(packagingDir, 'install.sh'), 0o755)
        os.remove(os.path.join(packagingDir, 'install_tmp.sh'))

    if buildOption.get('PAGMODE') is not None and buildOption['PAGMODE'] == "cloud":
        with open(os.path.join(packagingDir, 'install.sh'), 'r') as f_in,\
                tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
            for line in f_in:
                f_out.write(line.replace('pagMode=edge', 'pagMode=cloud'))
        os.replace(f_out.name, os.path.join(packagingDir, 'install.sh'))
        os.chmod(os.path.join(packagingDir, 'install.sh'), 0o755)

    if buildOption.get('PAGMODE') is not None and buildOption['PAGMODE'] == "lite":
        with open(os.path.join(packagingDir, 'install.sh'), 'r') as f_in,\
                tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
            for line in f_in:
                f_out.write(line.replace('pagMode=full', 'pagMode=lite'))
        os.replace(f_out.name, os.path.join(packagingDir, 'install.sh'))
        os.chmod(os.path.join(packagingDir, 'install.sh'), 0o755)

    logging.info("copy install.sh success")


def copyInstallClientScript(packageComponent, buildOption, verMode):
    logging.info("init install_client.sh ...")

    path = packageComponent['install_client.sh'].split('/')
    baseDir = globals()[path[0]]
    path[0] = baseDir
    utilCopyFile(os.path.join(*path), packagingDir)
    if buildOption['OSTYPE'] == 'Darwin':
        with open(os.path.join(packagingDir, 'install_client.sh')) as f_in, open(os.path.join(packagingDir, 'install_client_temp.sh'), 'w') as f_out:
            # 将 Linux 替换为 Darwin
            for line in f_in:
                f_out.write(line.replace('osType=Linux', 'osType=Darwin'))
                os.replace(os.path.join(packagingDir, 'install_client_temp.sh'), os.path.join(
                    packagingDir, 'install_client.sh'))

    if verMode == "cluster":
        shutil.copyfile(os.path.join(packagingDir, 'install_client.sh'), os.path.join(
            packagingDir, 'install_client_tmp.sh'))
        with open(os.path.join(packagingDir, 'install_client_tmp.sh')) as f_in:
            file_data = f_in.read()
        # 将 edge 替换为 cluster
        file_data = file_data.replace('verMode=edge', 'verMode=cluster')
        file_data = file_data.replace(
            'serverName2=\"taosd\"', 'serverName2=\"{0}d\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'configFile=\"taos.cfg\"', 'configFile=\"{0}.cfg\"'.format(buildOption['CUS_PROMPT']))
        file_data = file_data.replace(
            'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOption['CUS_NAME']))
        file_data = file_data.replace('emailName2=\"taosdata.com\"', 'emailName2=\"{0}\"'.format(
            buildOption['CUS_EMAIL'].split('@')[1]))
        logging.debug(file_data)
        with open(os.path.join(packagingDir, 'install_client.sh'), 'w') as f_out:
            f_out.write(file_data)
        os.chmod(os.path.join(packagingDir, 'install_client.sh'), 0o755)
        os.remove(os.path.join(packagingDir, 'install_client_tmp.sh'))

    if verMode == "cloud":
        with open(os.path.join(packagingDir, 'install_client.sh')) as f_in, open(os.path.join(packagingDir, 'install_client_temp.sh'), 'w') as f_out:
            for line in f_in:
                f_out.write(line.replace('pagMode=edge', 'pagMode=cloud'))
        os.replace(os.path.join(packagingDir, 'install_client_temp.sh'), os.path.join(
            packagingDir, 'install_client.sh'))
        os.chmod(os.path.join(packagingDir, 'install_client.sh'), 0o755)

    if buildOption.get('PAGMODE') is not None and buildOption['PAGMODE'] == "lite":
        with open(os.path.join(packagingDir, 'install_client.sh')) as f_in, open(os.path.join(packagingDir, 'install_client_temp.sh'), 'w') as f_out:
            # 将 full 替换为 lite
            for line in f_in:
                f_out.write(line.replace('pagMode=full', 'pagMode=lite'))
        os.replace(os.path.join(packagingDir, 'install_client_temp.sh'), os.path.join(
            packagingDir, 'install_client.sh'))
        os.chmod(os.path.join(packagingDir, 'install_client.sh'), 0o755)
    logging.info("copy client_install.sh success")


# ========= different package type =========


# make package.tar.gz(bin, lib, inc, cfg, init.d) also known as taos.tar.gz
def packageTarGz(packageComponent, buildOptions, verMode, type):
    if type not in ['server', 'client']:
        raise Exception("unsupported package type:{0}".format(type))
    if verMode not in ['cluster', 'cloud', 'edge']:
        raise Exception("unsupported verMode:{0}".format(verMode))
    packageDirs = []

    global binFiles, headerFiles, installDir, tarName, packagingDir, cfgFiles
    if packageComponent['component']['package.tar.gz'].get('bin') is not None:
        packageDirs.append('bin')
        initBinFiles(packageComponent['component'], buildOptions,verMode)
    else:
        binFiles.clear()
        logging.warning(
            f"no bin files will be packed,since {tarName}.bin is not set")

    if packageComponent['component']['package.tar.gz'].get('inc') is not None:
        packageDirs.append('inc')
        initHeadFiles(packageComponent['component'])
    else:
        headerFiles.clear()
        logging.warning(
            f"no header files will be packed,since {tarName}.inc is not set")

    if packageComponent['component']['package.tar.gz'].get('cfg') is not None:
        packageDirs.append('cfg')
        initConfigFiles(packageComponent['component'])
    else:
        cfgFiles.clear()
        logging.warning(
            f"no cfg files will be packed,since {tarName}.cfg is not set")

    if packageComponent['component']['package.tar.gz'].get('init.d') is not None:
        packageDirs.append('init.d')
        initdFiles(packageComponent['component'])
    else:
        initFileDeb.clear()
        initFileRpm.clear()
        logging.warning(
            f"no init.d files will be packed,since {tarName}.init.d is not set")

    # make dirs
    makePackageDirs(packageDirs)

    # copy files into installDir
    for bin in binFiles:
        utilCopyFile(bin, os.path.join(packagingDir, 'bin'))

    for header in headerFiles:
        utilCopyFile(header, os.path.join(packagingDir, 'inc'))

    for bin in binFiles:
        utilCopyFile(bin, os.path.join(packagingDir, 'bin'))

    for cfg in cfgFiles:
        utilCopyFile(cfg, os.path.join(packagingDir, 'cfg'))

    for deb in initFileDeb:
        utilCopyFile(deb, os.path.join(packagingDir, 'init.d', 'taosd.deb'))

    for rpm in initFileRpm:
        utilCopyFile(rpm, os.path.join(packagingDir, 'init.d', 'taosd.rpm'))

    if packageComponent['component']['package.tar.gz'].get('jemalloc') is not None:
        copyJemllocFiles(packageComponent['component'], type='server')

    if type == 'server':
        initRemoveServerScript(verMode, buildOptions)
    else:
        initRemoveClientScript(verMode, buildOptions)

   # nginxd current no need

   # tar installDir to package.tar.gz
    os.chdir(packagingDir)

    logging.info(f"tar {packagingDir} to {tarName}")
    # raise Exception(f"{os.getcwd()}")
    executeCommand(f'tar -zcv -f {tarName} ./* --remove-files || :')

    os.chdir(scriptDir)


# make server or client tarball，may include（driver，examples，install.sh,package.tar.gz,connector,share)
def collectionTarball(packageComponent, buildOptions, verMode, type):
    logging.info("collecting tarball package content")
    # driver
    if packageComponent['component'].get('driver') is not None:
        initLibFiles(packageComponent['component'], buildOptions)
        # copy into *.sh, driver,example,share
        copyDriver(buildOptions)
    else:
        libFiles.clear()
        logging.warning(
            "no driver files will be packed,since ['component']['package.tar.gz']['driver'] is not set")

    # install.sh
    if packageComponent['component'].get('install.sh') is not None:
     
        copyInstallServerScript(packageComponent['component'], buildOptions, verMode)
    else:
        logging.warning(
            "no install.sh will be packed,since ['component']['install.sh'] is not set")
    
    # install_client.sh
    if packageComponent['component'].get('install_client.sh') is not None:
        copyInstallClientScript(packageComponent['component'], buildOptions, verMode)
    else:
        logging.warning(
            "no install_client.sh will be packed,since ['component']['install_client.sh'] is not set")
    
    # example folder
    if packageComponent['component'].get('examples') is not None:
        # copy examples
        copyExamples(packageComponent['component'], buildOptions)
    else:
        logging.warning(
            "no examples will be packed,since ['component']['examples'] is not set")

    # connectors
    if packageComponent['component'].get('connector') is not None:
        # copy connectors
        copyConnectors(packageComponent['component'])
    else:
        logging.warning(
            "no connectors will be packed,since ['component']['connector'] is not set")

    # share
    if packageComponent['component'].get('share') is not None:
        # copy share
        copyShares(packageComponent['component'])
    else:
        logging.warning(
            "no share will be packed,since ['component']['share'] is not set")

    logging.info("collecting tarball package content done")
    
def tarServer(packageComponent, buildOptions, verMode):
    logging.info("make server.tar.gz package")
    
    packageTarGz(packageComponent, buildOptions, verMode, 'server')
    collectionTarball(packageComponent, buildOptions, verMode, 'server')
    os.chdir(releaseDir)
    package = makePackageName(buildOptions, verMode, 'server')
    
    logging.info(f"rename packaging_dir:{packagingDir} to {os.path.join(releaseDir,package['name'])}")   
    os.rename(packagingDir,os.path.join(releaseDir,package['folder']))
    logging.debug(os.getcwd())

    executeCommand('tar -zcv -f {0}.tar.gz "$(basename {1})" --remove-files || :'.format(package['name'],os.path.join(releaseDir,package['folder'])))
    
    logging.info(f"packing {package['name']}.tar.gz at {releaseDir} done")
    os.chdir(scriptDir)

def tarClient(packageComponent, buildOptions, verMode):
    logging.info("make client.tar.gz package")
    
    packageTarGz(packageComponent, buildOptions, verMode, 'client')
    collectionTarball(packageComponent, buildOptions, verMode, 'client')
    os.chdir(releaseDir)
    package = makePackageName(buildOptions, verMode, 'client')
    
    logging.info(f"rename packaging_dir:{packagingDir} to {os.path.join(releaseDir,package['name'])}")   
    os.rename(packagingDir,os.path.join(releaseDir,package['folder']))
    logging.debug(os.getcwd())
   
    executeCommand('tar -zcv -f {0}.tar.gz "$(basename {1})" --remove-files || :'.format(package['name'],os.path.join(releaseDir,package['folder'])))
    logging.info("packing {0}.tar.gz at {1} done".format(package['name'],releaseDir))
    os.chdir(scriptDir)

def makeTDengineTarball(tarOptions, buildOptions, verMode):
    global packagingDir
    packagingDir = os.path.join(releaseDir, "packaging-{0}-server".format(verMode))

    logging.info(f"server package {packagingDir}.tar.gz will be packed")

    if tarOptions.get('server') is not None and tarOptions['server']['enable'] == True:
        tarServer(tarOptions['server'], buildOptions, verMode)
    else:
        logging.warning("no server package will be packed")
    
    if tarOptions.get('client') is not None and tarOptions['client']['enable'] == True:
        tarClient(tarOptions['client'], buildOptions, verMode)
    else:
        logging.warning("no client package will be packed")
    
    
    if os.path.exists(os.path.join(releaseDir, 'build-taoskeeper')):
        logging.info(f"remove build-taoskeeper in {releaseDir}")
        shutil.rmtree(os.path.join(releaseDir, 'build-taoskeeper'))

def makeServeRpm(buildOptions, verMode):
    output_dir = os.path.join(communityDir, 'rpms')
    ret = subprocess.call("command -v rpmbuild", shell=True,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if ret == 0:
        logging.info("packing for server.rpm")
        if buildOptions['OSTYPE'] != 'Darwin':
            if verMode == 'edge' and buildOptions['CPUTYPE'] == 'x64' and buildOptions['CUS_PROMPT'] == 'taos':
                if not os.path.exists(output_dir):
                    #     shutil.rmtree(output_dir)
                    #     os.makedirs(output_dir)
                    # else:
                    os.makedirs(output_dir)

                os.chdir(os.path.join(communityDir, 'packaging', 'rpm'))
                executeCommand(
                    f"bash makerpm.sh {compileDir} {output_dir} {buildOptions['VERNUMBER']} {buildOptions['CPUTYPE']} {buildOptions['OSTYPE']} {verMode} {buildOptions['VERTYPE']}")
                executeCommand(
                    f"cp {output_dir}/TDengine-server* {releaseDir}/")

                os.chdir(scriptDir)
                logging.info(f"packing server.rpm done")
            else:
                logging.error("not support Linux-x64-community rpm server")
        else:
            logging.info("not support rpm server on macos")
    else:
        logging.error("rpmbuild not found, please install rpmbuild")

def makeClientRpm():
    logging.info("not support rpm client")

def makeServerDeb(buildOptions, verMode):
    logging.info("make server.deb package")
    output_dir = os.path.join(communityDir, 'debs')
    if buildOptions['OSTYPE'] != 'Darwin':
        logging.debug(buildOptions)
        if verMode == 'edge' and buildOptions['CPUTYPE'] == 'x64' and buildOptions['CUS_PROMPT'] == 'taos':
            logging.info("deb package only support Linux-x64 server")
            ret = subprocess.call("command -v dpkg", shell=True,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if ret == 0:
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # else:
                #     shutil.rmtree(output_dir)
                #     os.makedirs(output_dir)
                os.chdir(os.path.join(communityDir, 'packaging', 'deb'))
                executeCommand(
                    f"bash makedeb.sh {compileDir} {output_dir} {buildOptions['VERNUMBER']} {buildOptions['CPUTYPE']} {buildOptions['OSTYPE']} {verMode} {buildOptions['VERTYPE']}")
                executeCommand(
                    f"cp {output_dir}/TDengine-server* {releaseDir}/")
                os.chdir(scriptDir)
                logging.info("make server.deb package done")
            else:
                logging.error("dpkg found, cannot make server.deb package")
        else:
            logging.error(
                "server.deb package only support Linux-x64-community server")

    else:
        logging.info("server.deb package only support MacOS")

def makeClientDeb():
    logging.info("not support deb client")

def makePackages(tdenginePackageOptions, buildOptions, verMode):
    logging.info("make TDengine package")
    if tdenginePackageOptions['tar.gz']['enable'] == True and tdenginePackageOptions.get('tar.gz') is not None:
        logging.info("make TDengine")
        makeTDengineTarball(tdenginePackageOptions['tar.gz'], buildOptions, verMode)
    else:
        logging.warning("no tar.gz package will be packed")
    
    if tdenginePackageOptions.get('rpm') is not None and tdenginePackageOptions['rpm']['enable'] == True:
        makeServeRpm(buildOptions, verMode)
    else:
        logging.warning("no rpm package will be packed")
    
    if tdenginePackageOptions.get('deb') is not None and tdenginePackageOptions['deb']['enable'] == True:
        makeServerDeb(buildOptions, verMode)    
    else:
        logging.warning("no deb package will be packed")
    

# ====================TaosTools====================
def collectTaosToolsTarContent(tarTaosToolsOptions, buildOptions, verMode):
    global taosToolsInstallDir, taosToolsBinFiles
    logging.info("packaing taosTools tarball:{} ".format(
        taosToolsInstallDir))

    # copy taosToolsBinFiles into taosToolsInstallDir
    if tarTaosToolsOptions.get('bin') is not None:
        initTaosToolsBinFiles(tarTaosToolsOptions)
        os.makedirs(os.path.join(taosToolsInstallDir, 'bin'),
                    exist_ok=True, mode=0o755)
        for bin in taosToolsBinFiles:
            utilCopyFile(bin, os.path.join(taosToolsInstallDir, 'bin'))
        logging.info("copy taosToolsBinFiles done")
    else:
        logging.warning("taosTools tarball will not include bin files")
    
    # copy install-tools.sh into taosToolsInstallDir
    if tarTaosToolsOptions.get('install-tools.sh') is not None and tarTaosToolsOptions['install-tools.sh'] == True:
        installToolsPath = os.path.join(
            communityDir, *('tools/taos-tools/packaging/tools/install-tools.sh').split('/'))

        if os.path.exists(installToolsPath):
            utilCopyFile(installToolsPath, os.path.join(
            taosToolsInstallDir, 'bin'))
            logging.info("copy install-tools.sh done")
        else:
            logging.error(f"{installToolsPath} not exist")
            raise Exception(f"{installToolsPath} not exist")
    else:
        logging.warning("taosTools tarball will not include install-tools.sh")
        
    # copy unintall-tools.sh into taosToolsInstallDir
    if tarTaosToolsOptions.get('uninstall-tools.sh') is not None and tarTaosToolsOptions['uninstall-tools.sh'] == True:
        uninstallToolsPath = os.path.join(
             communityDir, *('tools/taos-tools/packaging/tools/uninstall-tools.sh').split('/'))

        if os.path.exists(uninstallToolsPath):
            utilCopyFile(uninstallToolsPath, os.path.join(
            taosToolsInstallDir, 'bin'))
            logging.info("copy uninstall-tools.sh done")
        else:
            logging.error(f"{uninstallToolsPath} not exist")
            raise Exception(f"{uninstallToolsPath} not exist")
    else:
        logging.warning("taosTools tarball will not include uninstall-tools.sh")

   # copy libarvro.so 23.0.0 into taosToolsInstallDir
    if tarTaosToolsOptions.get('arvo') is not None:
        libDir = os.path.join(buildDir, 'lib')
        os.makedirs(os.path.join(taosToolsInstallDir, 'arvo',
                    'lib', 'pkgconfig'), exist_ok=True)
        if os.path.exists(os.path.join(libDir, 'libavro.so.23.0.0')):
            executeCommand(
            f'cp {libDir}/libavro.so.* {taosToolsInstallDir}/arvo/lib ')
            utilCopyFile(os.path.join(libDir, 'pkgconfig', 'avro-c.pc'),
                     os.path.join(taosToolsInstallDir, 'arvo', 'lib', 'pkgconfig'))
            logging.info("copy libavro.so.23.0.0 done")
        else:
            logging.warning(
                f"{os.path.join(libDir, 'libavro.so.23.0.0')} not exist,skipping copying into {taosToolsInstallDir}")
    else:
        logging.warning("taosTools tarball will not include arvo files")
        
def makeTaosToolsDeb(buildOptions, verMode):
    logging.info("make taosTools.deb package")
    output_dir = os.path.join(communityDir, 'debs')
    if buildOptions['OSTYPE'] != 'Darwin':
        if verMode == 'edge' and buildOptions['CPUTYPE'] == 'x64' and buildOptions['CUS_PROMPT'] == 'taos':
            ret = subprocess.call("command -v dpkg", shell=True,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if ret == 0:
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # else:
                #     shutil.rmtree(output_dir)
                #     os.makedirs(output_dir)
                toolsVersion = getTaosToolVersion()
                if len(toolsVersion) == 0:
                    toolsVersion = '0.1.0'
                os.chdir(os.path.join(communityDir, 'tools',
                         'taos-tools', 'packaging', 'deb'))
                executeCommand(
                    f"bash make-taos-tools-deb.sh {topDir} {compileDir} {output_dir} {toolsVersion} {buildOptions['CPUTYPE']} {buildOptions['OSTYPE']} {verMode} {buildOptions['VERTYPE']} {buildOptions['VERCOMPATIBLE']}")
                executeCommand(f"cp {output_dir}/taosTools-* {releaseDir}/")
                logging.info("make server.deb package done")
                os.chdir(scriptDir)
            else:
                logging.info("dpkg found, cannot make server.deb package")
        else:
            logging.error("taosTools.deb package only support Linux-x64")

    else:
        logging.info("taosTools.deb package only support Linux-x64")

def makeTaosToolsRpm(buildOptions, verMode):
    output_dir = os.path.join(communityDir, 'rpms')
    ret = subprocess.call("command -v rpmbuild", shell=True,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if ret == 0:
        logging.info("packing for taosTools.rpm")
        if buildOptions['OSTYPE'] != 'Darwin':
            if verMode == 'edge' and buildOptions['CPUTYPE'] == 'x64' and buildOptions['CUS_PROMPT'] == 'taos':
                toolsVersion = getTaosToolVersion()
                os.chdir(os.path.join(communityDir, 'tools',
                         'taos-tools', 'packaging', 'rpm'))
                if len(toolsVersion) == 0:
                    toolsVersion = '0.1.0'
                executeCommand(
                    f"bash make-taos-tools-rpm.sh {topDir} {compileDir} {output_dir} {toolsVersion} {buildOptions['CPUTYPE']} {buildOptions['OSTYPE']} {verMode} {buildOptions['VERTYPE']} {buildOptions['VERCOMPATIBLE']}")
                executeCommand(f"cp {output_dir}/taosTools-* {releaseDir}/")

                os.chdir(scriptDir)
                logging.info(f"packing taosTools.rpm done")
            else:
                logging.error("taosTools.rpm package only support Linux-x64")
        else:
            logging.info("not support rpm taosTools on macos")
    else:
        logging.error("rpmbuild not found, please install rpmbuild")

def makeTaosToolsTar(tarTaosToolsOptions, buildOptions, verMode):
    global taosToolsInstallDir
    
    if os.path.isdir(os.path.join(communityDir, 'tools', 'taos-tools', 'packaging', 'deb')):
        taosToolsVersion = getTaosToolVersion()
        if len(getTaosToolVersion()) == 0:
            taosToolsVersion = "0.1.0"
        taosToolsInstallDir = os.path.join(releaseDir, "{0}Tools-{1}".format(
            buildOptions['CUS_PROMPT'],
            taosToolsVersion
        ))
    else:
        print(os.path.isdir(os.path.join(communityDir,
                  'tools', 'taos-tools', 'packaging', 'deb')))
        taosToolsVersion = buildOptions['VERNUMBER']
        taosToolsInstallDir = os.path.join(releaseDir, "{0}Tools-{1}".format(
            buildOptions['CUS_PROMPT'],
            taosToolsVersion
        ))

    logging.info(
            f"taos-tools package under {taosToolsInstallDir} will be packed")

    os.makedirs(taosToolsInstallDir, exist_ok=True)
    collectTaosToolsTarContent(tarTaosToolsOptions, buildOptions, verMode)
    
    # tar taosTools.tar.gz  
    taosToolsPackage = makePackageName(buildOptions, verMode, 'taosTools')
    # tar taosTools package
    logging.info(
        "start tar taos-tools package:{0}".format(taosToolsPackage['name']))
    os.chdir(releaseDir)
    
    if buildOptions['OSTYPE'] != 'Darwin':
        executeCommand(
            'tar -zcv -f "$(basename {taosToolsPackageName}).tar.gz" "$(basename {taosToolsInstallDir})" --remove-files || :'.format(taosToolsPackageName=taosToolsPackage['name'], taosToolsInstallDir=taosToolsInstallDir))
    else:
        logging.info("not support tar taosTools on macos")
        
    logging.info("tar taos-tools package {0}.tar.gz done".format(taosToolsPackage['name']))
    os.chdir(scriptDir)

def makeTaosToolPackages(taosToolsOptions, buildOptions, verMode):
    logging.info("make taosTools package")
    if taosToolsOptions['tar.gz']['enable'] == True and taosToolsOptions.get('tar.gz') is not None:
        makeTaosToolsTar(taosToolsOptions['tar.gz'], buildOptions, verMode)
    else:
        logging.warning("no tar.gz package will be packed")
    
    if taosToolsOptions['rpm']['enable'] == True and taosToolsOptions.get('rpm') is not None:
        makeTaosToolsRpm(buildOptions, verMode)
    else:
        logging.warning("no rpm package will be packed")
        
    if taosToolsOptions['deb']['enable'] == True and taosToolsOptions.get('deb') is not None:
        makeTaosToolsDeb(buildOptions, verMode)
    else:
        logging.warning("no deb package will be packed")
    
    logging.info("make taosTools package done")