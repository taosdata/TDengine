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
installDir = ''
cfgDir = ''

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
                    os.makedirs(os.path.join(baseDir, 'jemalloc', dir),exist_ok=True, mode=0o755)
                else:
                    os.makedirs(os.path.join(baseDir, 'jemalloc', dir),exist_ok=True)
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
            utilCopyFile(os.path.join(buildDir, 'lib', 'libjemalloc.a'),
                         os.path.join(baseDir, 'jemalloc', 'lib'))
            utilCopyFile(os.path.join(buildDir, 'lib', 'libjemalloc_pic.a'), os.path.join(
                baseDir, 'jemalloc', 'lib'))
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


def makePackageDirs(type):
    global installDir, clientInstallDir
    if type == 'server':
        os.makedirs(installDir, exist_ok=True)
        os.makedirs(os.path.join(installDir, 'bin'), exist_ok=True, mode=0o755)
        os.makedirs(os.path.join(installDir, 'cfg'), exist_ok=True)
        os.makedirs(os.path.join(installDir, 'inc'), exist_ok=True)
        os.makedirs(os.path.join(installDir, 'init.d'), exist_ok=True)
    elif type == 'client':
        os.makedirs(clientInstallDir, exist_ok=True)
        os.makedirs(os.path.join(clientInstallDir, 'bin'),
                    exist_ok=True, mode=0o755)
        os.makedirs(os.path.join(clientInstallDir, 'cfg'), exist_ok=True)
        os.makedirs(os.path.join(clientInstallDir, 'inc'), exist_ok=True)
    else:
        raise Exception("unknown type:{0}".format(type))


def makePackageName(buildOptions, verMode, type) -> str:
    if type == 'server':
        if verMode == 'cluster' and buildOptions['PAGMODE'] == 'full':
            # TDengine-enterprise-server-3.0.3.1.20230327-Linux-x64
            pkgName = '{0}-{1}-server-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'cluster' and buildOptions['PAGMODE'] == 'lite':
            pkgName = '{0}-{1}-server-{2}-{6}{3}-{4}-{5}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                                 buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'cloud' and buildOptions['PAGMODE'] == 'full':
            pkgName = '{0}-{1}-server-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'edge' and buildOptions['PAGMODE'] == 'full':
            pkgName = '{0}-server-{1}-{4}{2}-{3}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                         buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'edge' and buildOptions['PAGMODE'] == 'lite':
            pkgName = '{0}-server-{1}-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))

        else:
            logging.error("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
            raise Exception("unsupported verMOde:{0} or pagMode".format(
                verMode, buildOptions['PAGMODE']))
    elif type == 'client':
        if verMode == 'cluster' and buildOptions['PAGMODE'] == 'full':
            # TDengine-enterprise-client-3.0.3.1.20230327-Linux-x64
            pkgName = '{0}-{1}-client-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'cluster' and buildOptions['PAGMODE'] == 'lite':
            pkgName = '{0}-{1}-client-{2}-{3}-{6}{4}-{5}'.format(buildOptions['CUS_NAME'], 'enterprise', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                                 buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'cloud' and buildOptions['PAGMODE'] == 'full':
            pkgName = '{0}-{1}-client-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], 'cloud', buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'edge' and buildOptions['PAGMODE'] == 'full':
            pkgName = '{0}-client-{1}-{4}{2}-{3}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                         buildOptions['CPUTYPE'], '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
        elif verMode == 'edge' and buildOptions['PAGMODE'] == 'lite':
            pkgName = '{0}-client-{1}-{2}-{5}{3}-{4}'.format(buildOptions['CUS_NAME'], buildOptions['VERNUMBER'], buildOptions['OSTYPE'],
                                                             buildOptions['CPUTYPE'], 'Lite', '' if buildOptions['VERTYPE'] == 'stable' else '{}-'.format(buildOptions['VERTYPE']))
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
    else:
        logging.error("unsupported type:{0}".format(type))
        raise Exception("unsupported type:{0}".format(type))
    return pkgName


def copyDriver(packageComponent, buildOption, type):
    global installDir, clientInstallDir, libFiles
    logging.info("copying driver")

    if type == 'server':
        os.makedirs(os.path.join(installDir, 'driver'), exist_ok=True)
        tmpDir = installDir
    elif type == 'client':
        os.makedirs(os.path.join(clientInstallDir, 'driver'), exist_ok=True)
        tmpDir = clientInstallDir
    else:
        raise Exception("unsupported type:{0}".format(type))
    logging.debug(libFiles)
    for lib in libFiles:
        if lib == 'vercomp.txt':
            # write into vercomp.txt
            logging.info("writing vercomp.txt")
            with open(os.path.join(tmpDir, 'driver', 'vercomp.txt'), 'w') as f:
                f.write(buildOption['VERCOMPATIBLE'])
        else:
            utilCopyFile(os.path.join(lib),
                         os.path.join(tmpDir, 'driver'))


def copyExamples(packageComponent, buildOptions, type):
    logging.info("copying examples")
    global installDir, communityDir, taosToolsDir

    if type == 'server':
        tmpDir = installDir
    elif type == 'client':
        tmpDir = clientInstallDir
    else:
        raise Exception("unsupported type:{0}".format(type))

    if buildOptions['DBNAME'] == 'taos':
        dstExampleDir = os.path.join(tmpDir, 'examples')
        srcExampleDir = os.path.join(communityDir, 'examples')
        os.makedirs(dstExampleDir, exist_ok=True)
        if packageComponent.get('examples') == 1:
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

        if buildOptions['PAGMODE'] != 'lite' and buildOptions['CPUTYPE'] != 'aarch32':
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
            logging.info(
                "skip copying examples, since PAGMODE: is lite or CPUTYPE aarch32")
    else:
        logging.warning("skip copying examples, since DBNAME:{0} is not taos".format(
            buildOptions['DBNAME']))


def copyConnectors(packageComponent, buildOptions, verMode, type):
    gitRepos = {
        "go": "https://github.com/taosdata/driver-go",
        "python": "https://github.com/taosdata/taos-connector-python",
        "nodejs": "https://github.com/taosdata/taos-connector-node",
        "dotnet": "https://github.com/taosdata/taos-connector-dotnet",
        "rust": "https://github.com/taosdata/libtaos-rs"
    }
    global installDir, clientInstallDir
    logging.info("downloading connectors")

    if type == 'server':
        tmpDir = installDir
    elif type == 'client':
        tmpDir = clientInstallDir
    else:
        raise Exception("unsupported type:{0}".format(type))

    if verMode == 'cluster' and buildOptions['PAGMODE'] != 'lite' and buildOptions['CPUTYPE'] != 'aarch32':
        os.makedirs(os.path.join(tmpDir, 'connector'), exist_ok=True)
        for connector in packageComponent['connector']:
            url = gitRepos[connector]
            repoPath = os.path.join(tmpDir, 'connector', connector)
            logging.info("downloading {0} from {1}".format(connector, url))

            if not os.path.exists(repoPath):
                logging.info("cloning {0} to {1}".format(connector, repoPath))
                os.system(f"git clone --depth 1 {url} {repoPath}")
                shutil.rmtree(os.path.join(repoPath, '.git'))
            else:
                logging.info("skip cloning {0} to {1}, since it already exists".format(
                    connector, repoPath))
        logging.info("downloading connectors success")
    else:
        logging.info(
            "skip downloading connectors, since PAGMODE: is lite or CPUTYPE aarch32 or cluster mode is not enabled")


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
                with open(os.path.join(installDir, 'install.sh'),'a') as f_in:
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


def copyShares(verMode):
    logging.info("copying web files")
    if verMode in ['cluster', 'cloud']:
        if os.path.isdir(os.path.join(webDir, 'admin')):
            # os.makedirs(os.path.join(installDir, 'share'), exist_ok=True)
            os.makedirs(os.path.join(installDir, 'share',
                        'admin', 'images'), exist_ok=True)
            shutil.copytree(os.path.join(webDir, 'admin'), os.path.join(
                installDir, 'share'), symlinks=True, copy_function=shutil.copy2, dirs_exist_ok=True)
            utilCopyFile(os.path.join(webDir, 'png', 'taos.png'),
                         os.path.join(installDir, 'share', 'admin', 'images'))
            logging.info("copy web files success")
        else:
            logging.warning("fail to copy web files for enterprise release, since webDir:{0} is not exist".format(
                os.path.join(webDir, 'admin')))
    else:
        logging.info(
            "skip copying web files, since verMode:{0} is not cluster or cloud".format(verMode))


def initBinFiles(packageComponent, buildOptions):
    global binFiles
    binFiles.clear()
    logging.debug("packageComponent:{0}".format(packageComponent))
    if buildOptions['PAGMODE'] == 'lite':
        logging.info("todo: compatible with lite mode")
        for bin in packageComponent['package.tar.gz']['bin']:
            tmp = bin.split('/')
            baseDir = globals()[tmp[0]]
            tmp[0] = baseDir
            binFiles.append(os.path.join(*(tmp)))
    else:
        for bin in packageComponent['package.tar.gz']['bin']:
            tmp = bin.split('/')
            baseDir = globals()[tmp[0]]
            tmp[0] = baseDir
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
        taosToolsBinFiles.append(os.path.join(*(tmp)))
# ========= different package type =========


def tarServerLite(packageComponent):
    global binFiles
    for bin in packageComponent['package.tar.gz']['bin']:
        tmp = bin.split('/')
        baseDir = globals()[tmp[0]]
        tmp[0] = baseDir
        if tmp[-1] == 'taos':
            binFiles.append(os.path.join(installDir, *(tmp)))
        if tmp[-1] == 'taosd':
            binFiles.append(os.path.join(installDir, *(tmp)))
        if tmp[-1] == 'remove.sh':
            binFiles.append(os.path.join(installDir, *(tmp)))
        if tmp[-1] == 'startPre.sh':
            binFiles.append(os.path.join(installDir, *(tmp)))
        if tmp[-1] == 'taosBenchmark':
            binFiles.append(os.path.join(installDir, *(tmp)))

    cfgFiles = packageComponent['package.tar.gz']['cfg']
    incFiles = packageComponent['package.tar.gz']['inc']

    # install.sh
    logging.info("install.sh will be packed in this time release")


def tarClient(tarClientOptions, buildOptions, verMode):
    global clientInstallDir, tarName, releaseDir
    if buildOptions['OSTYPE'] != 'Darwin':
        # init files will be packed
        initBinFiles(tarClientOptions['component'], buildOptions)
        initLibFiles(tarClientOptions['component'], buildOptions)
        initHeadFiles(tarClientOptions['component'])
        initConfigFiles(tarClientOptions['component'])

        # mkdris
        makePackageDirs('client')

        # copy files into clientInstallDir
        for head in headerFiles:
            utilCopyFile(head, os.path.join(clientInstallDir, 'inc'))
        for bin in binFiles:
            utilCopyFile(bin, os.path.join(clientInstallDir, 'bin'))
        for cfg in cfgFiles:
            utilCopyFile(cfg, os.path.join(clientInstallDir, 'cfg'))

        copyJemllocFiles(tarClientOptions['component'], 'client')

        logging.info("Modify remove.sh for cluster base on buildoptions")
        if verMode == 'cluster':
            shutil.copyfile(os.path.join(clientInstallDir, 'bin', 'remove_client.sh'), os.path.join(
                clientInstallDir, 'bin', 'remove_client_tmp.sh'))
            with open(os.path.join(clientInstallDir, 'bin', 'remove_client_tmp.sh'), 'r') as f_in:
                file_data = f_in.read()
            file_data = file_data.replace('clientName2=\"taos\"', 'clientName2=\"{0}'.format(buildOptions['CUS_PROMPT']))
            file_data = file_data.replace('productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOptions['CUS_NAME']))
           
            with open(os.path.join(clientInstallDir, 'bin', 'remove_client.sh'), 'w') as f_out:
                f_out.write(file_data)
            
            os.chmod(os.path.join(clientInstallDir,
                         'bin', 'remove_client.sh'), 0o755)
            os.remove(os.path.join(clientInstallDir,
                         'bin', 'remove_client_tmp.sh'))
        
        logging.info(
            "taring package.tar.gz under {0}".format(clientInstallDir))
        os.chdir(clientInstallDir)
        # tar package.tar.gz
        if buildOptions['OSTYPE'] != 'Darwin':
            executeCommand(f"tar -zcv -f {tarName} * --remove-files || :")
            logging.info("tar package.tar.gz success")
        else:
            executeCommand(f"tar -zcv -f {tarName} *")
            shutil.move(f"{tarName}", "..")
            shutil.rmtree("./*")
            shutil.move(f"../{tarName}", ".")
            logging.info("tar package.tar.gz for MacOS success")
        os.chdir(scriptDir)

        # copy install_client.sh
        copyInstallScript(
            tarClientOptions['component'], buildOptions, verMode, type='client')
        # copy examples
        copyExamples(tarClientOptions['component'],
                     buildOptions, type='client')
        # copy driver
        copyDriver(tarClientOptions['component'], buildOptions, type='client')
        # copy connectors
        listOfComponents = tarClientOptions['component']
        if (listOfComponents.get('connector') == 1):
            copyConnectors(tarClientOptions['component'],
                           buildOptions, verMode, type='client')

        os.chdir(releaseDir)
        packageName = makePackageName(buildOptions, verMode, type='client')

        if buildOptions['OSTYPE'] != 'Darwin':
            logging.info("tar {0}/{1} ".format(releaseDir, packageName))
            executeCommand(
                f'tar -zcv -f "$(basename {packageName}).tar.gz" $(basename {clientInstallDir}) --remove-files || :')
            os.chdir(scriptDir)
            logging.info(
                "tar {0}/{1} success,".format(clientInstallDir, packageName))
        else:
            tmp_dir = os.path.join(clientInstallDir, 'tmp')
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
                shutil.make_archive(
                    os.path.join(tmp_dir, packageName),
                    'gztar',
                    root_dirname=os.path.basename(clientInstallDir)
                )
            shutil.move(os.path.join(tmp_dir, packageName),
                        os.path.join(releaseDir, packageName))
            shutil.rmtree(tmp_dir)
            logging.info(
                "tar {0}/{1} success,".format(clientInstallDir, packageName))
    else:
        logging.error("macOS is not supported")
        os.chdir(scriptDir)


def tarClientLite(packageComponent):
    return 1


def tarServer(packageComponent, buildOptions, verMode):
    downloadTDinsight()
    buildTaosKeeper(verMode, buildOptions['CPUTYPE'])
    global taosToolsBinFiles, binFiles, libFiles, headerFiles, installDir, tarName
    if packageComponent['component']['package.tar.gz'].get('bin') is not None:
        initBinFiles(packageComponent['component'], buildOptions)
        for bin in binFiles:
            utilCopyFile(bin, os.path.join(installDir, 'bin'))
    else:
        logging.warning(
            "no bin files will be packed,since package.tar.gz.bin is not set")

    initLibFiles(packageComponent['component'], buildOptions)
    initHeadFiles(packageComponent['component'])
    initConfigFiles(packageComponent['component'])
    initdFiles(packageComponent['component'])
    # make dirs
    makePackageDirs('server')
    # copy files into installDir
    for header in headerFiles:
        utilCopyFile(header, os.path.join(installDir, 'inc'))

    for bin in binFiles:
        utilCopyFile(bin, os.path.join(installDir, 'bin'))

    for cfg in cfgFiles:
        utilCopyFile(cfg, os.path.join(installDir, 'cfg'))

    for deb in initFileDeb:
        utilCopyFile(deb, os.path.join(installDir, 'init.d', 'taosd.deb'))

    for rpm in initFileRpm:
        utilCopyFile(rpm, os.path.join(installDir, 'init.d', 'taosd.rpm'))

    copyJemllocFiles(packageComponent['component'], type='server')

    if verMode == 'cluster':
        logging.info("appending remove_taoskeeper.sh into bin/remove.sh")
        with open(os.path.join(installDir, 'bin', 'remove.sh'), 'a') as f_out:
            with open(os.path.join(scriptDir,'remove_taoskeeper.sh'), 'r') as f_in:
                f_out.write(f_in.read())
        logging.info("appending remove_taoskeeper.sh into bin/remove.sh done")
        
        shutil.copyfile(os.path.join(installDir, 'bin', 'remove.sh'),
                        os.path.join(installDir, 'bin', 'remove_tmp.sh'))
        with open(os.path.join(installDir, 'bin', 'remove_tmp.sh'), 'r') as f_in:
            file_data = f_in.read()
        file_data = file_data.replace('verMode=edge', 'verMode=cluster')
        file_data = file_data.replace(
            'serverName2=\"taosd\"', 'clientName2=\"{0}d\"'.format(buildOptions['CUS_PROMPT']))
        file_data = file_data.replace(
            'clientName2=\"taos\"', 'clientName2=\"{0}\"'.format(buildOptions['CUS_PROMPT']))
        file_data = file_data.replace(
            'productName2=\"TDengine\"', 'productName2=\"{0}\"'.format(buildOptions['CUS_NAME']))

        with open(os.path.join(installDir, 'bin', 'remove.sh'), 'w') as f_out:
            f_out.write(file_data)
        os.chmod(os.path.join(installDir, 'bin', 'remove.sh'), 0o755)
        os.remove(os.path.join(installDir, 'bin', 'remove_tmp.sh'))
    elif verMode == 'cloud':
        with open(os.path.join(installDir, 'bin', 'remove.sh'), 'r') as f_in,\
                tempfile.NamedTemporaryFile(mode='w', delete=False) as f_out:
            for line in f_in:
                f_out.write(line.replace('verMode=edge', 'verMode=cluster'))
        os.replace(f_out.name, os.path.join(installDir, 'bin', 'remove.sh'))
        os.chmod(os.path.join(installDir, 'bin', 'remove.sh'), 0o755)

   # nginxd current no need

   # tar installDir to package.tar.gz
    os.chdir(installDir)

    logging.info(f"tar {installDir} to {tarName}")
    executeCommand(f'tar -zcv -f {tarName} * --remove-files || :')

    os.chdir(scriptDir)

    # copy install.sh into installDir/
    logging.info(f"copy install.sh to installDir")
    copyInstallScript(
        packageComponent['component'], buildOptions, verMode, type='server')

    # copy into *.sh, driver,example,share
    copyDriver(packageComponent['component'], buildOptions, type='server')

    # copty examples
    copyExamples(packageComponent['component'], buildOptions, type='server')

    # copty connectors
    listOfComponents = packageComponent['component']
    if (listOfComponents.get('connector') == 1):
        copyConnectors(packageComponent['component'],
                       buildOptions, verMode, type='server')

    copyShares(verMode)
    #
    os.chdir(releaseDir)
    packageName = makePackageName(buildOptions, verMode, 'server')

    if buildOptions['OSTYPE'] != 'Darwin':
        executeCommand(
            f'tar -zcv -f "$(basename {packageName}).tar.gz" $(basename {installDir}) --remove-files || :')
    else:
        executeCommand(
            f'tar -zcv -f "$(basename {packageName}).tar.gz" $(basename {installDir}) || :')
        executeCommand(f'rm -rf {installDir}')
        if os.path.isdir("build-taoskeeper"):
            shutil.rmtree("build-taoskeeper", ignore_errors=True)
    logging.info(f"packing {packageName} at {releaseDir} done")


def makeTarClient(tarClientOptions, buildOptions, verMode):
    global clientInstallDir

    if verMode == 'cluster':
        clientInstallDir = os.path.join(releaseDir, "{0}-enterprise-client-{1}".format(
            buildOptions['CUS_NAME'],
            buildOptions['VERNUMBER']
        ))
    else:
        clientInstallDir = os.path.join(releaseDir, "{0}-client-{1}".format(
            buildOptions['CUS_NAME'],
            buildOptions['VERNUMBER']
        ))

    if buildOptions['PAGMODE'] == 'lite':
        tarClientLite(tarClientOptions['client'])
        logging.info("tar lite client done")
    else:
        tarClient(tarClientOptions['client'], buildOptions, verMode)
        logging.info(f"packing client package {clientInstallDir}.tar.gz done")


def mkServerTar(tarServerOptions, buildOptions, verMode):
    global installDir
    installDir = os.path.join(releaseDir, "{0}{1}-server-{2}".format(
        buildOptions['CUS_NAME'],
        '-enterprise' if verMode == 'cluster' else '',
        buildOptions['VERNUMBER']
    ))

    logging.info(f"server package {installDir}.tar.gz will be packed")
    makePackageDirs('server')
    os.makedirs(os.path.join(installDir, 'init.d'), exist_ok=True)

    if buildOptions['PAGMODE'] == 'lite':
        tarServerLite(tarServerOptions['server'])
    else:
        tarServer(tarServerOptions['server'], buildOptions, verMode)
    
    if os.path.exists(os.path.join(releaseDir,'build-taoskeeper')):
        logging.info(f"remove build-taoskeeper in {releaseDir}")
        shutil.rmtree(os.path.join(releaseDir,'build-taoskeeper'))

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


def makeClientRpm(buildOptions):
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


def makeClientDeb(buildOptions):
    logging.info("not support deb client")


def tarTaosTools(tarTaosToolsOptions, buildOptions, verMode):
    global taosToolsInstallDir, taosToolsBinFiles
    downloadTDinsight()
    initTaosToolsBinFiles(tarTaosToolsOptions)
    logging.info("tar taosTools under taosToolsInstallDir:{} done".format(
        taosToolsInstallDir))

    # copy taosToolsBinFiles into taosToolsInstallDir
    os.makedirs(os.path.join(taosToolsInstallDir, 'bin'),
                exist_ok=True, mode=0o755)

    for bin in taosToolsBinFiles:
        utilCopyFile(bin, os.path.join(taosToolsInstallDir, 'bin'))
    logging.info("copy taosToolsBinFiles done")

    # copy install-tools.sh into taosToolsInstallDir

    installToolsPath = os.path.join(
        communityDir, *('tools/taos-tools/packaging/tools/install-tools.sh').split('/'))

    if os.path.exists(installToolsPath):
        utilCopyFile(installToolsPath, os.path.join(
            taosToolsInstallDir, 'bin'))
        logging.info("copy install-tools.sh done")
    else:
        logging.error(f"{installToolsPath} not exist")
        raise Exception(f"{installToolsPath} not exist")

    # copy unintall-tools.sh into taosToolsInstallDir

    uninstallToolsPath = os.path.join(
        communityDir, *('tools/taos-tools/packaging/tools/uninstall-tools.sh').split('/'))

    if os.path.exists(uninstallToolsPath):
        utilCopyFile(uninstallToolsPath, os.path.join(
            taosToolsInstallDir, 'bin'))
        logging.info("copy uninstall-tools.sh done")
    else:
        logging.error(f"{uninstallToolsPath} not exist")
        raise Exception(f"{uninstallToolsPath} not exist")

   # copy libarvro.so 23.0.0 into taosToolsInstallDir
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
            f"{os.path.join(libDir, 'libavro.so.23.0.0')} not exist,skipping copying into {taosToolsInstalldir}")

    taosToolsPackageName = makePackageName(buildOptions, verMode, 'taosTools')

    # tar taosTools package
    downloadTDinsight()
    logging.info(
        "start tar taos-tools package:{0}".format(taosToolsPackageName))
    os.chdir(releaseDir)
    if buildOptions['OSTYPE'] != 'Darwin':
        executeCommand(
            f'tar -zcv -f "$(basename {taosToolsPackageName}).tar.gz" "$(basename {taosToolsInstallDir})" --remove-files || :')
    else:
        executeCommand(
            f'tar -zcv -f "$(basename {taosToolsPackageName}).tar.gz" "$(basename {taosToolsInstallDir})" || :')
        executeCommand(f'rm -rf "$(basename {taosToolsInstallDir})" || :')
    logging.info(f"tar taos-tools package {taosToolsPackageName}.tar.gz done")
    os.chdir(scriptDir)


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
    if buildOptions['PAGMODE'] == 'lite':
        logging.info("Skip taos-tools packaging, since lite release mode")
    else:
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
        tarTaosTools(tarTaosToolsOptions, buildOptions, verMode)
        logging.info(f"taos-tools package {installDir}.tar.gz done")

    logging.info("deb client done")
