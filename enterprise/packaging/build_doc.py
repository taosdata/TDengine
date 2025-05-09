"""
Utility script to build the documentation for the TDengine
you can run this script with the following command:
generate oem zip and pdf :python3 build_doc.py -sb -sp -ozf -fu -cn LCIotDB -cp lc 
generate all zip and pdf :python3 build_doc.py -ezf
generate all zip and pdf without preparing, building and uploading zip : python3 build_doc.py -ezf -nu -sp -sb
"""


import argparse
import logging
import os
import subprocess
import sys

import git  # 导入 GitPython
from git import RemoteProgress

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
td_is_update = True
no_upload_arm = "no_upload_arm"
skip_prepare_env = False

def init_build_info():
    parser = argparse.ArgumentParser(description="build docs script")
    # all parameters
    parser.add_argument('-b', '--branch_name', default="main", help='Branch name of the TDengine repository')
    # parser.add_argument('-ee', '--build_enterprise_en', action='store_true', help='Build the build_enterprise en zip packagea')
    # parser.add_argument('-ez', '--build_enterprise_zh', action='store_true', help='Build the build_enterprise zh zip packagea')
    parser.add_argument('-sp', '--skip_prepare_env', action='store_true', help='Skip prepare env of docs build')
    parser.add_argument('-sb', '--skip_build', action='store_true', help='Skip build docs')
    parser.add_argument('-ez', '--build_enterprise_zip', action='store_true', help='Build the build_enterprise zh and en zip package')
    parser.add_argument('-ezf', '--build_enterprise_pdf', action='store_true', help='Build the build_enterprise zh zip and pdf package')
    parser.add_argument('-oz', '--build_oem_zh', action='store_true', help='Build the build_enterprise zh zip')
    parser.add_argument('-ozf', '--build_oem_zh_pdf', action='store_true', help='Build the build_enterprise zh zip packagea and pdf')
    parser.add_argument('-cn', '--cus_name', help='customized name')   
    parser.add_argument('-cp', '--cus_prompt', help='customized prompt')
    parser.add_argument('-nu', '--no_upload_arm', action='store_true', help='build taosexplorer with docs zip')
    parser.add_argument('-fu', '--force_not_update', action='store_true', help='build taosexplorer with docs zip')

    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"Unknown args: {unknown_args}")
        sys.exit(1)
    logger.info(f"args: {args}")
    return args

class EnvironmentPreparer:

    def __init__(self):
        pass
    
    def is_command_exist(self, command):
        from shutil import which
        return which(command) is not None
    
    def executeCommand(self, command):
        logger.info(f"Executing command: {command}")
        try:
            subprocess.run(command, shell=True, check=True)
            logger.info(f"Command succeeded: {command}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {command}\nError: {e}")
    
    def install_node_nvm(self):
        logger.info("Installing NVM (Node Version Manager)...")
        self.executeCommand("echo $PATH")
        self.executeCommand("curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash")
        self.executeCommand("export NVM_DIR=\"$HOME/.nvm\" && [ -s \"$NVM_DIR/nvm.sh\" ] && \. \"$NVM_DIR/nvm.sh\" && nvm install 23 && npm install yarn -g")

        logger.info("Node.js and yarn installation completed.")
        
        # 获取 Node.js 的安装路径并更新 PATH
        node_path = os.path.expanduser("~/.nvm/versions/node/v23.3.0/bin")  # 根据实际版本号调整
        os.environ["PATH"] += os.pathsep + node_path
        logger.info(f"Updated PATH: {os.environ['PATH']}")

    def install_required_packages(self):
        logger.info("Installing required packages...")
        # install zh fonts and zip 
        self.executeCommand("apt install -y fonts-noto-cjk zip")
        # install required libraries for chrome-package
        libraries = "libatspi2.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libdrm2 libexpat1 libgbm1 libglib2.0-0 libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libudev1 libuuid1 libx11-6 libx11-xcb1 libxcb-dri3-0 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxkbcommon0 libxrandr2 libxrender1 libxshmfence1 libxss1 libxtst6"
        self.executeCommand(f"apt install -y {libraries}")
    
    def prepare_build_doc_env(self):
        logger.info("Preparing build docs environment. Only supports Linux and macOS.")
        
        if sys.platform not in ['linux', 'darwin']:
            logger.info("Only supports Linux and macOS.")
            sys.exit(1)
        else:
            if skip_prepare_env:
                logger.info("Skip prepare env of docs build")
            else:
                if self.is_command_exist("node"):
                    logger.info("Node is already installed.")
                else:
                    logger.info("Node is not installed.")
                    self.install_node_nvm()
                self.install_required_packages()

class CloneProgress(RemoteProgress):
    def update(self, op_code, cur_count, max_count=None, message=''):
        if message:
            print(message)  # 或使用 logger.info(message) 记录日志


def checkout_branch(path, branch_name):
    try:
        # 切换到指定的路径
        os.chdir(path)
        print(f"Changed directory to {path}")

        # 打开 Git 仓库
        repo = git.Repo(path)
        print(f"Opened Git repository at {path}")

        # 切换到指定的分支
        repo.git.checkout(branch_name)
        repo.git.pull()   # 拉取最新代码

        logger.info(f"Checked out branch {branch_name}")
    except Exception as e:
        logger.error(f"Error: {e}")

def yarn_install(doc_reo_path):
    try:
        os.chdir(doc_reo_path)
        subprocess.run("yarn install",shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")


def build_doc(doc_repo_path):
    """
    使用 subprocess 模块调用 yarn build 命令构建文档
    """
    
    try:
        # 切换到指定的路径
        os.chdir(doc_repo_path)
        # 构建文档
        subprocess.run(["yarn", "install"], check=True)
        subprocess.run(["yarn", "ass", "local"], check=True)
        subprocess.run(["yarn", "build"], check=True)
        logger.info("Built the documentation successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")


# yarn serve --port 1234
def pre_view(doc_repo_path):
    '''
    yarn serve --port 1234 for preview
    '''
    try:
        os.chdir(doc_repo_path)
        subprocess.run(["yarn", "serve", "--port", "1234"], check=True)
        logger.info("Preview started successfully on port 1234")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")

def build_enterprise_zh(enterprise_path):
    """
    use doc.taosdata.com build result to generate doc zh zip
    """
    try:
        yarn_install(f"{enterprise_path}/enterprise-docs-zh/")
        
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")          
        # Run the build script
        subprocess.run(f"python3 build.py enterprise  zh  nozip  {no_upload_arm}", shell=True, check=True)
        
        # # Check if the build was successful
        # if subprocess.run(["echo", "$?"], capture_output=True, text=True).stdout.strip() == "0":
        #     print("开始部署本地预览")
        #     subprocess.run(["./deploy.sh"], check=True)
        # else:
        #     print("没有更新，退出脚本")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")



def build_doc_zip(enterprise_path):
    """
    use doc.taosdata.com build result to generate doc zh zip
    """
    try:
        yarn_install(f"{enterprise_path}/enterprise-docs-en/")
        yarn_install(f"{enterprise_path}/enterprise-docs-zh/")
        
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")          
        # Run the build script
        subprocess.run(f"python3 build.py enterprise  zh+en  zip  {no_upload_arm}", shell=True, check=True)
        
        # # Check if the build was successful
        # if subprocess.run(["echo", "$?"], capture_output=True, text=True).stdout.strip() == "0":
        #     print("开始部署本地预览")
        #     subprocess.run(["./deploy.sh"], check=True)
        # else:
        #     print("没有更新，退出脚本")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")

def build_doc_pdf(enterprise_path):
    """
    use doc.taosdata.com build result to generate doc zh zip
    """
    try:
        yarn_install(f"{enterprise_path}/pdf-docs-zh/")
        yarn_install(f"{enterprise_path}/pdf-docs-en/")
        yarn_install(f"{enterprise_path}/docs-to-pdf/")
        
        subprocess.run("pip3 install pymupdf==1.24.8", shell=True, check=True)
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")
        
        # Run the build script
        subprocess.run(["python3", "build.py", "pdf", "zh"], check=True)
        
        # # Check if the build was successful
        # if subprocess.run(["echo", "$?"], capture_output=True, text=True).stdout.strip() == "0":
        #     print("开始部署本地预览")
        #     subprocess.run(["./deploy.sh"], check=True)
        # else:
        #     print("没有更新，退出脚本")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")

def build_oem_zip(enterprise_path, cus_name, cus_prompt):
    """
    use doc.taosdata.com build result to generate doc oem zip
    """
    try:
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")          
        # Run the build script
        subprocess.run(f"python3 build.py oem  {cus_name} {cus_prompt} {no_upload_arm} ", shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")

def build_oem_pdf(enterprise_path):
    try:
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")          
        # Run the build script
        subprocess.run("python3 build.py pdf oem", shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")

def clone_repo(repo_url, repo_path):
    """
    Clone the repository from the specified URL to the specified path
    """
    try:
        logger.info(f"Cloned repository from {repo_url} to {repo_path}")
        git.Repo.clone_from(repo_url, repo_path, progress=CloneProgress())
    except Exception as e:
        logger.error(f"Error: {e}")

def update_repo(repo_path, branch_name):
    """
    Update the repository at the specified path
    """
    global td_is_update
    try:
        # Open the repository
        repo = git.Repo(repo_path)
        logger.info(f"Opened Git repository at {repo_path}")

        # Checkout the specified branch
        repo.git.checkout(branch_name)
        output = repo.git.pull()   # 拉取最新代码
        td_is_update = "Already up to date." not in output
        logger.info(f"Checked out branch {branch_name} and td_is_update is {td_is_update}")
    except Exception as e:
        logger.error(f"Error: {e}")

def prepare_repo(repo_url, repo_path, branch_name):
    """
    Prepare the repository by cloning it if it doesn't exist, or updating it if it does
    """
    
    if os.path.exists(repo_path):
        if repo_url == "https://github.com/taosdata/tdengine.com.git" :
        # If the repository already exists, update it
            update_repo(repo_path, branch_name)
    else:
        # If the repository doesn't exist, clone it
        clone_repo(repo_url, repo_path)    

def main():
    args = init_build_info()

    global no_upload_arm, td_is_update, skip_prepare_env
    no_upload_arm = "no_upload_arm" if args.no_upload_arm else ""
    skip_prepare_env = args.skip_prepare_env


    logger.info(f"Building the documentation.... is upload to arm:{no_upload_arm}")
    # Prepare the environment for building the documentation
    preparer = EnvironmentPreparer()
    preparer.prepare_build_doc_env()



    # set  workdir 
    script_path = os.path.dirname(__file__)
    # script_file = os.path.abspath(__file__)
    workdir = os.path.abspath(os.path.join(script_path, "../../../"))

    # workdir = "/root/enterprise_build_zip_work/"
    enterprise_doc_repo_path = f"{workdir}/enterprise-docs/"
    enterprise_doc_repo = "https://github.com/taosdata/enterprise-docs.git"
    doc_zh_repo_path = f"{workdir}/docs.taosdata.com/"
    doc_zh_repo = "https://github.com/taosdata/docs.taosdata.com.git"
    doc_en_repo_path = f"{workdir}/docs.tdengine.com/"
    doc_en_repo = "https://github.com/taosdata/docs.tdengine.com.git"
    TDengine_branch_name = args.branch_name
    TDengine_repo = "https://github.com/taosdata/tdengine.com.git"
    TDengine_repo_path = f"{workdir}/TDengine/"
    
    # prepare docs repo
    prepare_repo(doc_zh_repo,doc_zh_repo_path, "master")
    prepare_repo(doc_en_repo, doc_en_repo_path, "main")
    prepare_repo(enterprise_doc_repo, enterprise_doc_repo_path, "main")
    prepare_repo(TDengine_repo, TDengine_repo_path, TDengine_branch_name)

    if args.force_not_update :
        td_is_update = False   
    else:
        td_is_update = td_is_update

    # # 切换到 TDengine_repo 仓库并切换到指定分支
    # checkout_branch(TDengine_repo_path, TDengine_branch_name)

    # define oem dict
    oem_dict = {
        "ProDB": "prodb",
        "LCIotDB": "lc",
        # 可以在这里添加更多的键值对
    }

    if td_is_update:
        # change to doc_zh/en_repo and build the documentation
        build_doc(doc_zh_repo_path)
        build_doc(doc_en_repo_path)

        # generate zh/en zip docs for enterprise
        build_doc_zip(enterprise_doc_repo_path)
        build_doc_pdf(enterprise_doc_repo_path)

        # generate oem zip docs for TDengine
        
        for cus_name, cus_prompt in oem_dict.items():
            build_oem_zip(enterprise_doc_repo_path, cus_name, cus_prompt)


    if args.build_enterprise_zip:
        if args.skip_build:
            logger.info("Skip build docs")
        else: 
            build_doc(doc_zh_repo_path)
            build_doc(doc_en_repo_path)        
        # generate zh/en zip docs for enterprise
        build_doc_zip(enterprise_doc_repo_path)

    if args.build_enterprise_pdf:
        if args.skip_build:
            logger.info("Skip build docs")
        else: 
            build_doc(doc_zh_repo_path)
            build_doc(doc_en_repo_path)
        build_doc_zip(enterprise_doc_repo_path)
        # generate pdf docs for enterprise
        build_doc_pdf(enterprise_doc_repo_path)
    
    if args.build_oem_zh:
        if args.skip_build:
            logger.info("Skip build docs")
        else: 
            build_doc(doc_zh_repo_path)
            build_doc(doc_en_repo_path)
        # generate oem zip docs for TDengine
        build_enterprise_zh(enterprise_doc_repo_path)
        build_oem_zip(enterprise_doc_repo_path, args.cus_name, args.cus_prompt)
    
    
    if args.build_oem_zh_pdf:
        if args.skip_build:
            logger.info("Skip build docs")
        else: 
            build_doc(doc_zh_repo_path)
            build_doc(doc_en_repo_path)
        # generate oem zip docs for TDengine
        build_enterprise_zh(enterprise_doc_repo_path)
        build_oem_zip(enterprise_doc_repo_path, args.cus_name, args.cus_prompt)

        # generate oem pdf docs for TDengine
        build_oem_pdf(enterprise_doc_repo_path)
    


if __name__ == "__main__":
    main()
