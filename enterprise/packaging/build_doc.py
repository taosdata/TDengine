import os
import sys
import subprocess
import logging
import git  # 导入 GitPython
from git import RemoteProgress

# from prepare_env import EnvironmentPreparer

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 参数校验
if len(sys.argv) < 2:
    print("Usage: python3 build_doc.py <branchname>")
    sys.exit(1)

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
        self.executeCommand("curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash")
        self.executeCommand("export NVM_DIR=\"$HOME/.nvm\" && [ -s \"$NVM_DIR/nvm.sh\" ] && \. \"$NVM_DIR/nvm.sh\" && nvm install 23 && npm install yarn -g")
        logger.info("Node.js and yarn installation completed.")
    
    def prepare_build_doc_env(self):
        logger.info("Preparing build docs environment. Only supports Linux and macOS.")
        
        if sys.platform not in ['linux', 'darwin']:
            logger.info("Only supports Linux and macOS.")
            sys.exit(1)
        else:
            if self.is_command_exist("node"):
                logger.info("Node is already installed.")
            else:
                logger.info("Node is not installed.")
                self.install_node_nvm()


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


def build_doc(doc_repo_path):
    """
    使用 subprocess 模块调用 yarn build 命令构建文档
    """
    
    try:
        # 切换到指定的路径
        os.chdir(doc_repo_path)
        # 构建文档
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


def build_doc_zip(enterprise_path):
    """
    use doc.taosdata.com build result to generate doc zh zip
    """
    try:
        os.chdir(enterprise_path)
        print(f"Changed directory to {enterprise_path}")
        
        # Run the build script
        subprocess.run(["python3", "build.py", "enterprise", "zh+en", "zip"], check=True)
        
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
    try:
        # Open the repository
        repo = git.Repo(repo_path)
        logger.info(f"Opened Git repository at {repo_path}")

        # Checkout the specified branch
        repo.git.checkout(branch_name)
        repo.git.pull()   # 拉取最新代码

        logger.info(f"Checked out branch {branch_name}")
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
    # Prepare the environment for building the documentation
    preparer = EnvironmentPreparer()
    preparer.prepare_build_doc_env()
    
    # set  workdir 
    script_path = os.path.abspath(__file__)
    workdir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

    # workdir = "/root/enterprise_build_zip_work/"
    enterprise_doc_repo_path = f"{workdir}/enterprise-docs/"
    enterprise_doc_repo = "https://github.com/taosdata/enterprise-docs.git"
    doc_zh_repo_path = f"{workdir}/docs.taosdata.com/"
    doc_zh_repo = "https://github.com/taosdata/docs.taosdata.com.git"
    doc_en_repo_path = f"{workdir}/docs.tdengine.com/"
    doc_en_repo = "https://github.com/taosdata/docs.tdengine.com.git"
    TDengine_branch_name = sys.argv[1]
    TDengine_repo = "https://github.com/taosdata/tdengine.com.git"
    TDengine_repo_path = f"{workdir}/TDengine/"
    taos_tools_repo = "https://github.com/taosdata/taos-tools.git"
    taos_tools_repo_path = f"{workdir}/taos-tools/"
    
    # prepare docs repo
    prepare_repo(taos_tools_repo, taos_tools_repo_path, "main")
    prepare_repo(doc_zh_repo,doc_zh_repo_path, "master")
    prepare_repo(doc_en_repo, doc_en_repo_path, "main")
    prepare_repo(enterprise_doc_repo, enterprise_doc_repo_path, "main")
    prepare_repo(TDengine_repo, TDengine_repo_path, TDengine_branch_name)
    
    # # 切换到 TDengine_repo 仓库并切换到指定分支
    # checkout_branch(TDengine_repo_path, TDengine_branch_name)

    # 切换到 doc_zh_repo 仓库并构建文档
    # change to doc_zh_repo and build the documentation
    os.chdir(doc_zh_repo)
    build_doc(doc_zh_repo)

    # Change to doc_en_repo and build the documentation
    os.chdir(doc_en_repo)
    build_doc(doc_en_repo)
    # generate zip docs for enterprise
    build_doc_zip(enterprise_doc_repo)
     
    # generate pdf docs for enterprise
    build_doc_pdf(enterprise_doc_repo)

if __name__ == "__main__":
    main()
