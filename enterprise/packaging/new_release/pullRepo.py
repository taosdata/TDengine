import os
import shutil
import subprocess
import argparse
import logging


current_dir = os.path.dirname(os.path.realpath(__file__))


def executeCommand(command):
    logging.info(">>>>>>>>>>> {0}".format(command))
    ret = subprocess.run(command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
    logging.info(ret.stdout)
    ret.check_returncode()
    logging.info(">>>>>>>>>>> {0} done".format(command))


def cloneRepo(repo, name, path, branch, depth):
    if not os.path.exists(os.path.join(path,name)):
        if depth == 0 or depth is None:
            executeCommand('git clone {0} -b {1}  {3}'.format(repo, branch, name))
        else:
            executeCommand('git clone {0} -b {1} --depth {2} {3}'.format(repo, branch, depth, name))
    else:
        logging.info("Repo {0} already exists".format(name))
        shutil.rmtree(os.path.join(path,name))
        if depth == 0 or depth is None:
            executeCommand('git clone {0} -b {1}  {3}'.format(repo, branch, name))
        else:
            executeCommand('git clone {0} -b {1} --depth {2} {3}'.format(repo, branch, depth, name))
    
    os.chdir(os.path.join(path,name))
    logging.info("Current path: {0}".format(os.getcwd()))
    
    executeCommand('git remote get-url origin')
    executeCommand('git branch')
    executeCommand('git log|head -n 5')
    os.chdir(current_dir)  

def checkoutTarget(repo, path, target):
    os.chdir(os.path.join(path))
    logging.info("os.path(path):{0}".format(os.path.join(path)))
    logging.info("Current path: {0}".format(os.getcwd()))
    
    result = subprocess.run("git remote get-url origin", stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
    if result.returncode != 0:
        raise Exception(f"current path{path} is not a git repo")
    else:
        if repo in result.stdout:
            logging.info(
                "Current path is a git repo and the repo is {0} ".format(repo) )
        else:
            raise Exception(f"current path{path} is not a git repo is :{result.stdout} instead of {repo}")

    git_command = ['git reset --hard',  'git fetch', 'git log|head -n 20',
                   'git checkout {0} -f'.format(target), 'git pull', 'git log|head -n 20']
#'git clean -fdx',
    for command in git_command:
        logging.debug(command)
        executeCommand(command)

    os.chdir(current_dir)


def parse_args():
    parser = argparse.ArgumentParser()

    # For the repo to be cloned
    parser.add_argument(
        "-r", "--repo", help="Git repository name, like taos/TDengine", required=True)
    parser.add_argument("-p", "--path", help="Repo path name",
                        required=True, default=".")
    parser.add_argument(
        "-n", "--name", help="Set a name or the repo folder ", required=False, default=None)

    # base on the repo have been clone need to checkout out the target branch or commit id
    parser.add_argument(
        "-t", "--target", help="Target branch,tag,commit id", required=True, default="none")

    # use to clone a new repo for given branch and depth. For further use
    parser.add_argument(
        "-b", "--branch", help="Target branch,tag,commit id", required=False, default="main")
    parser.add_argument(
        "-d", "--depth", help="Depth repo to clone", required=False, default="1")

    return parser.parse_args()


if __name__ == "__main__":
    global args
    args = parse_args()

    logging.basicConfig(level=logging.INFO)

    repo = 'https://github.com/{0}.git'.format(args.repo)
    logging.info("Repo: {0}".format(repo))

    if args.name is None:
        path = os.path.join(args.path)
        name = args.repo.split('/')[-1]
    else:
        path = os.path.join(args.path)
        name = args.name

    checkoutTarget(args.repo, path, args.target)

    logging.info("Path: {0}".format(args.path))
