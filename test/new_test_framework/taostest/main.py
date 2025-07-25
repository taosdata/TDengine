import os
import signal
from typing import Union

from .frame import TaosTestFrame
from .dataclass import CmdOption
from .performance.perfor_basic import Env
from .util.sql import TDSql
from ..utils.log import tdLog

import json
import argparse
import sys
from dotenv import load_dotenv
import pathlib
from colorama import init


def get_dotenv_file() -> Union[None, str]:
    """
    Get .env file path and check the existence.
    If .env not exist, then create an empty one and return None else return the real path.
    """
    frame_setting_path = os.path.expanduser("~") + "/.taostest"
    frame_env_file = frame_setting_path + "/.env"
    if not os.path.exists(frame_env_file):
        os.mkdir(frame_setting_path)
        pathlib.Path(frame_env_file).touch()
        return None
    else:
        return frame_env_file


def params_get(pars):
    opts = CmdOption()
    opts.test_root = pars["test_root"] if "test_root" in pars else None
    os.environ["TEST_ROOT"] = opts.test_root
    opts.version = True if "version" in pars else False
    opts.init = True if "init" in pars else False
    opts.keep = True if "keep" in pars else False
    opts.reset = True if "reset" in pars else False
    opts.early_stop = True if "early_stop" in pars else False
    opts.env_init = True if "env_init" in pars else False
    opts.containers = True if "containers" in pars else False
    opts.swarm = True if "swarm" in pars else False
    opts.rm_containers = True if "rm_containers" in pars else False
    opts.taosd_valgrind = True if "taosd_valgrind" in pars else False
    opts.taosc_valgrind = True if "taosc_valgrind" in pars else False
    opts.clean = True if "clean" in pars else False
    opts.stop = True if "stop" in pars else False
    opts.disable_collection = True if "disable_collection" in pars else False
    opts.disable_data_collection = True if "disable_data_collection" in pars else False
    opts.sql_recording = True if "sql_recording" in pars else False
    if opts.sql_recording:
        os.environ[TDSql.taostest_enable_sql_recording_variable] = "TRUE"

    opts.fast_write_param_list = pars["fast_write_param_list"] if "fast_write_param_list" in pars else None
    opts.setup = pars["setup"] if "setup" in pars else None
    opts.destroy = pars["destroy"] if "destroy" in pars else None
    opts.server_pkg = pars["server_pkg"] if "server_pkg" in pars else None

    if opts.server_pkg is not None:
        if not os.path.exists(opts.server_pkg):
            tdLog.error("server-pkg {} not exist".format(opts.server_pkg))
            sys.exit(1)
    opts.client_pkg = pars["client_pkg"] if "client_pkg" in pars else None
    if opts.client_pkg is not None:
        if not os.path.exists(opts.client_pkg):
            tdLog.error("--client-pkg {} not exist".format(opts.client_pkg))
            sys.exit(1)
    opts.use = pars["use"] if "use" in pars else None
    opts.group_files = pars["group_file"] if "group_file" in pars else None
    opts.group_dirs = pars["group_dir"] if "group_dir" in pars else None
    opts.cases = pars["case"] if "case" in pars else None
    opts.log_level = pars["log_level"] if "log_level" in pars else None
    opts.source_dir = pars["source_dir"] if "source_dir" in pars else None
    opts.taostest_pkg = pars["taostest_pkg"] if "taostest_pkg" in pars else None

    if opts.taostest_pkg is not None:
        if not os.path.exists(opts.taostest_pkg):
            tdLog.error("taostest-pkg {} not exist".format(opts.taostest_pkg))
            sys.exit(1)
    opts.docker_network = pars["docker_network"] if "docker_network" in pars else None
    opts.cfg_file = pars["cfg_file"] if "cfg_file" in pars else None
    opts.case_param = pars["case_param"] if "case_param" in pars else None
    if "concurrency" in pars:
        if pars["concurrency"] < 1:
            tdLog.error("--concurrency can't less than 1")
            sys.exit(1)
        else:
            opts.concurrency = pars["concurrency"]
    if "mnode_count" in pars:
        if pars["mnode_count"]< 1:
            tdLog.error("--mnode-count can't less than 1")
            sys.exit(1)
        else:
            opts.mnode_count = pars["mnode_count"]
    else:
        opts.mnode_count = 1
    opts.uniform_dist = pars["uniform_dist"] if "uniform_dist" in pars else None
    opts.tag = pars["tag"] if "tag" in pars else None
    opts.prepare = pars["prepare"] if "prepare" in pars else None
    opts.tdcfg = pars["tdcfg"] if "tdcfg" in pars else None

    origin_cmds = [f"taostest {k} {v}" for k, v in pars.items()]
    tdLog.debug(origin_cmds)
    tdLog.debug(opts)
    str_cmds = ' '.join(origin_cmds)
    opts.cmds = str_cmds

    return opts

def check_opts(opts):
    # have one
    if not (opts.destroy or opts.use or opts.setup or opts.prepare):
        print("Must specify one option in:[ --destroy, --use, --setup, --prepare]")
        return False
    # have only one
    #if (opts.destroy and opts.use) or (opts.use and opts.setup) or (opts.destroy and opts.setup):
    #    print("Can only specify one environment option: --destroy or --use or --setup")
    #    return False
    # test groups and test cases can't run together.
    if (opts.group_dirs or opts.group_files) and opts.cases:
        print("--group-dir or --group-file can't be used together with --case")
        return False
    # rm_containers  option must use with --containters
    if opts.containers:
        print(" It will exec in containters ")
    if opts.rm_containers:
        if not (opts.rm_containers and opts.containers):
            print("--rm_containers must be used together with --containers")
            return False
    # --env_init must appear with --init
    if opts.env_init and not opts.init:
        print("--env_init must be used when using --init")
        return False

    if opts.cfg_file is not None:
        if not opts.cfg_file.startswith("/"):
            opts.cfg_file = opts.test_root + "/" + opts.cfg_file
        if not os.path.exists(opts.cfg_file):
            print("--cfg-file {} not exist".format(opts.cfg_file))
            return False
    return True



def ans_yes(ans):
    return ans in ['yes', 'y', '']


def confirm(text):
    print(text)
    ans = sys.stdin.readline()
    ans = ans.strip()
    return ans_yes(ans)


def init_test_root(test_root: str):
    create_test_root_sub_dirs(test_root)



def init_default_env():
    """when taostest --env_init, generate a default env yaml
    """
    print("init a env yaml file to ${testroot}/env ")

    test_root = os.environ["TEST_ROOT"]
    env = Env()
    taosd_env = env.set_taosd_env()
    taopy_env = env.set_taospy_env()
    taosBenchmark_env = env.set_taosBenchmark_env()
    env_dict = [taosd_env, taopy_env, taosBenchmark_env]
    env_dir = os.path.join(test_root, 'env')
    env.set_env_file(env_dict=env_dict, path=env_dir, filename="env_init.yaml")


def create_test_root_sub_dirs(test_root):
    name = os.path.join(test_root, "cases")
    print("create root dir of cases:", name)
    os.makedirs(name, exist_ok=True)
    name = os.path.join(test_root, "groups")
    print("create root dir of test group:", name)
    os.makedirs(name, exist_ok=True)
    name = os.path.join(test_root, "run")
    print("create root dir of run log:", name)
    os.makedirs(name, exist_ok=True)
    name = os.path.join(test_root, "env")
    print("create root dir of env settings:", name)
    os.makedirs(name, exist_ok=True)
    print("done!")


def main(params: dict):
    """
    1. check TEST_ROOT environment variable.
    2. parse command line arguments.
    3. construct TaosTestFrame object.
    4. start test TaosTestFrame.
    """
    tdLog.debug(f"Start taostest with params: {params}")
    opts = params_get(params)
    if opts.test_root is None:
        tdLog.error("Please set test_root")
        return 1

    if opts.version:
        tdLog.debug("0.1.5")
        return

    if opts.init:
        init_test_root(opts.test_root)
        if opts.env_init:
            init_default_env()
        return 0

    if not check_opts(opts):
        return 1

    taos_test = TaosTestFrame(opts)
    return taos_test.start()


def signal_handler(signum, frame):
    print("receive signal:", signum)
