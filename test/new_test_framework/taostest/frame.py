import importlib
import os
import queue
import random
import sys
import threading
import time
import traceback
import platform
from datetime import datetime
from typing import Dict, Any, List, Tuple

from colorama import Fore
from .components.taosd import TaosD
from ..utils.log import tdLog


try:
    import taos
except:
    print(f"{Fore.YELLOW}Warning: TDengine Client may not properly installed. You will can't use python connector on this device.{Fore.RESET}")

from .clustermanager import EnvManager, ResourceManager
from .dataclass import CmdOption, ResultLog
from .casetag import CaseTag
from .logger import Logger
from .logger import ThreadLogger
from .tdcase import TDCase
from .util.file import dict2yaml, dict2file
from .util.file import read_yaml
from .util.common import TDCom
from .util.localcmd import LocalCmd

try:
    from .util.sql import TDSql
except NameError:
    pass


class TaosTestFrame:
    max_timeout_second_default = 100
    max_timeout_second_variable = "MAX_TIMEOUT_SECOND"

    def __init__(self, opts: CmdOption):
        self._opts: CmdOption = opts
        self._str_cmds = opts.cmds
        self._test_root: str = os.environ["TEST_ROOT"]
        self._case_root = None
        self._group_root = None
        self._env_dir: str = os.path.join(self._test_root, "env")
        self._tmp_dir = os.environ["TEST_ROOT"] + '/tmp'
        self._compose_files = None
        self._taopy_version: str = "3.0.7.1"
        self._run_test = self._opts.cases or self._opts.group_dirs or self._opts.group_files
        self._set_up_only: bool = self._opts.setup and not self._run_test
        self._run_log_dir, self._log_dir_name = self._get_run_log_dir()
        self._logger: Logger = None
        self._init_logger()
        self._env_setting: Dict[str, Any] = None
        self._env_spec_dnode_system: str = "linux"
        self._containers_setting: Dict[str, Any] = None
        self._frame_containers_name: str = TDCom.taostest_container_name
        self._frame_containers_real_name: str = self._frame_containers_name
        self._frame_containers_swarm_name: str = ""
        self._services_hosts = []
        # default network name
        self._network_name = TDCom.docker_network_name_default
        if self._opts.docker_network is not None:
            self._network_name = self._opts.docker_network
        if self._opts.swarm:
            # swarm mode
            self._frame_containers_real_name = self._frame_containers_name
        else:
            # container mode
            self._frame_containers_real_name = self._network_name + "_" + self._frame_containers_name
        if self._opts.taosd_valgrind or self._opts.taosc_valgrind or self._opts.fast_write_param_list is not None or self._opts.disable_data_collection:
            self._env_mgr = EnvManager(self._logger, self._run_log_dir, self._opts)
        else:
            self._env_mgr = EnvManager(self._logger, self._run_log_dir)
        self._res_mgr = ResourceManager(self._logger, self._env_dir)
        self._worker_threads: List[threading.Thread] = []  # 工作线程
        self._task_queue = queue.Queue()
        self._result_queue = queue.Queue()
        self._done_workers = queue.Queue()
        self._ret = 0  # 返回值
        self._local_host = platform.node() # get local host name

    def _generate_random_str(self, randomlength):
        random_str = ''
        base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
        length = len(base_str) - 1
        for i in range(randomlength):
            random_str += base_str[random.randint(0, length)]
        return random_str

    def _get_run_log_dir(self) -> Tuple[str, str]:
        """
        compute run log dir name
        """
        log_root = os.path.join(self._test_root, "run")
        if TDCom.taostest_log_dir_variable in os.environ:
            log_root = os.environ[TDCom.taostest_log_dir_variable]
        log_dir_name = ""
        if self._opts.tag:
            log_dir_name += self._opts.tag
        elif self._opts.destroy:
            log_dir_name += "destroy"
        elif self._set_up_only:
            log_dir_name += "setup"
        elif self._opts.cases:
            case_name = self._opts.cases[0]
            # if '/' in case_name:
            #     case_name = case_name.split('/')[-1]
            # elif '\\' in case_name:
            #     case_name = case_name.split('\\')[-1]
            log_dir_name += case_name
        elif self._opts.group_files:
            log_dir_name += self._opts.group_files[0].split(".")[0]
        elif self._opts.group_dirs:
            log_dir_name += self._opts.group_dirs[0]
        else:
            log_dir_name = log_dir_name[:-1]
        log_dir_name = log_dir_name + time.strftime("_%Y%m%d_%H%M%S") + "_" + self._generate_random_str(8)
        return os.path.join(log_root, log_dir_name), log_dir_name

    def _init_logger(self):
        """
        create logger
        """
        os.makedirs(self._run_log_dir)
        log_queue = queue.Queue()
        thread_logger = ThreadLogger(os.path.join(self._run_log_dir, TDCom.taostest_log_file_name), log_queue, self._opts.log_level)
        self._logger = Logger(log_queue)
        thread_logger.start()
        # self._logger = Logger(os.path.join(self._run_log_dir, "test.log"))
        self._logger.debug("run log dir is " + self._run_log_dir)

    def _check_swarm_env(self):
        # check swarm environment
        cmd = "docker swarm join-token manager"
        ret = os.system(cmd)
        if ret != 0:
            self._logger.error("This node is not a swarm manager.")
            return False
        return True

    def _setup_taosc_config(self, taosc_config_file="/etc/taos/taos.cfg"):
        # setup taosc config
        # get queryPolicy from env variable
        if TDCom.taostest_query_policy_variable in os.environ:
            query_policy = os.getenv(TDCom.taostest_query_policy_variable)
            ret = os.system(f"grep \"^queryPolicy\" {taosc_config_file}")
            if ret == 0:
                ret = os.system(f"sed -i \"s:^queryPolicy.*:queryPolicy {query_policy}:\" {taosc_config_file}")
            else:
                ret = os.system(f"sed -i \"\$a queryPolicy {query_policy}\" {taosc_config_file}")
            if ret != 0:
                self._logger.error("set query policy failed")
                return False
        return True

    def start_work(self):
        """
        The main work flow of test framework
        """
        if self._opts.containers or self._opts.swarm:
            if self._opts.swarm:
                if not self._check_swarm_env():
                    self._ret = 1
                    self._logger.error("docker swarm environment error")
                    return
            # run in containers
            if self._opts.setup:
                self._generate_containers_setting(self._opts.setup)
            elif self._opts.use:
                self._generate_containers_setting(self._opts.use)
            elif self._opts.destroy:
                self._generate_containers_setting(self._opts.destroy)
            else:
                pass

            if self._opts.rm_containers:
                self._ret = self._containers_down()
                if self._ret != 0:
                    return

            # time.sleep(1)

            if self._opts.setup:
                self._containers_down()
                self._ret = self._containers_up()
                if self._ret != 0:
                    return
                self._ret = self._deploy_test_frame_in_containers()
                if self._ret != 0:
                    return
                self._ret = self._deploy_taospy_in_containers()
                if self._ret != 0:
                    return
                cmds = self._get_cmds_from_opts()
                print(cmds)
                self._ret = self._exec_cmds_in_containers(cmds)
                if self._ret != 0:
                    return

            elif self._opts.use or self._opts.destroy or self._opts.keep or self._opts.reset:
                cmds = self._get_cmds_from_opts()
                print(cmds)
                self._ret = self._exec_cmds_in_containers(cmds)
                if self._ret != 0:
                    return
            else:
                pass
        else:
            need_setup = 0
            if self._opts.destroy:
                self._read_env_setting(self._opts.destroy)
                self._destroy()
                tdLog.info("----------Destroy done----------")
                return
            elif self._opts.prepare:
                self._res_mgr.prepare(self._opts.prepare)
                return
            elif self._opts.tdcfg:
                self._update_taosd_cfg()
                self._destroy()
                need_setup = 1
            elif self._opts.setup:
                self._read_env_setting(self._opts.setup)
                if self._opts.clean:
                   self._destroy()
                need_setup = 1
            elif self._opts.use:
                self._read_env_setting(self._opts.use)
                self._use()
            # initialize coredump environment, then start nodes
            if need_setup == 1:
                self._nodeInit()
                self._setup()
                tdLog.info("----------Deploy done----------")
            if not self._run_test:
                return
            self._write_env_to_log_dir()
            if self._opts.reset:
                self._reset()
            if not self._setup_taosc_config(os.path.join(self._run_log_dir, "taos.cfg")):
                self._ret = 1
                return
            if self._opts.cases:
                self._execute_cases()
            elif self._opts.group_dirs or self._opts.group_files:
                self._execute_groups()
            if self._ret != 0 and not self._opts.disable_collection:
                # collect coredump file, etc.
                self._collect()
                print("collected is completed")
            if not self._opts.keep:
                # print(self._opts.keep)
                print("start to destroy:test chr")
                self._destroy()

    def start(self):
        try:
            self.start_work()
        except Exception as e:
            self._logger.error("exception %s", str(e))
            traceback.print_exc()
            self._ret = 2
        if self._opts.containers and self._opts.setup and not self._opts.keep:
            self._containers_down()
        self._logger.debug("start.ret = %d", self._ret)
        self._logger.terminate("done")
        return self._ret

    def _update_taosd_cfg(self):
        self._logger.debug("update test environment")
        if not self._opts.setup:
            init_env_file = os.path.join(self._test_root, "env", "env_init.yaml")
            if os.path.exists(init_env_file):
                self._env_setting = read_yaml(init_env_file)
                self._env_mgr.updateCfg(self._opts.tdcfg, component="taosd", node="all", restart=0)
            else:
                self._logger.error("there is no init-env yaml file exists! please exec taostest --env-init first ! or specify a env yaml with --setup")
                sys.exit(1)
        else:
            setting_file = os.path.join(self._test_root, 'env', self._opts.setup)
            self._env_setting = read_yaml(setting_file)
            self._env_mgr.env_setting = self._env_setting
            self._env_mgr.updateCfg(self._opts.tdcfg, component="taosd", node="all", restart=0)
        self._env_mgr.set_initial_env(self._env_setting)

    def _read_env_setting(self, opt_value):
        if opt_value.startswith("/") or opt_value.startswith("."):
            setting_file = opt_value
        else:
            setting_file = os.path.join(self._test_root, 'env', opt_value)
        self._logger.debug(f"read env setting {setting_file}")
        self._env_setting = read_yaml(setting_file)
        self._env_mgr.set_initial_env(self._env_setting)

    def _get_node_id(self):
        cmds_stdout = os.popen("docker info|grep NodeID|awk '{print $NF}'")
        node_id = cmds_stdout.read().strip()
        self._logger.debug(f"swarm NodeID: {node_id}")
        return node_id

    def _generate_containers_setting(self, opt_value):
        # self.generate_entry_point()
        if opt_value.startswith("/") or opt_value.startswith("."):
            setting_file = opt_value
        else:
            setting_file = os.path.join(self._test_root, 'env', opt_value)
        _env_setting = read_yaml(setting_file)

        components_types = ["taosd", "taospy",
                            "taosAdapter", "taosBenchmark", "prometheus", "agent", "create_table_tool"]
        dist_services = []
        for component in _env_setting['settings']:
            if component["name"] in components_types:
                if (not self._opts.uniform_dist is None) and component["name"] == self._opts.uniform_dist:
                    dist_services.extend(component["fqdn"])
                if component["name"] == "taospy":
                    self._taopy_version = component["spec"]["client_version"]
                if component["name"] == "prometheus":
                    for host in component["fqdn"]:
                        if host not in self._services_hosts:
                            self._services_hosts.append(host)
                        else:
                            continue
                    try:
                        # if prometheus has client hostname
                        if component["spec"]["node_exporter"]:
                            for host in component["spec"]["node_exporter"]["fqdn"]:
                                if host not in self._services_hosts:
                                    self._services_hosts.append(host)
                        if component["spec"]["process_exporter"]:
                            # if prometheus has process_exporter hostname
                            for host in component["spec"]["process_exporter"]["fqdn"]:
                                if host not in self._services_hosts:
                                    self._services_hosts.append(host)
                        else:
                            pass
                    except KeyError as e:
                        pass
                else:
                    for host in component["fqdn"]:
                        if host not in self._services_hosts:
                            self._services_hosts.append(host)

        # swarm mode and uniform distribution
        swarm_nodes = []
        swarm_nodes_count = 1
        swarm_node_index = 0
        if self._opts.swarm and not self._opts.uniform_dist is None:
            # get docker nodes
            swarm_nodes = self._get_swarm_nodes()
            swarm_nodes_count = len(swarm_nodes)

        self._services_hosts.append(self._frame_containers_name)
        compose_setting = {"version": "3", "services": {}, 'networks': {}}
        for host in self._services_hosts:
            if self._opts.swarm:
                # swarm mode
                service_name = host
            else:
                # container mode
                service_name = self._network_name + "_" + host
            compose_setting["services"][service_name] = {}
            compose_setting["services"][service_name]["environment"] = {}
            compose_setting["services"][service_name]["volumes"] = []
            if self._opts.swarm:
                # swarm mode
                if service_name == self._frame_containers_real_name:
                    compose_setting["services"][service_name]["volumes"].append(
                        self._test_root + ":" + self._test_root
                    )
                    compose_setting["services"][service_name]["environment"]['TEST_ROOT'] = self._test_root
                # swarm mode does not map source code dir
                # swarm mode does not map TEST_ROOT dir to every service
            else:
                # container mode
                compose_setting["services"][service_name]["container_name"] = service_name
                compose_setting["services"][service_name]["privileged"] = True
                compose_setting["services"][service_name]["volumes"].append(
                    self._test_root + ":" + self._test_root
                )
                if self._opts.source_dir is not None:
                    compose_setting["services"][service_name]["volumes"].append(self._opts.source_dir + ":" + TDCom.container_tdinternal_source_directory)
                compose_setting["services"][service_name]["environment"]['TEST_ROOT'] = self._test_root
            # os.system("docker rm -f " + service_name)
            if TDCom.docker_image_name_variable in os.environ:
                compose_setting["services"][service_name]["image"] = os.getenv(TDCom.docker_image_name_variable)
            else:
                compose_setting["services"][service_name]["image"] = TDCom.docker_image_name_default
            compose_setting["services"][service_name]["hostname"] = host
            compose_setting["services"][service_name]["environment"]['TZ'] = 'Asia/Shanghai'
            if TDCom.package_server_host_variable in os.environ:
                compose_setting["services"][service_name]["environment"][TDCom.package_server_host_variable] = os.getenv(TDCom.package_server_host_variable)
            if TDCom.package_server_username_variable in os.environ:
                compose_setting["services"][service_name]["environment"][TDCom.package_server_username_variable] = os.getenv(TDCom.package_server_username_variable)
            if TDCom.package_server_password_variable in os.environ:
                compose_setting["services"][service_name]["environment"][TDCom.package_server_password_variable] = os.getenv(TDCom.package_server_password_variable)
            if TDCom.package_server_root_variable in os.environ:
                compose_setting["services"][service_name]["environment"][TDCom.package_server_root_variable] = os.getenv(TDCom.package_server_root_variable)
            compose_setting["services"][service_name]["networks"] = [self._network_name]
            compose_setting["services"][service_name]["entrypoint"] = "sh -c \"nohup /usr/sbin/sshd >/home/sshd.log 2>&1 & while [ 1 ]; do sleep 10000; done\""
            if self._opts.swarm and (not self._opts.uniform_dist is None) and host in dist_services:
                # swarm mode
                # bind frame container to host
                if swarm_node_index >= swarm_nodes_count:
                    swarm_node_index = 0
                compose_setting["services"][service_name]["deploy"] = {}
                compose_setting["services"][service_name]["deploy"]["placement"] = {}
                self._logger.debug(f"current node index: {swarm_node_index}")
                compose_setting["services"][service_name]["deploy"]["placement"]["constraints"] = ["node.id == " + swarm_nodes[swarm_node_index]]
                swarm_node_index = swarm_node_index + 1

        if self._opts.swarm:
            # swarm mode
            # bind frame container to host
            compose_setting["services"][self._frame_containers_real_name]["deploy"] = {}
            compose_setting["services"][self._frame_containers_real_name]["deploy"]["placement"] = {}
            compose_setting["services"][self._frame_containers_real_name]["deploy"]["placement"]["constraints"] = ["node.id == " + self._get_node_id()]
        if self._opts.server_pkg is not None:
            compose_setting["services"][self._frame_containers_real_name]["volumes"].append(self._opts.server_pkg + ":" + self._opts.server_pkg)
        if self._opts.client_pkg is not None:
            compose_setting["services"][self._frame_containers_real_name]["volumes"].append(self._opts.client_pkg + ":" + self._opts.client_pkg)
        if self._opts.taostest_pkg is not None:
            compose_setting["services"][self._frame_containers_real_name]["volumes"].append(self._opts.taostest_pkg + ":" + self._opts.taostest_pkg)
        if self._opts.cfg_file is not None:
            compose_setting["services"][self._frame_containers_real_name]["volumes"].append(self._opts.cfg_file + ":" + self._opts.cfg_file)
        if TDCom.taostest_install_url_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDCom.taostest_install_url_variable] = os.getenv(TDCom.taostest_install_url_variable)
        if TDCom.taostest_log_dir_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["volumes"].append(os.getenv(TDCom.taostest_log_dir_variable) + ":" + os.getenv(TDCom.taostest_log_dir_variable))
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDCom.taostest_log_dir_variable] = os.getenv(TDCom.taostest_log_dir_variable)
        if TDSql.taostest_enable_sql_recording_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDSql.taostest_enable_sql_recording_variable] = os.getenv(TDSql.taostest_enable_sql_recording_variable)
        if TDCom.taostest_database_replicas_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDCom.taostest_database_replicas_variable] = os.getenv(TDCom.taostest_database_replicas_variable)
        if TDCom.taostest_database_cachemodel_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDCom.taostest_database_cachemodel_variable] = os.getenv(TDCom.taostest_database_cachemodel_variable)
        if TDCom.taostest_query_policy_variable in os.environ:
            compose_setting["services"][self._frame_containers_real_name]["environment"][TDCom.taostest_query_policy_variable] = os.getenv(TDCom.taostest_query_policy_variable)
        compose_setting['networks'] = {}
        compose_setting['networks'][self._network_name] = {}
        self._containers_setting = compose_setting
        self._compose_files = self._run_log_dir + "/" + TDCom.docker_compose_file_name
        if os.path.exists(self._compose_files):
            os.remove(self._compose_files)
        dict2yaml(self._containers_setting, self._run_log_dir, TDCom.docker_compose_file_name)

    def _write_env_to_log_dir(self):
        """
        Write initial environment setting to run log dir.
        """
        dict2yaml(self._env_setting, self._run_log_dir, "setting.yaml")

    def _destroy(self):
        self._logger.debug("destroy test environment")
        for node in self._env_setting["settings"]:
            self._env_mgr.destroyNode(node)

    def _setupMNodes(self):
        needSetup = False
        taosd_nodes = []
        for node in self._env_setting["settings"]:
            if node["name"] == "taosd":
                if (not node["spec"] is None) and (not node["spec"]["dnodes"] is None):
                    taosd_nodes = node["spec"]["dnodes"]
                    if (not node["spec"]["config"] is None) and (not node["spec"]["config"]["firstEP"] is None):
                        host = node["spec"]["config"]["firstEP"].split(":")[0]
                        port = node["spec"]["config"]["firstEP"].split(":")[1]
                        self._logger.debug("{} : {}".format(host, port))
                        if len(taosd_nodes) > 2:
                            needSetup = True
        # if len(taosd_nodes) > 0:
        #     # create qnode on dnode
        #     # for i in range(1, len(taosd_nodes) + 1):
        #     # for i in range(1, 1):
        #     #     self._logger.debug(f"taos -h {host} -P {port} -s \"create qnode on dnode {i}\"")
        #     #     ret = os.system(f"taos -h {host} -P {port} -s \"create qnode on dnode {i}\"")
        #     #     if ret != 0:
        #     #         self._logger.error(f"create qnode on dnode {i} failed")
        #     #         return False
        #     # show qnode
        #     self._logger.debug(f"taos -h {host} -P {port} -s \"show qnodes\"")
        #     cmd_stdout = os.popen(f"taos -h {host} -P {port} -s \"show qnodes\"")
        #     stdout = cmd_stdout.read().strip()
        #     self._logger.debug(stdout)
        #     lines = stdout.split("\n")
        # if len(taosd_nodes) > 0:
        #     # create snode on dnode
        #     for i in range(1, len(taosd_nodes) + 1):
        #         self._logger.debug(f"taos -h {host} -P {port} -s \"create snode on dnode {i}\"")
        #         ret = os.system(f"taos -h {host} -P {port} -s \"create snode on dnode {i}\"")
        #         if ret != 0:
        #             self._logger.error(f"create snode on dnode {i} failed")
        #             return False
        #     # show snode
        #     self._logger.debug(f"taos -h {host} -P {port} -s \"show snodes\"")
        #     cmd_stdout = os.popen(f"taos -h {host} -P {port} -s \"show snodes\"")
        #     stdout = cmd_stdout.read().strip()
        #     self._logger.debug(stdout)
        #     lines = stdout.split("\n")
        #if needSetup:
            # create mnode on dnode
            # for i in range(2, self._opts.mnode_count + 1):
            #     self._logger.debug(f"taos -h {host} -P {port} -s \"create mnode on dnode {i}\"")
            #     ret = os.system(f"taos -h {host} -P {port} -s \"create mnode on dnode {i}\"")
            #     if ret != 0:
            #         self._logger.error(f"create mnode on dnode {i} failed")
            #         return False
            # # show mnode
            # self._logger.debug(f"taos -h {host} -P {port} -s \"show mnodes\"")
            # cmd_stdout = os.popen(f"taos -h {host} -P {port} -s \"show mnodes\"")
            # stdout = cmd_stdout.read().strip()
            # self._logger.debug(stdout)
            # lines = stdout.split("\n")
            ## taos -s "show mnodes"
            # Welcome to the TDengine shell from Linux, Client Version:3.0.0.100
            # Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.
            #
            # taos> show mnodes
            #     id      |            endpoint            |     role     |  status   |       create_time       |
            # ====================================================================================================
            #           1 | dnode_1:6030                   | leader       | ready     | 2022-07-19 14:15:10.377 |
            #           2 | dnode_2:6030                   | follower     | ready     | 2022-07-19 14:15:21.898 |
            #           3 | dnode_3:6030                   | follower     | ready     | 2022-07-19 14:15:27.931 |
            # Query OK, 3 rows affected (0.008276s)
            # ret = 1
            # for line in lines:
            #     if line.find("Query OK") >= 0:
            #         ret = 0
            # if ret != 0:
            #     self._logger.error("show mnodes failed")
            #     return False
        return True

    def _setup(self):
        self._logger.debug("setup new test environment")
        # print("_setup_ ", self._env_setting)
        #for node in self._env_setting["settings"]:
        #    # print ("_setup_for_node", node)
        #    node["server_pkg"] = self._opts.server_pkg
        #    node["client_pkg"] = self._opts.client_pkg
        #    self._env_mgr.setupNode(node)
        for node in self._env_setting["settings"]:
            self._env_mgr.launchNode(node)
        self._setupMNodes()

    def _init_taostest(self):
        '''
        1. read /proc/sys/kernel/core_pattern and create dir
        2. mkdir -p $(( cat /proc/sys/kernel/core_pattern ))
        3. rm -rf coredump files
        4. don't support windows
        '''


        # host = platform.node()
        # for nodeDict in  self._env_setting["settings"]:
        #     if( "dnodes" in  nodeDict["spec"].keys() ):
        #         nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
        #         for dnodeList in nodeDictList:
        #             for dnode in dnodeList:
        #                 self._env_spec_dnode_system = dnode["system"] if "system" in dnode.keys() else "linux"
        #                 if self._env_spec_dnode_system != "windows":
        #                     corePattern = self._env_mgr._remote.cmd(host, ["cat " + TDCom.system_core_pattern_file])
        #                     if not corePattern is None:
        #                         self._logger.info("core pattern: {}".format(corePattern))
        #                         dirName = os.path.dirname(corePattern)
        #                         if dirName.startswith("/"):
        #                             self._env_mgr._remote.cmd(host, ["mkdir -p {} ".format(dirName)])
        #                 else:
        #                     pass
        if platform.system() != "Linux":
            self._logger.info("Skip core_pattern setup: not Linux")
            return
        
        host = platform.node()
        if host == self._local_host or host == "localhost":
            corePattern = self._env_mgr._remote.cmd(host, ["cat " + TDCom.system_core_pattern_file])
            if corePattern is not None:
                self._logger.debug("core pattern: {}".format(corePattern))
                dirName = os.path.dirname(corePattern)
                if dirName.startswith("/"):
                    self._env_mgr._remote.cmd(host, ["mkdir -p {} ".format(dirName)])
        else:
            for nodeDict in self._env_setting["settings"]:
                if "dnodes" in nodeDict["spec"].keys():
                    nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
                    for dnodeList in nodeDictList:
                        for dnode in dnodeList:
                            self._env_spec_dnode_system = dnode["system"] if "system" in dnode.keys() else "linux"
                            if self._env_spec_dnode_system != "windows":
                                corePattern = self._env_mgr._remote.cmd(host, ["cat " + TDCom.system_core_pattern_file])
                                if corePattern is not None:
                                    self._logger.debug("core pattern: {}".format(corePattern))
                                    dirName = os.path.dirname(corePattern)
                                    if dirName.startswith("/"):
                                        self._env_mgr._remote.cmd(host, ["mkdir -p {} ".format(dirName)])
                            else:
                                pass

    def _nodeInit(self):
        self._logger.debug("setup init test environment")
        for node in self._env_setting["settings"]:
            self._env_mgr.initNode(node)
        self._init_taostest()

    def _collect_taostest(self):
        if platform.system() != "Linux":
            self._logger.info("Skip core_pattern setup: not Linux")
            return
        
        host = platform.node()
        self._logger.debug("collect coredump files {}, etc.".format(host))
        logDir = self._run_log_dir
        coreDir = "{}/data/{}/coredump".format(logDir, host)
        os.system("mkdir -p {}".format(coreDir))
        corePattern = self._env_mgr._remote.cmd(host, ["cat " + TDCom.system_core_pattern_file])
        cmd = ""
        if not corePattern is None:
            dirName = os.path.dirname(corePattern)
            if dirName.startswith("/"):
                cmd = "cp -rf {} {}".format(dirName, coreDir)
                self._logger.debug("CMD: {}".format(cmd))
                os.system(cmd)
        # default data dir & log dir
        remoteDataDir = "/var/lib/taos"
        remoteLogDir = "/var/log/taos"
        # TODO: change log dir to configured log dir
        localLogDir = "{}/data/{}/log".format(logDir, host)
        os.system("mkdir -p {}".format(localLogDir))
        localDataDir = "{}/data/{}/data".format(logDir, host)
        os.system("mkdir -p {}".format(localDataDir))
        cmd = "cp -rf {} {}".format(remoteLogDir, localLogDir)
        self._logger.info("CMD: {}".format(cmd))
        os.system(cmd)
        if not self._opts.disable_data_collection:
            cmd = "cp -rf {} {}".format(remoteDataDir, localDataDir)
            self._logger.debug("CMD: {}".format(cmd))
            os.system(cmd)
        # TODO: backup server pkg and client pkg

    def _collect(self):
        self._logger.debug("collect coredump files, etc.")
        for node in self._env_setting["settings"]:
            self._env_mgr.collectNodeData(node, self._run_log_dir)
        self._collect_taostest()

    def _use(self):
        """
        When use an existing environment, only need to generate taos.cfg in run log dir.
        """
        self._logger.debug("use existing test environment")
        for node in self._env_setting["settings"]:
            if node["name"] == "taospy":
                config_dir = self._run_log_dir
                if "config_dir" in node["spec"]:
                    config_dir = os.path.join(self._run_log_dir, node["spec"]["config_dir"])
                dict2file(config_dir, "taos.cfg", node["spec"]["config"])

    def _reset(self):
        self._logger.debug("reset test environment")
        for node in self._env_setting["settings"]:
            self._env_mgr.resetNode(node)

    def generate_entry_point(self):
        entry_point_files = self._run_log_dir + "/" + TDCom.entry_point_file_name
        shell_cmds = [
            "nohup /usr/sbin/sshd >/home/sshd.log 2>&1 &",
            " while [ 1 ]; do sleep 1000; done "
        ]
        if os.path.exists(entry_point_files):
            os.remove(entry_point_files)
        with open(entry_point_files, "a+") as f:
            for shell_cmd in shell_cmds:
                f.write(shell_cmd + "\n")
            f.close()

    def run_cmd(self, cmd):

        code = os.system(cmd)
        if code != 0:
            print("{} exec failed ,code is {}".format(cmd, code))
            return 1
        return 0

    def _get_max_timeout_second(self):
        max_timeout_second = TaosTestFrame.max_timeout_second_default
        if TaosTestFrame.max_timeout_second_variable in os.environ:
            max_timeout_second_string = os.getenv(TaosTestFrame.max_timeout_second_variable)
            max_timeout_second = int(max_timeout_second_string)
        return max_timeout_second

    def _check_stack_status(self):
        # check stack status
        cmd = f"docker stack ps {self._network_name} --format \"{{{{.CurrentState}}}} {{{{.Name}}}}\"|grep \"^Running\""
        cmd_stdout = os.popen(cmd)
        stdout = cmd_stdout.read().strip()
        self._logger.debug(stdout)
        lines = stdout.split("\n")
        if len(lines) == len(self._services_hosts):
            return True
        return False

    def _get_swarm_nodes(self):
        cmd = f"docker node ls --format \"{{{{.ID}}}}\""
        cmd_stdout = os.popen(cmd)
        stdout = cmd_stdout.read().strip()
        self._logger.debug(stdout)
        nodes = stdout.split("\n")
        self._logger.debug(str(nodes))
        return nodes

    def _wait_until_stack_ready(self):
        count = 0
        check_ret = False
        sleep_interval = 3
        while count < self._get_max_timeout_second():
            time.sleep(sleep_interval)
            count = count + sleep_interval
            check_ret = self._check_stack_status()
            if check_ret:
                break
        return check_ret

    def _containers_up(self):

        compose_file = self._compose_files
        cmds = ""
        ret = 0
        # start containers
        if self._opts.swarm:
            # swarm mode
            cmds = f"docker stack deploy -c {compose_file} {self._network_name}"
            ret = self.run_cmd(cmds)
            if ret != 0:
                return ret
            # check stack status
            if not self._wait_until_stack_ready():
                self._logger.error("wait stack ready timeout, MAX_TIMEOUT_SECOND: {}".format(self._get_max_timeout_second()))
                return 1
        else:
            # container mode
            cmds = f"docker-compose -f {compose_file} --project-name {self._network_name} up -d --force-recreate"
            ret = self.run_cmd(cmds)
        return ret

    def _containers_down(self):
        compose_file = self._compose_files
        cmds = ""
        ret = 0
        # down containers
        if self._opts.swarm:
            # swarm mode
            cmds = f"docker stack rm {self._network_name}"
            ret = self.run_cmd(cmds)
            if ret == 0:
                # check service status
                count = 0
                check_ret = 0
                sleep_interval = 3
                while count < self._get_max_timeout_second():
                    time.sleep(sleep_interval)
                    count = count + sleep_interval
                    # make sure network has been removed
                    check_ret = os.system(f"docker network inspect {self._network_name}_{self._network_name} >/dev/null")
                    if check_ret != 0:
                        break
                if check_ret == 0:
                    self._logger.error(f"failed to remove docker network {self._network_name}_{self._network_name}")
                    self._logger.error("remove docker network timeout, MAX_TIMEOUT_SECOND: {}".format(self._get_max_timeout_second()))
                    ret = 1
        else:
            # container mode
            cmds = f"docker-compose -f {compose_file} --project-name {self._network_name} down --remove-orphans"
            ret = self.run_cmd(cmds)
        return ret

    def _get_frame_container_name(self):
        # down containers
        if self._opts.swarm:
            # swarm mode
            if self._frame_containers_swarm_name == "":
                cmds = "docker ps --format \"{{{{.Names}}}}\" --filter \"label=com.docker.swarm.service.name={}_{}\"".format(self._network_name, self._frame_containers_real_name)
                # execute command and get output
                cmds_stdout = os.popen(cmds)
                self._frame_containers_swarm_name = cmds_stdout.read().strip()
                self._logger.debug(f"swarm frame container name: {self._frame_containers_swarm_name}")
            return self._frame_containers_swarm_name
        else:
            # container mode
            return self._frame_containers_real_name

    def _deploy_test_frame_in_containers(self):
        containers_name = self._get_frame_container_name()
        print('===== deploy test-frame-work at {}'.format(containers_name))
        cmds = f'docker exec {containers_name} sh -c "pip3 uninstall taostest" '
        ret = self.run_cmd(cmds)
        if ret != 0:
            return ret
        if self._opts.taostest_pkg is None:
            # set online install url to default value
            taostest_install_url = TDCom.taostest_install_url_default
            if TDCom.taostest_install_url_variable in os.environ:
                # get online install url from environment variable
                taostest_install_url = os.getenv(TDCom.taostest_install_url_variable)
            deploy_shells = "pip3 install " + taostest_install_url
        else:
            deploy_shells = "pip3 install " + self._opts.taostest_pkg
        cmds = f'docker exec {containers_name} sh -c "{deploy_shells}" '
        ret = self.run_cmd(cmds)
        if ret != 0:
            return ret
        return 0

    def _deploy_taospy_in_containers(self):
        # set  package server host to default value
        server = TDCom.package_server_host_default
        if TDCom.package_server_host_variable in os.environ:
            # get package server host
            server = os.getenv(TDCom.package_server_host_variable)

        # set package server username to default value
        user = TDCom.package_server_username_default
        if TDCom.package_server_host_variable in os.environ:
            # get package server username
            user = os.getenv(TDCom.package_server_username_variable)

        # set package server password to default value
        password = TDCom.package_server_password_default
        if TDCom.package_server_password_variable in os.environ:
            # get package server password
            password = os.getenv(TDCom.package_server_password_variable)

        # set package server root to default value
        rootPath = TDCom.package_server_root_default
        if TDCom.package_server_root_variable in os.environ:
            # get package server root
            rootPath = os.getenv(TDCom.package_server_root_variable)

        client_version = self._taopy_version
        filePath = f"{rootPath}/v{client_version}/enterprise"
        fileName = f"TDengine-enterprise-client-{client_version}-Linux-x64.tar.gz"
        pkgDir = f"TDengine-enterprise-client-{client_version}"
        docker_tmp = f"/tmp/"
        install_cmds = [f"mkdir -p /tmp/"]
        if self._opts.client_pkg is None:
            install_cmds.append(f"sshpass -p {password} scp {user}@{server}:{filePath}/{fileName} {docker_tmp}")
        else:
            install_cmds.append(f"cp {self._opts.client_pkg} {docker_tmp}")
            fileName = os.path.basename(self._opts.client_pkg)
            pkgDir = fileName[: -len("-Linux-x64.tar.gz")]
        install_cmds.append(f"cd {docker_tmp} && tar xzf {fileName} && cd {pkgDir} && ./install_client.sh && pip3 install connector/python")
        for shell_cmd in install_cmds:
            print(shell_cmd)
            cmd = "docker exec {} sh -c ' {}' ".format(self._get_frame_container_name(), shell_cmd)
            ret = self.run_cmd(cmd)
            if ret != 0:
                return ret
        return 0

    def _get_cmds_from_opts(self):
        cmds = self._str_cmds
        get_cmds = cmds.replace(
            "--containers", "").replace("--rm_containers", "").replace("--swarm", "")
        return get_cmds

    def _exec_cmds_in_containers(self, cmds):
        containers_name = self._get_frame_container_name()
        cmds = f"docker exec  {containers_name} sh -c ' {cmds} '"
        print("it will execute in containers , taos-test-framework is at {} and test_root path {}".format(containers_name, self._test_root))
        return self.run_cmd(cmds)
            
    def _execute_cases(self):
        self._logger.debug("execute cases")
        self._add_case_root_to_sys_path()
        early_stop_flag = threading.Event()
        parent_env = dict(os.environ)
        self._worker_threads = []
        self._done_workers = queue.Queue()
        self._result_queue = queue.Queue()
        self._task_queue = queue.Queue()
        self._worker_threads.append(threading.Thread(target=self._do_work, name="worker-0", args=(early_stop_flag, parent_env)))
        for case_py in self._opts.cases:
            if case_py.endswith(".py"):
                module_path = self._case_path_to_module_path(case_py)
                self._task_queue.put((module_path, ""))
            else:
                self._logger.error(f"error case name {case_py}")
        self._start_workers()
        self._wait_results()

    def _execute_groups(self):
        self._logger.debug(f"execute groups with concurrency {self._opts.concurrency}")
        self._add_case_root_to_sys_path()
        early_stop_flag = threading.Event()
        parent_env = dict(os.environ)
        self._worker_threads = []
        self._done_workers = queue.Queue()
        self._result_queue = queue.Queue()
        self._task_queue = queue.Queue()
        for worker_id in range(self._opts.concurrency):
            self._worker_threads.append(threading.Thread(target=self._do_work, name="worker-" + str(worker_id), args=(early_stop_flag, parent_env)))
        self._group_root = os.path.join(self._test_root, "groups")
        if self._opts.group_files:
            self._add_task_from_group_files()
        if self._opts.group_dirs:
            self._add_task_from_group_dirs()
        self._start_workers()
        self._wait_results()
        self._generate_test_report()

    def _start_workers(self):
        for t in self._worker_threads:
            t.start()

    def _wait_results(self):
        """
        Wait for all workers done and log results.
        """
        result_log_file = os.path.join(self._run_log_dir, "result.log")
        success_count = 0
        fail_count = 0
        with open(result_log_file, "wt", encoding="utf8") as f:
            while True:
                try:
                    result: ResultLog = self._result_queue.get(block=True, timeout=1)
                    if not result.success:
                        self._ret = 1
                        fail_count += 1
                    else:
                        success_count += 1
                    line = result.to_json()
                    f.write(line)
                    f.write("\n")
                except queue.Empty:
                    if self._done_workers.qsize() == len(self._worker_threads):
                        break
        self._logger.debug(f"SUCCESS {success_count} FAILED {fail_count}")

    def _do_work(self, early_stop_flag, env: dict):
        worker_name = threading.current_thread().name
        if env is not None:
            os.environ.update(env)
        while True:
            if self._opts.early_stop and early_stop_flag.is_set():
                self._logger.debug("early stop")
                self._done_workers.put(worker_name)
                break
            try:
                case_module, group_name = self._task_queue.get(block=True, timeout=1)
                ret = self._run_case(case_module, group_name)
                if not ret:
                    early_stop_flag.set()
            except queue.Empty:
                self._done_workers.put(worker_name)
                self._logger.debug(worker_name + " exited")
                break
            except Exception as e:
                traceback.print_exc()
                self._logger.error(str(e))
                early_stop_flag.set()

    def _init_case(self, case_module_path: str):
        self._logger.debug("loading case " + case_module_path)
        try:
            case_module = importlib.import_module(case_module_path)
        except BaseException as e:
            self._logger.error(f"{case_module_path} {Fore.RED} FAILED {Fore.RESET}: {e}")
            self._result_queue.put(ResultLog(case_path=case_module_path))
            return None
        CaseClass = self._get_case_from_module(case_module)
        if CaseClass is None:
            self._logger.error("No Case found in %s", case_module_path)
            self._result_queue.put(ResultLog(case_path=case_module_path))
            return None
        case: TDCase = CaseClass()
        case.logger = self._logger
        case.run_log_dir = self._run_log_dir
        case.log_dir_name = self._log_dir_name
        case.name = case_module_path
        case.env_setting = self._env_setting
        case.envMgr = self._env_mgr
        case._frame_inst = self
        case.config_file = self._opts.cfg_file
        case.case_param = self._opts.case_param
        case.lcmd = LocalCmd(log=self._logger)
        return case

    def _run_case(self, case_module_path: str, group: str = ""):
        """
        直接在当前线程执行 case
        """
        case: TDCase = self._init_case(case_module_path)
        if case is None:
            return False
        start_time = datetime.now()
        try:
            case.tdSql = TDSql(logger=self._logger, run_log_dir=self._run_log_dir, set_error_msg=case.set_error_msg)
            case.init()
            suc = case.run()
            if suc or suc is None:
                suc = True
                self._logger.info(f"{case.name}{Fore.GREEN} SUCCESS {Fore.RESET}")
            else:
                if not case.error_msg:
                    self._logger.error(f"{case.name} {Fore.RED} FAILED {Fore.RESET} : no error message")
                else:
                    self._logger.error(f"{case.name} {Fore.RED} FAILED: {Fore.RESET} {case.error_msg}")
        except BaseException as e:
            self._logger.error(f"{case.name} {Fore.RED} FAILED {Fore.RESET}: {e}")
            traceback.print_exc()
            suc = False
            case.error_msg = str(e)
        self._run_case_final(case, group, start_time, suc)
        return suc

    def _run_case_process(self, case_module_path: str, group: str, start_time: datetime, ret, env):
        if env is not None:
            os.environ.update(env)
        try:
            case: TDCase = self._init_case(case_module_path)
            case.tdSql = TDSql(logger=self._logger, run_log_dir=self._run_log_dir, set_error_msg=case.set_error_msg)
            case.init()
            suc = case.run()
            if suc or suc is None:
                suc = True
                self._logger.info(f"{case.name}{Fore.GREEN} SUCCESS {Fore.RESET}")
            else:
                if not case.error_msg:
                    self._logger.error(f"{case.name} {Fore.RED} FAILED {Fore.RESET} : no error message")
                else:
                    self._logger.error(f"{case.name} {Fore.RED} FAILED: {Fore.RESET} {case.error_msg}")
        except BaseException as e:
            self._logger.error(f"{case.name} {Fore.RED} FAILED {Fore.RESET}: {e}")
            traceback.print_exc()
            suc = False
            case.error_msg = str(e)
        ret.value = suc
        self._run_case_final(case, group, start_time, suc)

    def _run_case_final(self, case: TDCase, group: str, start_time: datetime, suc: bool):
        end_time = datetime.now()
        tags = self._format_case_tags(case.tags())
        report = case.get_report(start_time, end_time)
        self._result_queue.put(ResultLog(
            case_group=group,
            case_path=case.name,
            author=case.author(),
            tags=tags,
            desc=case.desc(),
            start_time=start_time,
            stop_time=end_time,
            success=suc,
            error_msg=case.error_msg,
            report=report
        ))
        try:
            case.cleanup()
        except:
            pass

    def _add_case_root_to_sys_path(self):
        import sys
        self._case_root = os.path.join(self._test_root, "cases")
        sys.path.append(self._case_root)

    @staticmethod
    def _case_path_to_module_path(case_py: str) -> str:
        """
        @param case_py: "path/test.py" or "path\test.py"
        @return: "path.test"
        """
        case_py = case_py[:-3]
        case_module = case_py.replace("/", ".")
        if '\\' in case_module:
            return case_module.replace("\\", ".")
        else:
            return case_module

    def _get_case_from_module(self, case_module):
        for name in dir(case_module):
            if name == "TDCase":
                continue
            attr = getattr(case_module, name)
            if type(attr).__name__ == 'ABCMeta' and issubclass(attr, TDCase):
                self._logger.debug("discovered case class " + name)
                return attr

    def _generate_test_report(self):
        self._logger.debug("generate test report")

    def _add_task_from_group_files(self):
        for group_file in self._opts.group_files:
            file = os.path.join(self._group_root, group_file)
            if not os.path.exists(file):
                self._logger.error(f"can't find group file {group_file} in $TEST_ROOT/groups")
                sys.exit(1)
            group_name = group_file.split(".")[0] if "." in group_file else group_file
            with open(file) as f:
                lines = f.readlines()
                for line in lines:
                    case_py = line.strip()
                    if case_py and case_py.endswith(".py"):
                        case_module = self._case_path_to_module_path(case_py)
                        self._task_queue.put((case_module, group_name))

    def _add_task_from_group_dirs(self):
        for group_name in self._opts.group_dirs:
            # group_name is group directory name.
            group_dir_list = list(os.scandir(os.path.join(self._case_root, group_name)))
            random.shuffle(group_dir_list)
            for file_entry in group_dir_list:
                file_name: str = file_entry.name
                if file_name.endswith(".py"):
                    case_path = group_name + '/' + file_name
                    case_module = self._case_path_to_module_path(case_path)
                    self._task_queue.put((case_module, group_name))

    def _format_case_tags(self, origin_tags):
        if origin_tags is None:
            return ["NoTag"]
        result = []
        if not isinstance(origin_tags, list) and not isinstance(origin_tags, tuple):
            origin_tags = [origin_tags]
        for tag in origin_tags:
            if type(tag).__name__ == "type" and issubclass(tag, CaseTag):
                result.append(tag.__tag_name__())
            else:
                result.append(str(tag))
        return result
