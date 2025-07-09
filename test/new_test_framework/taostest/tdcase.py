from abc import abstractmethod, ABCMeta
from datetime import datetime
from typing import Union
from .util.localcmd import LocalCmd
from .clustermanager import EnvManager
from .logger import Logger
from .util.benchmark import gen_benchmark_json
from .util.file import *
from .errors import LabelNotFoundException

try:
    from .util.sql import TDSql
except NameError:
    pass


class TDCase(metaclass=ABCMeta):
    def __init__(self):
        self.logger: Logger = None  # set value by frame
        self.run_log_dir: str = None  # full path of run log dir, set value by frame
        self.log_dir_name: str = None  # name of run log dir, set value by frame
        self.env_setting: str = None  # set value by frame
        self.name: str = None  # case name, set value by frame
        self.tdSql: TDSql = None  # run sql helpers, set value by frame
        self.envMgr: EnvManager = None  # wrap environment related api, set value by frame
        self.error_msg = None
        self.config_file = None
        self.case_param = None
        self.lcmd: LocalCmd = None

    def init(self):
        """
        Lifecycle method, will be called before `run`.
        :return: None
        """
        q = "create database if not exists " + self.get_default_database()
        if "DATABASE_REPLICAS" in os.environ:
            q = q + ' replica ' + os.environ["DATABASE_REPLICAS"]
        if "DATABASE_CACHEMODEL" in os.environ:
            q = q + ' cachemodel ' + f'"{os.environ["DATABASE_CACHEMODEL"]}"'
        #self.tdSql.execute(q)

        q = "use " + self.get_default_database()
        #self.tdSql.execute(q)

        self.logger.info("initialized " + self.name)

    @abstractmethod
    def run(self):
        """
        Lifecycle method. The main logic of this test case.
        :return: False for failure, True or None for success.
        """

    def cleanup(self):
        """
        Lifecycle method that will be called after `run`
        :return: None
        """
        # q = "drop database if exists " + self.get_default_database()
        # self.tdSql.execute(q)
        self.tdSql.close()

    @abstractmethod
    def desc(self) -> str:
        """
        @return: description of the case
        """

    @abstractmethod
    def author(self) -> str:
        """
        @return: author of the case
        """

    @abstractmethod
    def tags(self):
        """
        @return: tuple or str.
        如果只确定case所属的顶级分类，那么返回顶级分类的名称， 比如query;
        如果能确定更详细的分类，那么返回详细的路径，比如： return query.math_function.ABS
        如果属于多个分类，那么返回Tuple， 比如： return management.cluster, performance.benchmark.insert
        """

    def get_report(self, start_time: datetime, stop_time: datetime) -> str:
        """
        返回case的测试报告，格式为一段markdown文本。
        如果需要插入图片则需要case提前生成图片，放在run_log_dir.
        然后用markdown语法引用图片，入：![](image.png)
        """

    def set_error_msg(self, msg):
        self.error_msg = msg

    def get_default_database(self):
        return self.name.replace('.', '_')[-32:].lower()

    def genBenchmarkJson(self,
                         out_file_name: str,
                         base_config: Union[str, dict],
                         updates: Dict = None,
                         **kwargs):
        gen_benchmark_json(self.run_log_dir, out_file_name,
                           base_config, updates, **kwargs)

    def get_fqdn(self, component_name):
        result = []
        for component in self.env_setting["settings"]:
            if component["name"] == component_name:
                result.extend(component["fqdn"])
        return result

    def get_component_by_name(self, component_name):
        result = []
        for component in self.env_setting["settings"]:
            if component["name"] == component_name:
                result.append(component)
        return result

    def get_component_by_labels(self, label):
        result = []
        for component in self.env_setting["settings"]:
            if "labels" in component:
                labels = component["labels"]
                if isinstance(labels, list):
                    if label in labels:
                        result.append(component)
                elif isinstance(label, str):
                    if labels == label:
                        result.append(component)
        if len(result) == 0:
            raise LabelNotFoundException(label)
        return result

    def get_case_folder(self):
        """
        返回 case 所在目录的绝对路径
        """
        case_root = os.environ["TEST_ROOT"]
        case_path = "/".join(self.name.split(".")[:-1])
        return os.path.join(case_root, "cases", case_path)
