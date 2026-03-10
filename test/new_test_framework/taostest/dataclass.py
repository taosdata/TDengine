import json
from dataclasses import dataclass
from typing import List, Any
import datetime


@dataclass
class CmdOption:
    destroy: str = None
    setup: str = None
    server_pkg: str = None
    client_pkg: str = None
    use: str = None
    concurrency: int = 1
    group_files: List[str] = None
    group_dirs: List[str] = None
    cases: List[str] = None
    tag: str = None
    keep: str = False
    reset: str = False
    early_stop: bool = False
    help: bool = None
    version: bool = None
    init: bool = None
    prepare: str = None
    tdcfg: dict = None
    env_init: bool = False
    containers: bool = False
    rm_containers: bool = False
    cmds: str = None
    log_level = None
    docker_network = None
    swarm = None
    disable_collection = None
    disable_data_collection = None
    uniform_dist = None
    taostest_pkg = None
    case_param = None
    taosd_valgrind: bool = False
    taosc_valgrind: bool = False
    fast_write_param_list: list = None
    stop: bool = False


class TaosJsonEncoder(json.JSONEncoder):

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            super(TaosJsonEncoder, self).default(o)


@dataclass
class ResultLog:
    case_group: str = ""
    case_path: str = ""
    author: str = None
    tags: List[str] = None
    desc: str = None
    start_time: datetime.datetime = None
    stop_time: datetime.datetime = None
    elapse: float = None
    success: bool = None
    error_msg: str = ""
    report: Any = None

    def to_json(self):
        if self.start_time and self.stop_time:
            delta = self.stop_time - self.start_time
            self.elapse = delta.seconds + delta.microseconds / 1000000
        return json.dumps(self.__dict__, cls=TaosJsonEncoder, ensure_ascii=False)
