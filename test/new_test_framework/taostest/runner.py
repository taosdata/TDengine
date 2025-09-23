"""
提供了运行测试用例的编程API， 作为 taostest 命令行工具的补充。
通常一个 case 可以使用 taostest 命令，但是在 case 开发阶段，需要能够在IDE中调试单个 Case 。 比如打断点单步执行。
此时可以使用 run_case 方法执行case。run_case 方法假设测试环境都已经setup好。
如果需要自定义 case 执行的 option, 可以使用 run_case_use_option 函数。
使用示例：main.py
import taostest
taostest.runner.run_case("test_env.yaml", "query/case1.py")
"""

from .dataclass import CmdOption
from .frame import TaosTestFrame
#from .main import check_env


def run_case(env_file, case_file):
    #env_ok = check_env()
    #if not env_ok:
    #    print("set TEST_ROOT")
    #    return
    opts = CmdOption()
    opts.use = env_file
    opts.cases = [case_file]
    opts.keep = True
    taos_test = TaosTestFrame(opts)
    taos_test.start()
    return taos_test


def run_case_use_option(opt: CmdOption):
    #env_ok = check_env()
    #if not env_ok:
    #    print("set TEST_ROOT")
    #    return
    if opt is None or not isinstance(opt, CmdOption):
        print("wrong opt")
        return
    taos_test = TaosTestFrame(opt)
    taos_test.start()
    return taos_test
