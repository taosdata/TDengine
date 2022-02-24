from taostest import run_case_use_option
from taostest.dataclass import CmdOption


def run():
    opt = CmdOption()
    opt.use = "walrestore.yaml"
    opt.cases = ["abnormal/walrestore.py"]
    opt.keep = True
    # opt.reset = True
    run_case_use_option(opt)


def destroy():
    opt = CmdOption()
    opt.destroy = "walrestore.yaml"

    run_case_use_option(opt)


if __name__ == '__main__':
    run()

# tt --use=walrestore.yaml --case=abnormal/walrestore.py --reset --keep
# tt --setup=walrestore.yaml
