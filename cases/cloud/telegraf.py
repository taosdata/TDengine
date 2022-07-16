import subprocess as sp

from taostest import TDCase, T


class Telegraf(TDCase):

    def init(self):
        pass

    def run(self):
        cwd = self.env_setting["work_dir"]
        self.logger.info("cwd: %s", cwd)
        env = self.env_setting["env"]
        self.logger.info("env: %s", env)
        p = sp.Popen(["telegraf", "--debug", "--config", "telegraf.conf"],
                     stdout=sp.PIPE,
                     stderr=sp.STDOUT,  # redirect stderr to stdout
                     cwd=cwd, env=env, )
        # Read first 20 lines of log
        for _ in range(20):
            ret_status = p.poll()
            if ret_status is not None:
                self.set_error_msg(f"process exit unexpectedly with code {ret_status}")
                return False
            line = p.stdout.readline()
            self.logger.info(line)
            if "Successfully connected to outputs.http" in line:
                p.kill()
                return True
        self.set_error_msg("Not found success message from telegraf's log")
        return False

    def desc(self) -> str:
        return "Test connectivity between cloud and telegraf"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud
