import logging
import subprocess
import os
import signal
from ..logger import get_basic_logger


class LocalCmd:
    def __init__(self, log=None):
        self.log: logging.Logger = log if log else get_basic_logger()

    def run(self, cmd: str, cwd: str, env: dict = None):
        """
        Execute command, not capture stdout and stderr, just print to console.
        Rais exception when command failed.
        """
        self.log.info("Run: %s", cmd)
        return subprocess.run(cmd.split(), cwd=cwd, check=True, env=env)

    def run_local_command(self, str_cmd, timeout=60):
        """
        Execute the commands of windows and linux within timeout, default timeout is 60 seconds
        """
        self.format = "utf-8"
        p = subprocess.Popen(str_cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, close_fds=True, start_new_session=True)
        if not timeout:
            timeout = 60

        try:
            (msg, errs) = p.communicate(timeout=timeout)
            ret_code = p.poll()
            if ret_code >= 1:
                code = 1
                msg = str(msg, encoding=self.format)
            else:
                code = 0
                msg = str(msg, encoding=self.format)
        except subprocess.TimeoutExpired:
            # os.killpg is needed to kill all sub processes
            p.kill()
            p.terminate()
            os.killpg(p.pid, signal.SIGTERM)

            # note：the results are returned until the command execution finished if open below two line c    odes
            (outs, errs) = p.communicate()
            code = 1
            # msg = "Timeout Error: Command '" + str_cmd + "' timed out after " + str(timeout) + " seconds    , outs:" + outs.decode(self.format)
            msg = "Timeout Error: Command '" + str_cmd + "' timed out after " + str(timeout) + " seconds,     outs:" + str(outs)
            raise TimeoutError(msg)
        except Exception as e:
            code = 1
            msg = "Unknown Error: " + str(e)
            raise Exception(msg)

        return str(msg), str(errs), code
