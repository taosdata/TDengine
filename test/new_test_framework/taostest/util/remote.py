"""
执行远程shell命令
"""
import os
import platform
from typing import Union
import winrm

import asyncio
import asyncssh
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List
import subprocess

from fabric2 import Connection, Result


from ..logger import Logger
from ...utils.log import tdLog

class Remote:
    def __init__(self, logger):
        self._logger: Logger = logger
        self._local_host = platform.node()

    def cmd_old(self, host, cmd_list, password="") -> Union[str, None]:
        """
        用于执行本地shell命令或远程shell命令。
        如果没有抛异常,那么返回stdout; 如果抛异常,返回None;
        """
        if isinstance(cmd_list, list):
            cmd_line = " & ".join(cmd_list)
        else:
            cmd_line = cmd_list
        self._logger.info("cmd on %s: %s", host, cmd_line)
        # # 执行本地shell命令
        # if host == self._local_host:
        #     return os.popen(cmd_line).read().strip()
        # 执行本地shell命令
        if host == self._local_host:
            result = subprocess.run(cmd_line, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0:
                self._logger.error(result.stderr.decode())
                return None
            return result.stdout.decode().strip()
        # 执行远程shell命令
        try:
            with Connection(host, user="root", connect_kwargs={"password": password}) as c:
                result = c.run(cmd_line, warn=True)
                if result.failed:
                    self._logger.error(result.stderr)
                return result.stdout.strip()
        except Exception as e:
            self._logger.exception(f"Exception occur---{e}")
            return None

    async def async_cmd(self, host, cmd_list, password="", error_output=None) -> Union[str, None]:
        """
        异步执行本地shell命令或远程shell命令。
        如果没有抛异常,那么返回stdout; 如果抛异常,返回None;
        """
        if isinstance(cmd_list, list):
            cmd_line = " & ".join(cmd_list)
        else:
            cmd_line = cmd_list
        if "taos" in cmd_line:
            tdLog.info("cmd on %s: %s", host, cmd_line)
        else:
            tdLog.debug("cmd on %s: %s", host, cmd_line)
        # 执行本地shell命令
        if host == self._local_host or host == "localhost":
            if error_output is None:
                result = await asyncio.to_thread(subprocess.run, cmd_line, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                with open(error_output, "a") as f:
                    result = await asyncio.to_thread(subprocess.run, cmd_line, shell=True, stdout=f, stderr=f)
            if result.returncode != 0:
                try:
                    err_msg = result.stderr.decode("utf-8")
                except UnicodeDecodeError:
                    err_msg = result.stderr.decode("gbk", errors="ignore")
                tdLog.error(f"running command: {cmd_line}")
                tdLog.error(err_msg)
                return None
            return result.stdout.decode().strip()
        # 执行远程shell命令
        try:
            async with asyncssh.connect(host, username="root", password=password, known_hosts=None) as conn:
                result = await conn.run(cmd_line)
                if result.exit_status != 0:
                    self._logger.error(result.stderr)
                    return None
                return result.stdout.strip()
        except asyncssh.PermissionDenied:
            self._logger.error(f"Permission denied for user root on host {host}")
            return None
        except Exception as e:
            self._logger.exception(f"Exception occur---{e}")
            return None



    def cmd(self, host, cmd_list, password="", error_output=None) -> Union[str, None]:
        return asyncio.run(self.async_cmd(host, cmd_list, password, error_output))
    

    # def cmd_parallel(self, host_cmd_list: List[tuple], password="") -> List[Union[str, None]]:
    #     """
    #     并行执行多个命令 cmd_parallel。
    #     host_cmd_list: [(host, cmd_list), ...]
    #     """
    #     self._logger.debug(f"host_cmd_list: {host_cmd_list}")
    #     if not all(isinstance(item, tuple) and len(item) == 2 for item in host_cmd_list):
    #         raise ValueError("host_cmd_list should be a list of tuples (host, cmd_list)")
    #     loop = asyncio.get_event_loop()
    #     tasks = [self.async_cmd(host, cmd_list, password) for host, cmd_list in host_cmd_list]
    #     return loop.run_until_complete(asyncio.gather(*tasks))

    
    def cmd_all(self, system, host, cmd_list, password="") -> Union[str, None]:
        """
        用于执行本地shell命令或远程shell命令。
        如果没有抛异常,那么返回stdout; 如果抛异常,返回None;
        """
        if isinstance(cmd_list, list):
            cmd_line = " && ".join(cmd_list)
        else:
            cmd_line = cmd_list
        self._logger.info("cmd on %s: %s", host, cmd_line)
        # 执行本地shell命令
        if host == self._local_host:
            return os.popen(cmd_line).read().strip()
        # 执行远程shell命令
        '''
        hard code for windows user and password
        '''
        try:
            if system == "windows":
                print("execute in windows")
                win_con= winrm.Session(f'http://{host}:5985/wsman',auth=('administrator', 'tbase125!'))
                print("start execute in windows")
                result = win_con.run_cmd(f'{cmd_line}')
                return str(result.std_out,encoding='gbk')
                    # if result.failed:
                    #     self._logger.error(result.stderr)
                # return result.std_out.strip()                    
            else:
                with Connection(host, user="root", connect_kwargs={"password": password}) as c:
                    result = c.run(cmd_line, warn=True)
                    if result.failed:
                        self._logger.error(result.stderr)
                    return result.stdout.strip()
        except Exception as e:
            self._logger.exception(f"Exception occur---{e}")
            return None
            
    def cmd_windows(self, host, cmd_list, password="") -> Union[str, None]:
        """
        Running local or remote  windows commands。
        remote windows Commands are executed using winrm:
            If no exception is thrown, then return if return failed(return code=2)
            otherwise None is returned; 
        """
        if isinstance(cmd_list, list):
            cmd_line = " && ".join(cmd_list)
        else:
            cmd_line = cmd_list
        self._logger.debug("cmd on %s: %s", host, cmd_line)
        # 执行本地shell命令
        if host == self._local_host or host == "localhost":
            return os.system(cmd_line)
        # 执行远程shell命令
        else:
            '''
            hard code for windows user and password
            '''
            try:
                win_con= winrm.Session(f'http://{host}:5985/wsman',auth=('administrator', 'tbase125!'))
                print("start execute in windows")
                result = win_con.run_cmd(f'{cmd_line}')
                code = result.status_code
                content = result.std_out if code == 0 else result.std_err
                if code != 0 :
                    self._logger.error(content)
                    return (f"cmd failed code:%d"%code)
                else:
                    try:
                        result_dec = content.decode("utf8")
                    except:
                        result_dec = content.decode("GBK")
                    return result_dec.strip()
                # return result.std_out.strip()                    
            except Exception as e:
                self._logger.exception(f"Exception occur---{e}")
                return None


    def cmd2(self, host, cmd_list, password="") -> Result:
        """
        使用fabric执行远程shell命令。
        返回fabric2.Result对象。
        用result.ok和result.failed,判断远程命令的返回状态。
        用result.stdout和result.stderr获取远程命令的输出。
        """
        if isinstance(cmd_list, list):
            cmd_line = " && ".join(cmd_list)
        else:
            cmd_line = cmd_list
        self._logger.info("cmd2 on %s: %s", host, cmd_line)
        with Connection(host, user="root", connect_kwargs={"password": password}) as c:
            return c.run(cmd_line, warn=True)

    def rsync_dir(local_path, remote_host, remote_path, exclude=".git", user="root"):
        exclude_opt = f"--exclude={exclude}" if exclude else ""
        cmd = f'rsync -az {exclude_opt} -e "ssh -o StrictHostKeyChecking=no" {local_path} {user}@{remote_host}:{remote_path}'
        result = subprocess.run(cmd, shell=True)
        return result.returncode == 0

    def put(self, host, file, path, password="") -> bool:
        self._logger.debug("put %s to %s:%s", file, host, path)
        if host == platform.node() or host == "localhost":
            if not os.path.exists(path):
                self.mkdir(host, path)
            os.system("cp -rf {0} {1}".format(file, path))
            if platform.system().lower() == "windows":
                os.system(f'xcopy /E /Y "{file}" "{path}\\"')
            else:
                os.system(f'cp -rf "{file}" "{path}"')
            return True
        try:
            with Connection(host, user="root", connect_kwargs={"password": password}) as c:
                cmd = "mkdir -p " + path
                self._logger.debug("run: %s", cmd)
                c.run(cmd, warn=True)
                if os.path.isdir(file):
                    self.rsync_dir(file, host, path, exclude=".git")
                else:
                    c.put(file, path)
                return True
        except:
            self._logger.error(f"put failed on {host} for file {file}")
            return False

    def get(self, host, file, path, password=""):
        """
        Copy remote file to local path.
        """
        cmd = f"scp -rp -o StrictHostKeyChecking=no root@{host}:{file} {path}"
        if password != "":
            cmd = f"sshpass -p {password} scp -rp -o StrictHostKeyChecking=no root@{host}:{file} {path}"
        self._logger.debug("cmd: %s", cmd)
        ret = os.system(cmd)
        self._logger.debug("return: %s", ret)

    def delete(self, host, file, password=""):
        deleteCmd = 'rm -rf {}'.format(file)
        self.cmd(host, [deleteCmd], password)

    def mkdir(self, host, path, password=""):
        if platform.system().lower() == "windows":
            path = path.replace("/", "\\")
            cmd = f"mkdir {path}"
            self.cmd_windows(host, [cmd], password)
        else:
            self.cmd(host, [f"mkdir -p {path}"], password)

    def command_exists(self, host, prog_name):
        """
        检查一个命令行程序是否可用
        """
        cmd = "command -v " + prog_name
        return self.cmd2(host, [cmd]).ok
