# -*-coding: utf-8-*-
from fabric import Connection
from config.env_init import *

class RemoteModule():
    def __init__(self, ip, port, user, passwd):
        self.ip     = ip 
        self.port   = port     
        self.user   = user      
        self.passwd = passwd
        
    def upload_file(self, remote_dir, upload_file):
        """
            remote_dir: remote upload dir
            upload_file: local file with path
        """
        try:
            logger.info(f'{self.ip}: uploading {upload_file} to {remote_dir}')
            c = Connection(self.ip, user=self.user, port=self.port, connect_timeout=120, connect_kwargs={"password": self.passwd})
            c.put(upload_file, remote_dir)
            c.close()
        except Exception as e:
            logger.error(f"{upload_file} send failed----{e}, please check config/perf_test.yaml")
        
    def download_file(self, remote_file_with_path, local_path):
        """
            remote_file_with_path:: file with Absolute Path    eg:/root/maple/bin/maple
            local_path:: remote path    eg:/root
        """
        try:
            c = Connection(self.ip, user=self.user, port=self.port, connect_timeout=120, connect_kwargs={"password": self.passwd})
            c.get(remote_file_with_path, local_path)
            c.close()
        except Exception as e:
            logger.error(f"download file {remote_file_with_path} failed：{e}");

    def exec_cmd(self, cmd):
        """
            cmd:: remote exec cmd
        """
        try:
            logger.info(f'{self.ip}: executing cmd: {cmd}')
            c = Connection(self.ip, user=self.user, port=self.port, connect_timeout=120, connect_kwargs={"password": self.passwd})
            result = c.run(cmd, pty=False, warn=True, hide=False)
            c.close()
            return result.stdout
        except Exception as e:
            logger.error(f"exec cmd {cmd} failed：{e}");
        
if __name__ == '__main__':
    pass