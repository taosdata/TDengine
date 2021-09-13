# -*-coding: utf-8-*-
# for TD-5159
import time
import taos
import sys, time, os, re, platform
from RemoteModule import RemoteModule
class Cal():
    def __init__(self):
        master_ip = "192.168.1.189"
        master_ssh_port = "22"
        ssh_user = "root"
        ssh_passwd = "tbase125!"
        log_dir = ""
        remote_dir = ""
        self.RM_master = RemoteModule(master_ip, master_ssh_port, ssh_user, ssh_passwd, log_dir, remote_dir)

    def execShellCmd(self, shell_cmd):
        result = os.popen(shell_cmd).read().strip()
        return result
    
    def caltimeFromKill(self):
        try:
            conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/home/ubuntu/abt_taos")
            while "failed" in str(conn):
                conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/home/ubuntu/abt_taos")
                if "failed" not in str(conn):
                    break
            c1 = conn.cursor()
            c1.execute("use test")
            insert_tag = 0 
            times = 0
            self.RM_master.exec_cmd('ps -ef | grep taosd | grep -v grep | awk \'{print $2}\' | xargs sudo kill -9')
            start_time = time.time()
            while insert_tag == 0 and times < 10:
                insert_res = c1.execute('insert into stb_22 values (now,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)')
                if insert_res == 1:
                    insert_tag = 1
                    end_time = time.time()
                    break
                else:
                    times += 1
            use_time = end_time - start_time
            print(use_time)
            return use_time
        except Exception:
            print("last failed")
if __name__ == '__main__':   
    cal = Cal()
    cal.caltimeFromKill()

