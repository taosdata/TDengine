import hashlib
import time
import os
from shutil import copyfile, rmtree
def get_file_md5(filename):
    f = open(filename, "r")
    f_md5 = hashlib.md5()
    f_md5.update(f.read().encode("utf-8"))
    f.close
    return f_md5.hexdigest()

def get_file_size(filename):
    return os.path.getsize(filename)

def mv_log():
    log_dir = "/var/log/taos"
    des_dir = "/var/log/taos_log_dir_0721"
    if not os.path.exists(des_dir):
        os.makedirs(des_dir)
    else:
        rmtree(des_dir)
        os.makedirs(des_dir)
    
    cp_delay = 10
    finish_tag = 0
    new_log_start_tag = 1
    while finish_tag == 0:
        for log_file in [f'{log_dir}/taosdlog.0', f'{log_dir}/taosdlog.1']:
            if os.path.exists(log_file):
                file_size1 = get_file_size(log_file)
                time.sleep(cp_delay)
                file_size2 = get_file_size(log_file)
                if file_size1 == file_size2:
                    copyfile(log_file, f'{des_dir}/taosdlog.{new_log_start_tag}')
                    new_log_start_tag += 1
mv_log()