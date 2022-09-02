# -*- coding: utf-8 -*-

import os
import sys
from time import sleep

# param init
file_dir = "/root/cyjia/taosx-backup"
dbname = 'db'
tbname_m = 'd'
tb_num = 1000
row_num = 10000
taosd_cfg_dir = "/etc/taos"


def export_childtable_check(export_type):
    for file_type in ['csv','parquet']:
        if export_type == 'native':
            os.system(
                f"taosx run -f 'taos://root:taosdata@localhost:6030/{dbname}?query=select * from {tbname_m}0' -t 'csv:{file_dir}/{tbname_m}0.{file_type}' -v")
        elif export_type == 'websocket':
            os.system(
                f"taosx run -f 'taos+ws://root:taosdata@localhost:6041/{dbname}?query=select * from {tbname_m}0' -t 'csv:{file_dir}/{tbname_m}0.{file_type}' -v")
        total = sum(1 for line in open(f"{file_dir}/{tbname_m}0.{file_type}")) - 1
        if total == row_num:
            print(
                f"test OK with file in {export_type} mode: {tbname_m}0.{file_type}  execpt rows : {row_num} ,actual rows : {total}")
        else:
            print(
                f"\033[1;31;40m export child_table check fail with file in {export_type} mode: {tbname_m}0.{file_type} ! except rows : {row_num} ,actual rows : {total} \033[0m")
            sys.exit("\033[1;31;40m test case execute failure! \033[0m")
        os.system(f"rm -rf {file_dir}/*")

def export_stable_check(export_type):
    for file_type in ['csv','parquet']:
        if export_type == 'native':
            os.system(
                f"taosx run -f 'taos://root:taosdata@localhost:6030/{dbname}?query=select * from meters' -t 'csv:{file_dir}/meters.{file_type}' -v")
        elif export_type == 'websocket':
            os.system(
                f"taosx run -f 'taos+ws://root:taosdata@localhost:6041/{dbname}?query=select * from meters' -t 'csv:{file_dir}/meters.{file_type}' -v")
        total = sum(1 for line in open(f"{file_dir}/meters.{file_type}")) - 1
        if total == row_num*tb_num:
            print(
                f"test OK with file in {export_type} mode : meters.{file_type} execpt rows : {row_num*tb_num} ,actual rows : {total}")
        else:
            print(
                f"\033[1;31;40m export child_table check fail with file {export_type} mode: meters.{file_type} ! except rows : {row_num*tb_num} ,actual rows : {total} \033[0m")
            sys.exit("\033[1;31;40m test case execute failure! \033[0m")
        os.system(f"rm -rf {file_dir}/*")

if __name__ == '__main__':
    os.system(f"mkdir -p {file_dir}")
    os.system("killall taosd && killall taosadapter")
    os.system(f"ulimit -n 1048576 && screen -d -m taosd -c {taosd_cfg_dir} && screen -d -m taosadapter")
    sleep(5)
    os.system(f"taosBenchmark -y -t {tb_num} -n {row_num} -d {dbname} -m {tbname_m}")
    for export_type in ['native','websocket']:
        export_childtable_check(export_type)
        export_stable_check(export_type)
    os.system(f"rm -rf {file_dir}")
    sys.exit("\033[1;32;40m test case execute successfully!\033[0m")
    
    
