###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import os ,sys
import random
import argparse
import subprocess
import time

# set path about run instance

core_path = '/home/coredump/'
base_dir = os.path.dirname(os.path.realpath(__file__))
if base_dir.find("community")>0:
    repo = "community"
elif base_dir.find("TDengine")>0:
    repo = "TDengine"
else:
    repo ="TDengine"
print("base_dir:",base_dir)
home_dir = base_dir[:base_dir.find(repo)]
print("home_dir:",home_dir)
run_dir = os.path.join(home_dir,'run_dir')
run_dir = os.path.abspath(run_dir)
print("run dir is set at :",run_dir)
if not os.path.exists(run_dir):
    os.mkdir(run_dir)
run_log_file = run_dir+'/crash_gen_run.log'
crash_gen_cmds_file = os.path.join(run_dir, 'crash_gen_cmds.sh')
exit_status_logs =  os.path.join(run_dir, 'crash_exit.log')

def get_path():
    buildPath=''
    selfPath = os.path.dirname(os.path.realpath(__file__))
    if ("community" in selfPath):
        projPath = selfPath[:selfPath.find("community")]
    else:
        projPath = selfPath[:selfPath.find("tests")]

    for root, dirs, files in os.walk(projPath):
        if ("taosd" in files):
            rootRealPath = os.path.dirname(os.path.realpath(root))
            if ("packaging" not in rootRealPath):
                buildPath = root[:len(root) - len("/build/bin")]
                break
    return buildPath

  
def random_args(args_list):
    nums_args_list = ["--max-dbs","--num-replicas","--num-dnodes","--max-steps","--num-threads",] # record int type arguments
    bools_args_list = ["--auto-start-service" , "--debug","--run-tdengine","--ignore-errors","--track-memory-leaks","--larger-data","--mix-oos-data","--dynamic-db-table-names",
    "--per-thread-db-connection","--record-ops","--verify-data","--use-shadow-db","--continue-on-exception"
    ]  # record bool type arguments
    strs_args_list = ["--connector-type"]  # record str type arguments

    args_list["--auto-start-service"]= True
    args_list["--continue-on-exception"]=True
    # connect_types=['native','rest','mixed'] # restful interface has change ,we should trans dbnames to connection or change sql such as "db.test"
    connect_types=['native']
    # args_list["--connector-type"]=connect_types[random.randint(0,2)]
    args_list["--connector-type"]= connect_types[0]
    args_list["--max-dbs"]= random.randint(1,10)
    
    dnodes = [1,3] # set single dnodes;
    
    args_list["--num-dnodes"]= random.sample(dnodes,1)[0]
    args_list["--num-replicas"]= random.randint(1,args_list["--num-dnodes"])
    args_list["--debug"]=False
    args_list["--per-thread-db-connection"]=True
    args_list["--track-memory-leaks"]=False

    num = random.randint(1, 10)   # set --track-memory-leaks randomly
    if num > 8 :
        args_list["--track-memory-leaks"]=True
    

    args_list["--max-steps"]=random.randint(300,500)
    
    threads = [32,64,128]
    args_list["--num-threads"]=random.sample(threads,1)[0] #$ debug
    args_list["--ignore-errors"]=[]   ## can add error codes for detail

    
    args_list["--run-tdengine"]= False
    args_list["--use-shadow-db"]= False
    args_list["--dynamic-db-table-names"]= False
    args_list["--verify-data"]= False
    args_list["--record-ops"] = False

    for key in bools_args_list:
        set_bool_value = [True,False]
        if key == "--auto-start-service" :
            continue
        elif key =="--run-tdengine":
            continue
        elif key == "--ignore-errors":
            continue
        elif key == "--debug":
            continue
        elif key == "--per-thread-db-connection":
            continue
        elif key == "--continue-on-exception":
            continue
        elif key == "--use-shadow-db":
            continue
        elif key =="--track-memory-leaks":
            continue
        elif key == "--dynamic-db-table-names":
            continue
        elif key == "--verify-data":
            continue
        elif key == "--record-ops":
            continue
        else:
            args_list[key]=set_bool_value[random.randint(0,1)]
    return args_list

def limits(args_list):
    if args_list["--use-shadow-db"]==True:
        if args_list["--max-dbs"] > 1:
            print("Cannot combine use-shadow-db with max-dbs of more than 1 ,set max-dbs=1")
            args_list["--max-dbs"]=1
        else:
            pass

    elif args_list["--num-replicas"]==0:
        print(" make sure num-replicas is at least 1 ")
        args_list["--num-replicas"]=1
    elif args_list["--num-replicas"]==1:
        pass

    elif args_list["--num-replicas"]>1:
        if not args_list["--auto-start-service"]:
            print("it should be deployed by crash_gen auto-start-service for multi replicas")
            
    else:
        pass
         
    return  args_list

def get_cmds(args_list):
    build_path = get_path()
    if repo == "community":
        crash_gen_path = build_path[:-5]+"community/tests/pytest/"
    elif repo == "TDengine":
        crash_gen_path = build_path[:-5]+"/tests/pytest/"
    else:
        pass

    bools_args_list = ["--auto-start-service" , "--debug","--run-tdengine","--ignore-errors","--track-memory-leaks","--larger-data","--mix-oos-data","--dynamic-db-table-names",
    "--per-thread-db-connection","--record-ops","--verify-data","--use-shadow-db","--continue-on-exception"]
    arguments = ""
    for k ,v in args_list.items():
        if k == "--ignore-errors":
            if v:
                arguments+=(k+"="+str(v)+" ")
            else:
                arguments+=""
        elif  k in bools_args_list and v==True:
            arguments+=(k+" ")
        elif k in bools_args_list and v==False:
            arguments+=""
        else:
            arguments+=(k+"="+str(v)+" ")
    crash_gen_cmd = 'cd %s && ./crash_gen.sh %s '%(crash_gen_path ,arguments)
    return crash_gen_cmd

def run_crash_gen(crash_cmds,result_file):
    os.system('echo "%s">>%s'%(crash_cmds,crash_gen_cmds_file))
    os.system("cp %s %s"%(crash_gen_cmds_file, core_path))
    os.system("%s>>%s"%(crash_cmds,result_file))

def check_status(result_file):
    run_code = subprocess.Popen("tail -n 50 %s"%result_file, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
    os.system("tail -n 50 %s>>%s"%(result_file,exit_status_logs))
    mem_status = check_memory(run_dir)
    if mem_status >0:
        return mem_status
    if "Crash_Gen is now exiting with status code: 1" in run_code:
        return 1
    elif "Crash_Gen is now exiting with status code: 0" in run_code:
        return 0
    else:
        return 2 

def check_memory(run_dir):
    '''
    invalid read, invalid write
    '''
    mem_report_path = os.path.join(run_dir , "reporter")
    if not os.path.exists(mem_report_path):
        os.mkdir(mem_report_path)
    status = 0
    stderr_files = subprocess.Popen("find %s -name \"stderr.log\" "%run_dir , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
    stderr_list = stderr_files.split("\n")[:-1]
    for stderr_file in stderr_list:
        print(stderr_file)
        grep_res = subprocess.Popen("grep -i 'Invalid read' %s "%stderr_file , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        dnode_name = stderr_file.split("/")[-3]
        back_path = os.path.join(core_path,"reporter",dnode_name)
        if not os.path.exists(back_path):
            os.system("mkdir -p %s"%back_path)

        if grep_res:
            os.system("cp %s %s"%(stderr_file , back_path))
            status = 4
            break
        
        grep_res = subprocess.Popen("grep -i 'Invalid write' %s "%stderr_file , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        if grep_res:
            status = 4
            os.system("cp %s %s"%(stderr_file , back_path))
            break
    return status

def main():
    args_list = {"--auto-start-service":False ,"--max-dbs":0,"--connector-type":"native","--debug":False,"--run-tdengine":False,"--ignore-errors":[],
        "--num-replicas":1 ,"--track-memory-leaks":False , "--larger-data":False, "--mix-oos-data":False, "--dynamic-db-table-names":False,"--num-dnodes":1,
        "--per-thread-db-connection":False , "--record-ops":False , "--max-steps":100, "--num-threads":10, "--verify-data":False,"--use-shadow-db":False , 
        "--continue-on-exception":False }

    build_path = get_path()
    os.system("pip3 install taospy")
    if repo =="community":
        crash_gen_path = build_path[:-5]+"community/tests/pytest/"
    elif repo =="TDengine":
        crash_gen_path = build_path[:-5]+"/tests/pytest/"
    else:
        pass
    print(crash_gen_path)
    if os.path.exists(crash_gen_path+"crash_gen.sh"):
        print(" make sure crash_gen.sh is ready")
    else:
        print( " crash_gen.sh is not exists ")
        sys.exit(1)
    
    # set value args

    args = random_args(args_list)
    args = limits(args)
    crash_cmds = get_cmds(args)
    crash_cmds= crash_cmds+"--set-path="+"%s"%run_dir
    # clean run_dir
    os.system('rm -rf %s'%run_dir )
    if not os.path.exists(run_dir):
        os.mkdir(run_dir)
    print(crash_cmds)
    run_crash_gen(crash_cmds,run_log_file)
    status = check_status(run_log_file)
    
    print("exit status : ", status)
    if status ==4:
        print('======== crash_gen found memory bugs at reporter ========')
    if status >0:
        print('======== crash_gen run failed and not exit as expected ========')
    else:
        print('======== crash_gen run sucess and exit as expected ========')
    exit(status)
    

if __name__ == '__main__':
    main()


