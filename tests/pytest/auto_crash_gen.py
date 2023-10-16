import datetime
import os
import socket
import requests

# -*- coding: utf-8 -*-
import os ,sys
import random
import argparse
import subprocess
import time
import platform

# valgrind mode ?
valgrind_mode =  False

msg_dict = {0:"success" , 1:"failed" , 2:"other errors" , 3:"crash occured" , 4:"Invalid read/write" , 5:"memory leak" }

# formal
hostname = socket.gethostname()

group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'

def get_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "Crash_gen Monitor",
                    "content": [
                        [{
                            "tag": "text",
                            "text": text
                        }
                        ]]
                }
            }
        }
    }


def send_msg(json):
    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(url=group_url, headers=headers, json=json)
    inf = req.json()
    if "StatusCode" in inf and inf["StatusCode"] == 0:
        pass
    else:
        print(inf)


# set path about run instance

core_path = subprocess.Popen("cat /proc/sys/kernel/core_pattern", shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
core_path = "/".join(core_path.split("/")[:-1])
print(" =======   core path is %s ======== " %core_path)
if not os.path.exists(core_path):
    os.mkdir(core_path)

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
print("run dir is *** :",run_dir)
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

# generate crash_gen start script randomly

def random_args(args_list):
    nums_args_list = ["--max-dbs","--num-replicas","--num-dnodes","--max-steps","--num-threads",] # record int type arguments
    bools_args_list = ["--auto-start-service" , "--debug","--run-tdengine","--ignore-errors","--track-memory-leaks","--larger-data","--mix-oos-data","--dynamic-db-table-names",
    "--per-thread-db-connection","--record-ops","--verify-data","--use-shadow-db","--continue-on-exception"
    ]  # record bool type arguments
    strs_args_list = ["--connector-type"]  # record str type arguments

    args_list["--auto-start-service"]= False
    args_list["--continue-on-exception"]=True
    # connect_types=['native','rest','mixed'] # restful interface has change ,we should trans dbnames to connection or change sql such as "db.test"
    connect_types=['native']
    # args_list["--connector-type"]=connect_types[random.randint(0,2)]
    args_list["--connector-type"]= connect_types[0]
    args_list["--max-dbs"]= random.randint(1,10)
    
    # dnodes = [1,3] # set single dnodes;
    
    # args_list["--num-dnodes"]= random.sample(dnodes,1)[0]
    # args_list["--num-replicas"]= random.randint(1,args_list["--num-dnodes"])
    args_list["--debug"]=False
    args_list["--per-thread-db-connection"]=True
    args_list["--track-memory-leaks"]=False

    args_list["--max-steps"]=random.randint(500,2000)

    # args_list["--ignore-errors"]=[]   ## can add error codes for detail

    
    args_list["--run-tdengine"]= False
    args_list["--use-shadow-db"]= False
    args_list["--dynamic-db-table-names"]= True
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

    if args_list["--larger-data"]:
        threads = [16,32]
    else:
        threads = [32,64,128,256] 
    args_list["--num-threads"]=random.sample(threads,1)[0] #$ debug

    return args_list

def limits(args_list):
    if args_list["--use-shadow-db"]==True:
        if args_list["--max-dbs"] > 1:
            print("Cannot combine use-shadow-db with max-dbs of more than 1 ,set max-dbs=1")
            args_list["--max-dbs"]=1
        else:
            pass

    # env is start by test frame , not crash_gen instance
    
    # elif args_list["--num-replicas"]==0:
    #     print(" make sure num-replicas is at least 1 ")
    #     args_list["--num-replicas"]=1
    # elif args_list["--num-replicas"]==1:
    #     pass

    # elif args_list["--num-replicas"]>1:
    #     if not args_list["--auto-start-service"]:
    #         print("it should be deployed by crash_gen auto-start-service for multi replicas")
            
    # else:
    #     pass
         
    return  args_list

def get_auto_mix_cmds(args_list ,valgrind=valgrind_mode):
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
    
    if valgrind :
         
        crash_gen_cmd = 'cd %s && ./crash_gen.sh  --valgrind %s -g 0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012 '%(crash_gen_path ,arguments)

    else:

        crash_gen_cmd = 'cd %s && ./crash_gen.sh  %s -g 0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012'%(crash_gen_path ,arguments)

    return crash_gen_cmd

def start_taosd():
    build_path = get_path()
    if repo == "community":
        start_path = build_path[:-5]+"community/tests/system-test/"
    elif repo == "TDengine":
        start_path = build_path[:-5]+"/tests/system-test/"
    else:
        pass

    start_cmd = 'cd %s && python3 test.py >>/dev/null '%(start_path)
    os.system(start_cmd)

def get_cmds(args_list):    
    crash_gen_cmd = get_auto_mix_cmds(args_list,valgrind=valgrind_mode)
    return crash_gen_cmd

def run_crash_gen(crash_cmds):

    # prepare env of taosd
    start_taosd()

    build_path = get_path()
    if repo == "community":
        crash_gen_path = build_path[:-5]+"community/tests/pytest/"
    elif repo == "TDengine":
        crash_gen_path = build_path[:-5]+"/tests/pytest/"
    else:
        pass
    result_file = os.path.join(crash_gen_path, 'valgrind.out')


    # run crash_gen and back logs
    os.system('echo "%s">>%s'%(crash_cmds,crash_gen_cmds_file))
    os.system("%s >>%s "%(crash_cmds,result_file))


def check_status():
    build_path = get_path()
    if repo == "community":
        crash_gen_path = build_path[:-5]+"community/tests/pytest/"
    elif repo == "TDengine":
        crash_gen_path = build_path[:-5]+"/tests/pytest/"
    else:
        pass
    result_file = os.path.join(crash_gen_path, 'valgrind.out')
    run_code = subprocess.Popen("tail -n 50 %s"%result_file, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
    os.system("tail -n 50 %s>>%s"%(result_file,exit_status_logs))

    core_check = subprocess.Popen('ls -l  %s | grep "^-" | wc -l'%core_path, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")

    if int(core_check.strip().rstrip()) > 0:
        # it means core files has occured
        return 3
    
    if "Crash_Gen is now exiting with status code: 1" in run_code:
        return 1
    elif "Crash_Gen is now exiting with status code: 0" in run_code:
        return 0
    else:
        return 2


def main():

    args_list = {"--auto-start-service":False ,"--max-dbs":0,"--connector-type":"native","--debug":False,"--run-tdengine":False,"--ignore-errors":[],
    "--track-memory-leaks":False , "--larger-data":False, "--mix-oos-data":False, "--dynamic-db-table-names":False,
    "--per-thread-db-connection":False , "--record-ops":False , "--max-steps":100, "--num-threads":10, "--verify-data":False,"--use-shadow-db":False , 
    "--continue-on-exception":False }

    args = random_args(args_list)
    args = limits(args)


    build_path = get_path()
        
    if repo =="community":
        crash_gen_path = build_path[:-5]+"community/tests/pytest/"
    elif repo =="TDengine":
        crash_gen_path = build_path[:-5]+"/tests/pytest/"
    else:
        pass
    
    if os.path.exists(crash_gen_path+"crash_gen.sh"):
        print(" make sure crash_gen.sh is ready")
    else:
        print( " crash_gen.sh is not exists ")
        sys.exit(1)
    
    git_commit = subprocess.Popen("cd %s && git log | head -n1"%crash_gen_path, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")[7:16]
    
    # crash_cmds = get_cmds()
    
    crash_cmds = get_cmds(args)
    # clean run_dir
    os.system('rm -rf %s'%run_dir )
    if not os.path.exists(run_dir):
        os.mkdir(run_dir)
    print(crash_cmds)
    starttime = datetime.datetime.now()
    run_crash_gen(crash_cmds)
    endtime = datetime.datetime.now()
    status = check_status()
    
    print("exit status : ", status)
    
    if status ==4:
        print('======== crash_gen found memory bugs   ========')
    if status ==5:
        print('======== crash_gen found memory errors   ========')
    if status >0:
        print('======== crash_gen run failed and not exit as expected ========')
    else:
        print('======== crash_gen run sucess and exit as expected ========')

    try:
        cmd = crash_cmds.split('&')[2]
        if status == 0:
            log_dir = "none"            
        else:
            log_dir= "/root/pxiao/crash_gen_logs" 
        
        if status == 3:
            core_dir = "/root/pxiao/crash_gen_logs"
        else:
            core_dir = "none"
            
        text = f'''
        exit status: {msg_dict[status]}
        test scope: crash_gen
        owner: pxiao
        hostname: {hostname}
        start time: {starttime}
        end time: {endtime}
        git commit :  {git_commit}
        log dir: {log_dir}
        core dir: {core_dir}
        cmd: {cmd}'''
                
        send_msg(get_msg(text))  
    except Exception as e:
        print("exception:", e)
    exit(status)
        

if __name__ == '__main__':
    main()


