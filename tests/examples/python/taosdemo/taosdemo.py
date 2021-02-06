#!/usr/bin/python3
#  * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#  *
#  * This program is free software: you can use, redistribute, and/or modify
#  * it under the terms of the GNU Affero General Public License, version 3
#  * or later ("AGPL"), as published by the Free Software Foundation.
#  *
#  * This program is distributed in the hope that it will be useful, but WITHOUT
#  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  * FITNESS FOR A PARTICULAR PURPOSE.
#  *
#  * You should have received a copy of the GNU Affero General Public License
#  * along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

import sys
import getopt
import requests
import json
import random
import time
import datetime
from multiprocessing import Manager, Pool, Lock
from multipledispatch import dispatch
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


@dispatch(str, str)
def v_print(msg: str, arg: str):
    if verbose:
        print(msg % arg)


@dispatch(str, str, str)
def v_print(msg: str, arg1: str, arg2: str):
    if verbose:
        print(msg % (arg1, arg2))


@dispatch(str, str, str, str)
def v_print(msg: str, arg1: str, arg2: str, arg3: str):
    if verbose:
        print(msg % (arg1, arg2, arg3))


@dispatch(str, str, str, str, str)
def v_print(msg: str, arg1: str, arg2: str, arg3: str, arg4: str):
    if verbose:
        print(msg % (arg1, arg2, arg3, arg4))


@dispatch(str, int)
def v_print(msg: str, arg: int):
    if verbose:
        print(msg % int(arg))


@dispatch(str, int, str)
def v_print(msg: str, arg1: int, arg2: str):
    if verbose:
        print(msg % (int(arg1), str(arg2)))


@dispatch(str, str, int)
def v_print(msg: str, arg1: str, arg2: int):
    if verbose:
        print(msg % (arg1, int(arg2)))


@dispatch(str, int, int)
def v_print(msg: str, arg1: int, arg2: int):
    if verbose:
        print(msg % (int(arg1), int(arg2)))


@dispatch(str, int, int, str)
def v_print(msg: str, arg1: int, arg2: int, arg3: str):
    if verbose:
        print(msg % (int(arg1), int(arg2), str(arg3)))


@dispatch(str, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3)))


@dispatch(str, int, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int, arg4: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3), int(arg4)))


def restful_execute(host: str, port: int, user: str, password: str, cmd: str):
    url = "http://%s:%d/rest/sql" % (host, restPort)

    v_print("restful_execute - cmd: %s", cmd)

    resp = requests.post(url, cmd, auth=(user, password))

    v_print("resp status: %d", resp.status_code)

    if debug:
        v_print(
            "resp text: %s",
            json.dumps(
                resp.json(),
                sort_keys=True,
                indent=2))
    else:
        print("resp: %s" % json.dumps(resp.json()))


def query_func(process: int, thread: int, cmd: str):
    v_print("%d process %d thread cmd: %s", process, thread, cmd)

    if oneMoreHost != "NotSupported" and random.randint(
            0, 1) == 1:
        v_print("%s", "Send to second host")
        if native:
            cursor2.execute(cmd)
        else:
            restful_execute(
                oneMoreHost, port, user, password, cmd)
    else:
        v_print("%s%s%s", "Send ", cmd, " to the host")
        if native:
            pass
#            cursor.execute(cmd)
        else:
            restful_execute(
                host, port, user, password, cmd)


def query_data_process(q_lock, i: int, cmd: str):
    time.sleep(0.01)
    v_print("Process:%d threads: %d cmd: %s", i, threads, cmd)

    q_lock.aquire()
    cursor.execute(cmd)
    q_lock.release()

    return i


def query_data(cmd: str):
    v_print("query_data processes: %d, cmd: %s", processes, cmd)

    q_lock = Lock()

    pool = Pool(processes)
    for i in range(processes):
        pool.apply_async(query_data_process, args=(q_lock, i, cmd))
#        time.sleep(1)
    pool.close()
    pool.join()


def insert_data(processes: int):
    i_lock = Lock()
    pool = Pool(processes)

    begin = 0
    end = 0

    quotient = numOfTb // processes
    if quotient < 1:
        processes = numOfTb
        quotient = 1

    remainder = numOfTb % processes
    v_print(
        "insert_data num of tables: %d, quotient: %d, remainder: %d",
        numOfTb,
        quotient,
        remainder)

    for i in range(processes):
        begin = end

        if i < remainder:
            end = begin + quotient + 1
        else:
            end = begin + quotient

        v_print("insert_data Process %d from %d to %d", i, begin, end)
        pool.apply_async(insert_data_process, args=(i_lock, i, begin, end))

    pool.close()
    pool.join()


def create_stb():
    for i in range(0, numOfStb):
        if native:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS %s%d (ts timestamp, value float) TAGS (uuid binary(50))" %
                (stbName, i))
        else:
            restful_execute(
                host,
                port,
                user,
                password,
                "CREATE TABLE IF NOT EXISTS %s%d (ts timestamp, value float) TAGS (uuid binary(50))" %
                (stbName, i)
            )


def use_database():

    if native:
        cursor.execute("USE %s" % current_db)
    else:
        restful_execute(host, port, user, password, "USE %s" % current_db)


def create_databases():
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))

        if native:
            cursor.execute(
                "CREATE DATABASE IF NOT EXISTS %s%d" % (dbName, i))
        else:
            restful_execute(
                host,
                port,
                user,
                password,
                "CREATE DATABASE IF NOT EXISTS %s%d" % (dbName, i))


def drop_tables():
    # TODO
    v_print("TODO: drop tables total %d", numOfTb)
    pass


def drop_stable():
    # TODO
    v_print("TODO: drop stables total %d", numOfStb)
    pass


def drop_databases():
    v_print("drop databases total %d", numOfDb)

    # drop exist databases first
    for i in range(0, numOfDb):
        v_print("will drop database db%d", int(i))

        if native:
            cursor.execute(
                "DROP DATABASE IF EXISTS %s%d" %
                (dbName, i))
        else:
            restful_execute(
                host,
                port,
                user,
                password,
                "DROP DATABASE IF EXISTS %s%d" %
                (dbName, i))


def insert_func(process: int, thread: int):
    v_print("%d process %d thread, insert_func ", process, thread)

    # generate uuid
    uuid_int = random.randint(0, numOfTb + 1)
    uuid = "%s" % uuid_int
    v_print("uuid is: %s", uuid)

    # establish connection if native
    if native:
        v_print("host:%s, user:%s passwd:%s configDir:%s ", host, user, password, configDir)
        try:
            conn = taos.connect(
                host=host,
                user=user,
                password=password,
                config=configDir)
            print("conn: %s" % str(conn.__class__))
        except Exception as e:
            print("Error: %s" % e.args[0])
            sys.exit(1)

        try:
            cursor = conn.cursor()
            print("cursor:%d %s" % (id(cursor), str(cursor.__class__)))
        except Exception as e:
            print("Error: %s" % e.args[0])
            sys.exit(1)

    v_print("numOfRec %d:", numOfRec)

    row = 0
    while row < numOfRec:
        v_print("row: %d", row)
        sqlCmd = ['INSERT INTO ']
        try:
            sqlCmd.append(
                "%s.%s%d " % (current_db, tbName, thread))

            if (numOfStb > 0 and autosubtable):
                sqlCmd.append("USING %s.%s%d TAGS('%s') " %
                              (current_db, stbName, numOfStb - 1, uuid))

            start_time = datetime.datetime(
                2021, 1, 25) + datetime.timedelta(seconds=row)

            sqlCmd.append("VALUES ")
            for batchIter in range(0, batch):
                sqlCmd.append("('%s', %f) " %
                              (
                               start_time +
                                datetime.timedelta(
                                    milliseconds=batchIter),
                                  random.random()))
                row = row + 1
                if row >= numOfRec:
                    v_print("BREAK, row: %d numOfRec:%d", row, numOfRec)
                    break

        except Exception as e:
            print("Error: %s" % e.args[0])

        cmd = ' '.join(sqlCmd)

        if measure:
            exec_start_time = datetime.datetime.now()

        if native:
            affectedRows = cursor.execute(cmd)
        else:
            restful_execute(
                host, port, user, password, cmd)

        if measure:
            exec_end_time = datetime.datetime.now()
            exec_delta = exec_end_time - exec_start_time
            print(
                "%s, %d" %
                (time.strftime('%X'),
                 exec_delta.microseconds))

        v_print("cmd: %s, length:%d", cmd, len(cmd))

    if native:
        cursor.close()
        conn.close()


def create_tb_using_stb():
    # TODO:
    pass


def create_tb():
    v_print("create_tb() numOfTb: %d", numOfTb)
    for i in range(0, numOfDb):
        if native:
            cursor.execute("USE %s%d" % (dbName, i))
        else:
            restful_execute(
                host, port, user, password, "USE %s%d" %
                (dbName, i))

        for j in range(0, numOfTb):
            if native:
                cursor.execute(
                    "CREATE TABLE %s%d (ts timestamp, value float)" %
                    (tbName, j))
            else:
                restful_execute(
                    host,
                    port,
                    user,
                    password,
                    "CREATE TABLE %s%d (ts timestamp, value float)" %
                    (tbName, j))


def insert_data_process(lock, i: int, begin: int, end: int):
    lock.acquire()
    tasks = end - begin
    v_print("insert_data_process:%d table from %d to %d, tasks %d", i, begin, end, tasks)

    if (threads < (end - begin)):
        for j in range(begin, end, threads):
            with ThreadPoolExecutor(max_workers=threads) as executor:
                k = end if ((j + threads) > end) else (j + threads)
                workers = [
                    executor.submit(
                        insert_func,
                        i,
                        n) for n in range(
                        j,
                        k)]
                wait(workers, return_when=ALL_COMPLETED)
    else:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            workers = [
                executor.submit(
                    insert_func,
                    i,
                    j) for j in range(
                    begin,
                    end)]
            wait(workers, return_when=ALL_COMPLETED)

    lock.release()


def query_db(i):
    if native:
        cursor.execute("USE %s%d" % (dbName, i))
    else:
        restful_execute(
            host, port, user, password, "USE %s%d" %
            (dbName, i))

    for j in range(0, numOfTb):
        if native:
            cursor.execute(
                "SELECT COUNT(*) FROM %s%d" % (tbName, j))
        else:
            restful_execute(
                host, port, user, password, "SELECT COUNT(*) FROM %s%d" %
                (tbName, j))


def printConfig():

    print("###################################################################")
    print("# Use native interface:              %s" % native)
    print("# Server IP:                         %s" % host)
    if native:
        print("# Server port:                       %s" % port)
    else:
        print("# Server port:                       %s" % restPort)

    print("# Configuration Dir:                 %s" % configDir)
    print("# User:                              %s" % user)
    print("# Password:                          %s" % password)
    print("# Number of Columns per record:      %s" % colsPerRecord)
    print("# Number of Threads:                 %s" % threads)
    print("# Number of Processes:               %s" % processes)
    print("# Number of Tables:                  %s" % numOfTb)
    print("# Number of records per Table:       %s" % numOfRec)
    print("# Records/Request:                   %s" % batch)
    print("# Database name:                     %s" % dbName)
    print("# Replica:                           %s" % replica)
    print("# Use STable:                        %s" % useStable)
    print("# Table prefix:                      %s" % tbName)
    if useStable:
        print("# STable prefix:                     %s" % stbName)

    print("# Data order:                        %s" % outOfOrder)
    print("# Data out of order rate:            %s" % rateOOOO)
    print("# Delete method:                     %s" % deleteMethod)
    print("# Query command:                     %s" % queryCmd)
    print("# Insert Only:                       %s" % insertOnly)
    print("# Verbose output                     %s" % verbose)
    print("# Test time:                         %s" %
          datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    print("###################################################################")


if __name__ == "__main__":

    native = False
    verbose = False
    debug = False
    measure = True
    dropDbOnly = False
    colsPerRecord = 3
    numOfDb = 1
    dbName = "test"
    replica = 1
    batch = 1
    numOfTb = 1
    tbName = "tb"
    useStable = False
    numOfStb = 0
    stbName = "stb"
    numOfRec = 10
    ieration = 1
    host = "127.0.0.1"
    configDir = "/etc/taos"
    oneMoreHost = "NotSupported"
    port = 6030
    restPort = 6041
    user = "root"
    defaultPass = "taosdata"
    processes = 1
    threads = 1
    insertOnly = False
    autosubtable = False
    queryCmd = "DEFAULT"
    outOfOrder = 0
    rateOOOO = 0
    deleteMethod = 0
    skipPrompt = False

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       'Nh:p:u:P:d:a:m:Ms:Q:T:C:r:l:t:n:c:xOR:D:vgyH',
                                       [
            'native', 'host', 'port', 'user', 'password', 'dbname', 'replica', 'tbname',
            'stable', 'stbname', 'query', 'threads', 'processes',
            'recPerReq', 'colsPerRecord', 'numOfTb', 'numOfRec', 'config',
            'insertOnly', 'outOfOrder', 'rateOOOO', 'deleteMethod',
            'verbose', 'debug', 'skipPrompt', 'help'
        ])
    except getopt.GetoptError as err:
        print('ERROR:', err)
        print('Try `taosdemo.py --help` for more options.')
        sys.exit(1)

    if bool(opts) is False:
        print('Try `taosdemo.py --help` for more options.')
        sys.exit(1)

    for key, value in opts:
        if key in ['-H', '--help']:
            print('')
            print(
                'taosdemo.py for TDengine')
            print('')
            print('Author: Shuduo Sang <sangshuduo@gmail.com>')
            print('')

            print('\t-H, --help                        Show usage.')
            print('')

            print('\t-N, --native                      flag, Use native interface if set. Default is using RESTful interface.')
            print('\t-h, --host <hostname>             host, The host to connect to TDengine. Default is localhost.')
            print('\t-p, --port <port>                 port, The TCP/IP port number to use for the connection. Default is 0.')
            print('\t-u, --user <username>             user, The user name to use when connecting to the server. Default is \'root\'.')
            print('\t-P, --password <password>         password, The password to use when connecting to the server. Default is \'taosdata\'.')
            print('\t-l, --colsPerRec <number>         num_of_columns_per_record, The number of columns per record. Default is 3.')
            print(
                  '\t-d, --dbname <dbname>             database, Destination database. Default is \'test\'.')
            print('\t-a, --replica <replications>      replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.')
            print(
                  '\t-m, --tbname <table prefix>       table_prefix, Table prefix name. Default is \'t\'.')
            print(
                  '\t-M, --stable                      flag, Use super table. Default is no')
            print(
                  '\t-s, --stbname <stable prefix>     stable_prefix, STable prefix name. Default is \'st\'')
            print('\t-Q, --query <DEFAULT | NO | command>   query, Execute query command. set \'DEFAULT\' means select * from each table')
            print(
                  '\t-T, --threads <number>            num_of_threads, The number of threads. Default is 1.')
            print(
                  '\t-C, --processes <number>          num_of_processes, The number of threads. Default is 1.')
            print('\t-r, --batch <number>              num_of_records_per_req, The number of records per request. Default is 1000.')
            print(
                  '\t-t, --numOfTb <number>            num_of_tables, The number of tables. Default is 1.')
            print('\t-n, --numOfRec <number>           num_of_records_per_table, The number of records per table. Default is 1.')
            print('\t-c, --config <path>               config_directory, Configuration directory. Default is \'/etc/taos/\'.')
            print('\t-x, --inserOnly                   flag, Insert only flag.')
            print('\t-O, --outOfOrder                  out of order data insert, 0: In order, 1: Out of order. Default is in order.')
            print('\t-R, --rateOOOO <number>           rate, Out of order data\'s rate--if order=1 Default 10, min: 0, max: 50.')
            print('\t-D, --deleteMethod <number>       Delete data methods 0: don\'t delete, 1: delete by table, 2: delete by stable, 3: delete by database.')
            print('\t-v, --verbose                     Print verbose output')
            print('\t-g, --debug                       Print debug output')
            print(
                  '\t-y, --skipPrompt                  Skip read key for continous test, default is not skip')
            print('')
            sys.exit(0)

        if key in ['-N', '--native']:
            try:
                import taos
            except Exception as e:
                print("Error: %s" % e.args[0])
                sys.exit(1)
            native = True

        if key in ['-h', '--host']:
            host = value

        if key in ['-p', '--port']:
            port = int(value)

        if key in ['-u', '--user']:
            user = value

        if key in ['-P', '--password']:
            password = value
        else:
            password = defaultPass

        if key in ['-d', '--dbname']:
            dbName = value

        if key in ['-a', '--replica']:
            replica = int(value)
            if replica < 1:
                print("FATAL: number of replica need > 0")
                sys.exit(1)

        if key in ['-m', '--tbname']:
            tbName = value

        if key in ['-M', '--stable']:
            useStable = True
            numOfStb = 1

        if key in ['-s', '--stbname']:
            stbName = value

        if key in ['-Q', '--query']:
            queryCmd = str(value)

        if key in ['-T', '--threads']:
            threads = int(value)
            if threads < 1:
                print("FATAL: number of threads must be larger than 0")
                sys.exit(1)

        if key in ['-C', '--processes']:
            processes = int(value)
            if processes < 1:
                print("FATAL: number of processes must be larger than 0")
                sys.exit(1)

        if key in ['-r', '--batch']:
            batch = int(value)

        if key in ['-l', '--colsPerRec']:
            colsPerRec = int(value)

        if key in ['-t', '--numOfTb']:
            numOfTb = int(value)
            v_print("numOfTb is %d", numOfTb)

        if key in ['-n', '--numOfRec']:
            numOfRec = int(value)
            v_print("numOfRec is %d", numOfRec)
            if numOfRec < 1:
                print("FATAL: number of records must be larger than 0")
                sys.exit(1)


        if key in ['-c', '--config']:
            configDir = value
            v_print("config dir: %s", configDir)

        if key in ['-x', '--insertOnly']:
            insertOnly = True
            v_print("insert only: %d", insertOnly)

        if key in ['-O', '--outOfOrder']:
            outOfOrder = int(value)
            v_print("out of order is %d", outOfOrder)

        if key in ['-R', '--rateOOOO']:
            rateOOOO = int(value)
            v_print("the rate of out of order is %d", rateOOOO)

        if key in ['-D', '--deleteMethod']:
            deleteMethod = int(value)
            if (deleteMethod < 0) or (deleteMethod > 3):
                print(
                    "inputed delete method is %d, valid value is 0~3, set to default 0" %
                    deleteMethod)
                deleteMethod = 0
            v_print("the delete method is %d", deleteMethod)

        if key in ['-v', '--verbose']:
            verbose = True

        if key in ['-g', '--debug']:
            debug = True

        if key in ['-y', '--skipPrompt']:
            skipPrompt = True

    if verbose:
        printConfig()

    if not skipPrompt:
        input("Press any key to continue..")

    # establish connection first if native
    if native:
        v_print("host:%s, user:%s passwd:%s configDir:%s ", host, user, password, configDir)
        try:
            conn = taos.connect(
                host=host,
                user=user,
                password=password,
                config=configDir)
            print("conn: %s" % str(conn.__class__))
        except Exception as e:
            print("Error: %s" % e.args[0])
            sys.exit(1)

        try:
            cursor = conn.cursor()
            print("cursor:%d %s" % (id(cursor), str(cursor.__class__)))
        except Exception as e:
            print("Error: %s" % e.args[0])
            sys.exit(1)

    # drop data only if delete method be set
    if deleteMethod > 0:
        if deleteMethod == 1:
            drop_tables()
            print("Drop tables done.")
        elif deleteMethod == 2:
            drop_stables()
            print("Drop super tables done.")
        elif deleteMethod == 3:
            drop_databases()
            print("Drop Database done.")
        sys.exit(0)

    # create databases
    drop_databases()
    create_databases()

    # use last database
    current_db = "%s%d" % (dbName, (numOfDb - 1))
    use_database()

    if measure:
        start_time_begin = time.time()

    if numOfStb > 0:
        create_stb()
        if (autosubtable == False):
            create_tb_using_stb()
    else:
        create_tb()

    if measure:
        end_time = time.time()
        print(
            "Total time consumed {} seconds for create table.".format(
            (end_time - start_time_begin)))

    if native:
        cursor.close()
        conn.close()

    # start insert data
    if measure:
        start_time = time.time()

    manager = Manager()
    lock = manager.Lock()
    pool = Pool(processes)

    begin = 0
    end = 0

    quotient = numOfTb // processes
    if quotient < 1:
        processes = numOfTb
        quotient = 1

    remainder = numOfTb % processes
    v_print(
        "num of tables: %d, quotient: %d, remainder: %d",
        numOfTb,
        quotient,
        remainder)

    for i in range(processes):
        begin = end

        if i < remainder:
            end = begin + quotient + 1
        else:
            end = begin + quotient
        pool.apply_async(insert_data_process, args=(lock, i, begin, end,))
#        pool.apply_async(text, args=(lock, i, begin, end,))

    pool.close()
    pool.join()
    time.sleep(1)

    if measure:
        end_time = time.time()
        print(
            "Total time consumed {} seconds for insert data.".format(
            (end_time - start_time)))


    # query data
    if queryCmd != "NO":
        print("queryCmd: %s" % queryCmd)
        query_data(queryCmd)

    if measure:
        end_time = time.time()
        print(
            "Total time consumed {} seconds.".format(
                (end_time - start_time_begin)))

    print("done")
