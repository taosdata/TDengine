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
from multiprocessing import Process, Pool
from multipledispatch import dispatch
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


@dispatch(str, str)
def v_print(msg: str, arg: str):
    if verbose:
        print(msg % arg)


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
    url = "http://%s:%d/rest/sql" % (host, port)

    if verbose:
        v_print("cmd: %s", cmd)

    resp = requests.post(url, cmd, auth=(user, password))

    v_print("resp status: %d", resp.status_code)

    if verbose:
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
        restful_execute(
            oneMoreHost, port, user, password, cmd)
    else:
        v_print("%s", "Send to first host")
        restful_execute(
            host, port, user, password, cmd)


def query_data_process(i: int, cmd: str):
    v_print("Process:%d threads: %d cmd: %s", i, threads, cmd)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        workers = [
            executor.submit(
                query_func,
                i,
                j,
                cmd) for j in range(
                0,
                threads)]

        wait(workers, return_when=ALL_COMPLETED)

    return i


def query_data(cmd: str):
    v_print("query_data processes: %d, cmd: %s", processes, cmd)
    pool = Pool(processes)
    for i in range(processes):
        pool.apply_async(query_data_process, args=(i, cmd))
        time.sleep(1)
    pool.close()
    pool.join()


def insert_data(processes: int):
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

        v_print("Process %d from %d to %d", i, begin, end)
        pool.apply_async(insert_data_process, args=(i, begin, end))

    pool.close()
    pool.join()


def create_stb():
    for i in range(0, numOfStb):
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE TABLE IF NOT EXISTS st%d (ts timestamp, value float) TAGS (uuid binary(50))" %
            i)


def create_databases():
    for i in range(0, numOfDb):
        v_print("will create database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "CREATE DATABASE IF NOT EXISTS db%d" %
            i)


def drop_databases():
    v_print("drop databases total %d", numOfDb)

    # drop exist databases first
    for i in range(0, numOfDb):
        v_print("will drop database db%d", int(i))
        restful_execute(
            host,
            port,
            user,
            password,
            "DROP DATABASE IF EXISTS db%d" %
            i)


def insert_func(process: int, thread: int):
    v_print("%d process %d thread, insert_func ", process, thread)

    # generate uuid
    uuid_int = random.randint(0, numOfTb + 1)
    uuid = "%s" % uuid_int
    v_print("uuid is: %s", uuid)

    v_print("numOfRec %d:", numOfRec)
    if numOfRec > 0:
        row = 0
        while row < numOfRec:
            v_print("row: %d", row)
            sqlCmd = ['INSERT INTO ']
            try:
                sqlCmd.append(
                    "%s.tb%s " % (current_db, thread))

                if (numOfStb > 0 and autosubtable):
                    sqlCmd.append("USING %s.st%d TAGS('%s') " %
                                  (current_db, numOfStb - 1, uuid))

                start_time = datetime.datetime(
                    2020, 9, 25) + datetime.timedelta(seconds=row)

                sqlCmd.append("VALUES ")
                for batchIter in range(0, batch):
                    sqlCmd.append("('%s', %f) " %
                                  (start_time +
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

            if oneMoreHost != "NotSupported" and random.randint(
                    0, 1) == 1:
                v_print("%s", "Send to second host")
                restful_execute(
                    oneMoreHost, port, user, password, cmd)
            else:
                v_print("%s", "Send to first host")
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


def create_tb_using_stb():
    # TODO:
    pass


def create_tb():
    v_print("create_tb() numOfTb: %d", numOfTb)
    for i in range(0, numOfDb):
        restful_execute(host, port, user, password, "USE db%d" % i)
        for j in range(0, numOfTb):
            restful_execute(
                host,
                port,
                user,
                password,
                "CREATE TABLE tb%d (ts timestamp, value float)" %
                j)


def insert_data_process(i: int, begin: int, end: int):
    tasks = end - begin
    v_print("Process:%d table from %d to %d, tasks %d", i, begin, end, tasks)

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


if __name__ == "__main__":

    verbose = False
    measure = False
    dropDbOnly = False
    numOfDb = 1
    batch = 1
    numOfTb = 1
    numOfStb = 0
    numOfRec = 10
    ieration = 1
    host = "127.0.0.1"
    oneMoreHost = "NotSupported"
    port = 6041
    user = "root"
    defaultPass = "taosdata"
    processes = 1
    threads = 1
    insertonly = False
    autosubtable = False
    queryCmd = ""

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:],
                                       'Nh:p:u:P:d:a:m:Ms:Q:T:P:r:t:n:c:xOR:D:vgyH',
                                       [
            'native', 'host', 'port', 'user', 'password', 'dbname', 'replica', 'tbname',
            'supertable', 'stbname', 'query', 'numOfThreads', 'numOfProcesses',
            'numOfRecPerReq', 'numbOfTb', 'numOfRec', 'config',
            'insertOnly', 'outOfOrder', 'rateOOOO','deleteMethod',
            'verbose', 'debug', 'skipprompt', 'help'
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

            print('\t-H, --help                         Show usage.')
            print('')

            print('\t-N, --native                       flag, Use native interface if set. Default is using RESTful interface.')
            print('\t-h, --host <hostname>              host, The host to connect to TDengine. Default is localhost.')
            print('\t-p, --port <port>                  port, The TCP/IP port number to use for the connection. Default is 0.')
            print('\t-u, --user <username>              user, The user name to use when connecting to the server. Default is \'root\'.')
            print('\t-P, --password <password>          password, The password to use when connecting to the server. Default is \'taosdata\'.')
            print('\t-d, --dbname <dbname>              database, Destination database. Default is \'test\'.')
            print('\t-a, --replica <replications>       replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.')
            print('\t-m, --tbname <table prefix>        table_prefix, Table prefix name. Default is \'t\'.')
            print('\t-M, --supertable                   flag, Use super table. Default is no')
            print('\t-s, --stbname <stable prefix>      stable_prefix, STable prefix name. Default is \'st\'')
            print('\t-Q, --query <DEFAULT | command>    query, Execute query command. set \'DEFAULT\' means select * from each table')
            print('\t-T, --numOfThreads <number>        num_of_threads, The number of threads. Default is 1.')
            print('\t-P, --numOfProcesses <number>      num_of_processes, The number of threads. Default is 1.')
            print('\t-r, --numOfRecPerReq <number>      num_of_records_per_req, The number of records per request. Default is 1000.')
            print('\t-t, --numOfTb <number>             num_of_tables, The number of tables. Default is 1.')
            print('\t-n, --numOfRec <number>            num_of_records_per_table, The number of records per table. Default is 1.')
            print('\t-c, --config <path>                config_directory, Configuration directory. Default is \'/etc/taos/\'.')
            print('\t-x, --inserOnly                    flag, Insert only flag.')
            print('\t-O, --outOfOrder                   out of order data insert, 0: In order, 1: Out of order. Default is in order.')
            print('\t-R, --rateOOOO <number>            rate, Out of order data\'s rate--if order=1 Default 10, min: 0, max: 50.')
            print('\t-D, --deleteMethod <number>        Delete data methods 0: don\'t delete, 1: delete by table, 2: delete by stable, 3: delete by database.')
            print('\t-v, --verbose                      Print verbose output')
            print('\t-g, --debug                        Print debug output')
            print('\t-y, --skipprompt                   Skip read key for continous test, default is not skip')
            print('')
            sys.exit(0)

        if key in ['-s', '--hoSt']:
            host = value

        if key in ['-m', '--one-More-host']:
            oneMoreHost = value

        if key in ['-o', '--pOrt']:
            port = int(value)

        if key in ['-u', '--User']:
            user = value

        if key in ['-w', '--passWord']:
            password = value
        else:
            password = defaultPass

        if key in ['-v', '--Verbose']:
            verbose = True

        if key in ['-A', '--Autosubtable']:
            autosubtable = True

        if key in ['-M', '--Measure']:
            measure = True

        if key in ['-P', '--Processes']:
            processes = int(value)
            if processes < 1:
                print("FATAL: number of processes must be larger than 0")
                sys.exit(1)

        if key in ['-T', '--Threads']:
            threads = int(value)
            if threads < 1:
                print("FATAL: number of threads must be larger than 0")
                sys.exit(1)

        if key in ['-q', '--Query']:
            queryCmd = str(value)

        if key in ['-p', '--droPdbonly']:
            dropDbOnly = True

        if key in ['-d', '--numofDb']:
            numOfDb = int(value)
            v_print("numOfDb is %d", numOfDb)
            if (numOfdb <= 0):
                print("ERROR: wrong number of database given!")
                sys.exit(1)

        if key in ['-c', '--batCh']:
            batch = int(value)

        if key in ['-t', '--numofTb']:
            numOfTb = int(value)
            v_print("numOfTb is %d", numOfTb)

        if key in ['-b', '--numofstB']:
            numOfStb = int(value)
            v_print("numOfStb is %d", numOfStb)

        if key in ['-r', '--numofRec']:
            numOfRec = int(value)
            v_print("numOfRec is %d", numOfRec)

        if key in ['-f', '--File']:
            fileOut = value
            v_print("file is %s", fileOut)

        if key in ['-x', '--insertonLy']:
            insertonly = True
            v_print("insert only: %d", insertonly)

#    if verbose:
#        restful_execute(
#            host,
#            port,
#            user,
#            password,
#            "SHOW DATABASES")

    if dropDbOnly:
        drop_databases()
        print("Drop Database done.")
        sys.exit(0)

    if queryCmd != "":
        print("queryCmd: %s" % queryCmd)
        query_data(queryCmd)
        sys.exit(0)

    # create databases
    if (insertonly == False):
        drop_databases()
    create_databases()

    if measure:
        start_time = time.time()

    # use last database
    current_db = "db%d" % (numOfDb - 1)
    restful_execute(host, port, user, password, "USE %s" % current_db)

    if numOfStb > 0:
        create_stb()
        if (autosubtable == False):
            create_tb_using_stb()

        insert_data(processes)

        if verbose:
            for i in range(0, numOfDb):
                for j in range(0, numOfStb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM db%d.st%d" % (i, j,))

        print("done")

        if measure:
            end_time = time.time()
            print(
                "Total time consumed {} seconds.".format(
                    (end_time - start_time)))

        sys.exit(0)

    if numOfTb > 0:
        create_tb()
        insert_data(processes)

        if verbose:
            for i in range(0, numOfDb):
                restful_execute(host, port, user, password, "USE db%d" % i)
                for j in range(0, numOfTb):
                    restful_execute(host, port, user, password,
                                    "SELECT COUNT(*) FROM tb%d" % (j,))

    print("done")
    if measure:
        end_time = time.time()
        print(
            "Total time consumed {} seconds.".format(
                (end_time - start_time)))
