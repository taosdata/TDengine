import os
import taos
import time


# --------------------- util -----------------------
def exe(command, show = False):
    code = os.system(command)
    if show:
        print(f"eos.exe retcode={code} command:{command}")
    return code

def readFileContext(filename):
    file = open(filename)
    context = file.read()
    file.close()
    return context

# run return output and error
def run(command):
    id = time.time_ns() % 100000
    out = f"out_{id}.txt"
    err = f"err_{id}.txt"
    
    code = exe(command + f" 1>{out} 2>{err}")

    # read from file
    output = readFileContext(out)
    error  = readFileContext(err)

    # del
    if os.path.exists(out):
        os.remove(out)
    if os.path.exists(err):
        os.remove(err)

    return output, error, code


def waitStreamReady(stream_name="", timeout=120):
    sql = "select * from information_schema.ins_stream_tasks where type = 'Trigger' and status != 'Running'"
    if len(stream_name) > 0:
        sql += f" and name = '{stream_name}'"
    
    conn = taos.connect()
    cursor = conn.cursor()   
    print(f"Wait stream ready...")
    time.sleep(5)
    for i in range(timeout):
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) == 0:
            conn.close()
            return
        time.sleep(1)
        

    info = f"stream task status not ready in {timeout} seconds"
    print(info)
    conn.close()
    raise Exception(info)

# 
# --------------------- man ---------------------------
#

def showDatabase():
    conn = taos.connect()
    databases = conn.execute("SHOW DATABASES").fetchall()
    for db in databases:
        print(db)
    conn.close()

def getRelativePath(file_name):
    current_dir = os.path.dirname(__file__)
    return os.path.join(current_dir, file_name)

def execSqlFile(file_path):
    conn = taos.connect()
    with open(file_path, 'r') as file:
        sql_commands = file.readlines()
        for command in sql_commands:
            command = command.strip()
            if command:
                conn.execute(command)
                print(f"Exec: {command} success.")
    conn.close()

def taosXImportCsv(desc, source, parser_file, batch_size):
    command = f"taosx run -t '{desc}' -f '{source}?batch_size={batch_size}' --parser '@{parser_file}'"
    print(f"Executing command: {command}")
    output, error, code = run(command)
    print(f"Output: {output}")
    print(f"Error: {error}")
    print(f"Return code: {code}")
    
    return output, error, code

def waitStreamEnd(verify_sql, expect_rows, timeout=120):
    print("Waiting for stream processing to complete...")
    print(f"Verify SQL: {verify_sql}, Expect Rows: {expect_rows}, Timeout: {timeout} seconds")
    conn = taos.connect()
    cursor = conn.cursor()
    for i in range(timeout):
        try:        
            cursor.execute(verify_sql)
            results = cursor.fetchall()
            rows = results[0][0]
            if rows == expect_rows:
                print(f"i={i} stream processing completed.")
                conn.close()
                return
            print(f"{i} real rows: {rows}, expect rows: {expect_rows}")
        except Exception as e:
            print(f"i={i} query stream result table except: {e}")
        
        time.sleep(1)

    info = f"stream processing not completed in {timeout} seconds"
    print(info)
    conn.close()
    raise Exception(info)

if __name__ == "__main__":
    #  prepare environment
    pre_sql_file = getRelativePath('data/pre.sql')
    execSqlFile(pre_sql_file)
    
    waitStreamReady()
    
    # taosX import csv
    start = time.time()
    output, error, code = taosXImportCsv (
        desc = "taos://root:taosdata@localhost:6030/test",
        source = f"csv:" + getRelativePath('data/data.csv'),
        parser_file = getRelativePath('data/taosX.json'),
        batch_size = 50000
    )
    end = time.time()
    cost = end - start
    print("taosX import csv time cost: %.3f seconds"%(cost))
    
    # wait stream end
    waitStreamEnd(
        verify_sql = "select count(*) from test.result_1",
        expect_rows = 100
    )
    end = time.time()
    cost = end - start
    print("Stream deal finished time cost: %.3f seconds"%(cost))
    