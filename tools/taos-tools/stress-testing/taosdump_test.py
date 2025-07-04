import os
import subprocess
import time
import json
import argparse
import taos
from ast import literal_eval


def init_data(path):
    cmd = "taosBenchmark -f " + path
    os.system("%s" % cmd)
    if not os.path.exists("./dump_data"):
        os.makedirs("./dump_data")
    else:
        os.system("rm -rf ./dump_data")
        os.makedirs("./dump_data")

    if not os.path.exists("./result"):
        os.makedirs("./result")

    if not os.path.exists("./logs"):
        os.makedirs("./logs")

    print("Data initialization completed successfully.")


def export_data(args):


    cmd = "taosdump -D power -o ./dump_data/ -T 10"
    if args.connectMode == "websocket" or args.connectMode == "1":
        cmd += " -Z 1"

    id = time.time_ns() % 100000
    out = f"logs/export_out_{id}.txt"
    err = f"logs/export_err_{id}.txt"
    cmd += f" > {out} 2> {err}"

    start = time.perf_counter()
    os.system("%s" % cmd)
    end = time.perf_counter()
    print("Data export completed successfully, time taken: %.2f seconds." % (end - start))
    return (end - start)


def import_data(args):
    cmd = "taosdump -i ./dump_data/ -T 10 -W 'power=newpower'"

    if args.connectMode == "websocket" or args.connectMode == "1":
        cmd += " -Z 1"

    id = time.time_ns() % 100000
    out = f"logs/{args.caseName}_import_out_{id}.txt"
    err = f"logs/{args.caseName}_import_err_{id}.txt"
    cmd += f" > {out} 2> {err}"

    start = time.perf_counter()
    os.system("%s" % cmd)
    end = time.perf_counter()
    print("Data import completed successfully, time taken: %.2f seconds." % (end - start))
    return (end - start)


def parse_column_definitions(column_str: str) -> list:
    """
    解析命令行中的列定义字符串
    格式: '{"name":"ts", "type":"TIMESTAMP"}, {"name":"value", "type":"FLOAT", "min":0, "max":100}'
    """
    try:
        # 将单引号字符串转换为双引号字符串（JSON标准）
        json_str = column_str.replace("'", '"')
        # 尝试直接解析为JSON数组
        try:
            return json.loads(f'[{json_str}]')
        except json.JSONDecodeError:
            # 如果失败，使用ast.literal_eval安全解析
            return literal_eval(f'[{column_str}]')
    except Exception as e:
        raise ValueError(f"列定义解析失败: {e}") from e


def get_all_type():
    columns = [
        {"type": "bool", "count": 150},
        {"type": "float", "max": 1, "min": 0, "count": 150},
        {"type": "double", "max": 1, "min": 0, "count": 150},
        {"type": "tinyint", "max": 500, "min": 0, "count": 150},
        {"type": "smallint", "max": 500, "min": 0, "count": 150},
        {"type": "int", "max": 500, "min": 0, "count": 150},
        {"type": "bigint", "max": 500, "min": 0, "count": 150},
        {"type": "utinyint", "max": 500, "min": 0, "count": 150},
        {"type": "usmallint", "max": 500, "min": 0, "count": 150},
        {"type": "uint", "max": 500, "min": 0, "count": 150},
        {"type": "ubigint", "max": 500, "min": 0, "count": 150},
        {"type": "binary", "len": 32, "count": 150},
        {"type": "nchar", "len": 64, "count": 150},
        {"type": "varbinary", "len": 64, "count": 150}
    ]

    tags = [
        {"name": "int_tag", "type": "TINYINT"}
    ]
    return columns, tags


def create_benchmark_json(args):
    with open("./taosdump_test.json", "r") as file:
        config = json.load(file)
    config['databases'][0]['dbinfo']['vgroups'] = args.vgroups
    config['databases'][0]['super_tables'][0]['childtable_count'] = args.childTableCount
    config['databases'][0]['super_tables'][0]['insert_rows'] = args.insertRows
    config['thread_count'] = args.threadCount
    config['num_of_records_per_req'] = args.batchSize

    if args.colTypes == "all":
        columns, tags = get_all_type()
        config['databases'][0]['super_tables'][0]['tags'] = tags
        config['databases'][0]['super_tables'][0]['columns'] = columns
    else:
        config['databases'][0]['super_tables'][0]['tags'] = parse_column_definitions(args.tagTypes)
        config['databases'][0]['super_tables'][0]['columns'] = parse_column_definitions(args.colTypes)

    json_config = json.dumps(config)
    filePath = "./" + args.caseName + ".json"
    with open(filePath, "w") as file:
        file.write(json_config)
    return filePath, json_config

def check_test_result(args):
    conn = taos.connect()
    sql = "SELECT count(*) from newpower.meters"
    result: taos.TaosResult = conn.query(sql)
    data = result.fetch_all()
    nResult = 0 if data[0][0] == args.childTableCount * args.insertRows else 1
    message = ""
    if nResult == 0:
        conn.query("DROP DATABASE IF EXISTS newpower")
    else:
        print(f"Test failed: Expected {args.childTableCount * args.insertRows} rows, but found {data[0][0]} rows.")
        message = f"Test failed: Expected {args.childTableCount * args.insertRows} rows, but found {data[0][0]} rows."
    conn.close()
    return nResult, message

def make_test_result(args, exceptTime, importTime, json_config, total_run_time):
    struct_time = time.localtime(time.time())
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", struct_time)
    export_qps = float(args.childTableCount * args.insertRows) / float(exceptTime)
    import_qps = float(args.childTableCount * args.insertRows) / float(importTime)
    result_code, result_msg = check_test_result(args)
    result = {
        "ts": time_str,
        "result_code": result_code,
        "result_msg": result_msg,
        "total_run_time": f"{total_run_time} second",
        "test_name": args.caseName,
        "version": getVersion(),
        "concurrency": args.threadCount,
        "child_table_count": args.childTableCount,
        "insert_rows": args.insertRows * args.childTableCount,
        "vgroups": args.vgroups,
        "batch_size": args.batchSize,
        "config": json_config,
        "connect_mode": args.connectMode,
        "export_time": f"{exceptTime} second",
        "import_time": f"{importTime} second",
        "export_qps": f"{export_qps} records/second",
        "import_qps": f"{import_qps} records/second"
    }
    result_file = f"./result/{args.caseName}_result.json"
    with open(result_file, 'w') as f:
        json.dump(result, f, indent=4)
    print(f"Test results saved to {result_file}")


def getVersion():
    result = subprocess.run(
        "taosdump -V",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=5
    )
    return result.stdout

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TAOS Data Management Script",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # 显示默认值
    )

    parser.add_argument(
        "-name", "--caseName",
        required=True,
        help="本次测试名称，以此名称命名测试结果文件")

    # 必需参数
    parser.add_argument(
        "-T", "--threadCount",
        type=int,
        default=10,
        help="Number of worker threads for data insertion"
    )

    parser.add_argument(
        "-t", "--childTableCount",
        type=int,
        default=1000000,
        help="Number of child tables to create"
    )

    parser.add_argument(
        "-n", "--insertRows",
        type=int,
        default=1000,
        help="Number of entries written to each sub table"
    )

    parser.add_argument(
        "-v", "--vgroups",
        type=int,
        default=10,
        help="Number of vgroups for database"
    )

    parser.add_argument(
        "-b", "--batchSize",
        type=int,
        default=2000,
        help="Batch size for bulk insert operations"
    )

    parser.add_argument(
        "-A", "--tagTypes",
        default="",
        help="tag JSON 字符串")

    parser.add_argument(
        "-C", "--colTypes",
        required=True,
        help="列 JSON 字符串")

    parser.add_argument(
        "-Z", "--connectMode",
        type=str,
        choices=["native", "websocket", "0", "1"],
        default="native",
        help="TDengine connection mode"
    )

    return parser.parse_args()


if __name__ == "__main__":
    start = time.perf_counter()
    getVersion()
    args = parse_arguments()
    parth, json_config = create_benchmark_json(args)
    init_data(parth)
    exceptTime = export_data(args)
    importTime = import_data(args)
    end = time.perf_counter()
    make_test_result(args, exceptTime, importTime, json_config, end - start)
    print("All operations completed successfully.")
