#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deploy and start taosd for crash_gen without running test cases."""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import time

import taos

from crash_gen_paths import get_build_path, get_proj_path


def _get_bin_path():
    build_path = get_build_path()
    if not build_path:
        raise RuntimeError("taosd binary not found")
    return os.path.join(build_path, "build", "bin", "taosd")


def _get_work_dir():
    build_path = get_build_path()
    if build_path:
        path = os.path.realpath(build_path)
        while path and path != os.path.dirname(path):
            if os.path.basename(path) == "debug":
                return os.path.join(os.path.dirname(path), "sim")
            path = os.path.dirname(path)
    return os.path.join(get_proj_path(), "sim")


def _stop_taosd():
    subprocess.run(
        "ps -ef|grep -w taosd|grep -v grep|awk '{print $2}'|xargs -r kill -TERM",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)


def _write_cfg(cfg_path, options):
    with open(cfg_path, "w", encoding="utf-8") as cfg_file:
        for key, value in options.items():
            if isinstance(value, list):
                for item in value:
                    cfg_file.write(f"{key} {item}\n")
            else:
                cfg_file.write(f"{key} {value}\n")


def _deploy_dnode(work_dir, index, port, hostname):
    dnode_dir = os.path.join(work_dir, f"dnode{index}")
    for sub in ("cfg", "data", "log"):
        os.makedirs(os.path.join(dnode_dir, sub), exist_ok=True)
    cfg_path = os.path.join(dnode_dir, "cfg", "taos.cfg")
    _write_cfg(
        cfg_path,
        {
            "fqdn": hostname,
            "serverPort": str(port),
            "firstEp": f"{hostname}:{port}",
            "secondEp": f"{hostname}:{port + 100}",
            "dataDir": os.path.join(dnode_dir, "data"),
            "logDir": os.path.join(dnode_dir, "log"),
            "monitor": "0",
            "asyncLog": "0",
            "telemetryReporting": "0",
            "supportVnodes": "1024",
        },
    )
    return dnode_dir


def _start_taosd(bin_path, cfg_dir):
    cmd = f"nohup {bin_path} -c {cfg_dir} > /dev/null 2>&1 &"
    if os.system(cmd) != 0:
        raise RuntimeError(f"failed to start taosd: {cmd}")
    time.sleep(1)


def start_single_dnode(work_dir, bin_path):
    _stop_taosd()
    hostname = socket.gethostname()
    dnode_dir = _deploy_dnode(work_dir, 1, 6030, hostname)
    _start_taosd(bin_path, os.path.join(dnode_dir, "cfg"))


def start_cluster(work_dir, bin_path, dnode_nums, mnode_nums):
    _stop_taosd()
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir, exist_ok=True)

    hostname = socket.gethostname()
    dnode_dirs = []
    for index in range(1, dnode_nums + 1):
        port = 6030 + (index - 1) * 100
        dnode_dirs.append(_deploy_dnode(work_dir, index, port, hostname))

    for dnode_dir in dnode_dirs:
        _start_taosd(bin_path, os.path.join(dnode_dir, "cfg"))

    conn = taos.connect(host=hostname, config=os.path.join(dnode_dirs[0], "cfg"))
    cursor = conn.cursor()
    for index in range(2, dnode_nums + 1):
        port = 6030 + (index - 1) * 100
        cursor.execute(f"create dnode '{hostname}:{port}'")
    for index in range(2, mnode_nums + 1):
        cursor.execute(f"create mnode on dnode {index}")
    for _ in range(10):
        cursor.execute("select * from information_schema.ins_dnodes")
        rows = cursor.fetchall()
        if len(rows) >= dnode_nums and all(row[4] == "ready" for row in rows[:dnode_nums]):
            break
        time.sleep(1)
    else:
        raise RuntimeError("cluster dnodes not ready")
    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Start taosd for crash_gen")
    parser.add_argument("-N", "--dnodeNums", type=int, default=1, help="number of dnodes")
    parser.add_argument("-M", "--mnodeNums", type=int, default=0, help="number of mnodes")
    args = parser.parse_args()

    bin_path = _get_bin_path()
    work_dir = _get_work_dir()
    os.makedirs(work_dir, exist_ok=True)

    if args.dnodeNums <= 1:
        start_single_dnode(work_dir, bin_path)
    else:
        start_cluster(work_dir, bin_path, args.dnodeNums, args.mnodeNums)


if __name__ == "__main__":
    main()
