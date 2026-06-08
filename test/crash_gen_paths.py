# -*- coding: utf-8 -*-
"""Path helpers for crash_gen scripts in taos-community/test."""

import os


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def detect_repo():
    base_dir = get_script_dir()
    if "taos-community" in base_dir:
        return "taos-community"
    if base_dir.find("community") > 0:
        return "community"
    if base_dir.find("TDengine") > 0:
        return "TDengine"
    return "TDengine"


def get_proj_path():
    self_path = get_script_dir()
    norm_path = self_path.replace("\\", "/")
    if "taos-community" in norm_path:
        idx = norm_path.find("source/taos-community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
        idx = norm_path.find("taos-community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
    elif "community" in norm_path:
        idx = norm_path.find("community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
    elif "TDengine" in norm_path:
        idx = norm_path.find("TDengine")
        return os.path.normpath(norm_path[: idx + len("TDengine")])
    idx = norm_path.find("test")
    if idx != -1:
        return os.path.normpath(norm_path[:idx])
    return os.path.normpath(os.path.dirname(self_path))


def get_build_path():
    proj_path = get_proj_path()
    for root, _dirs, files in os.walk(proj_path):
        if "taosd" in files:
            root_real_path = os.path.dirname(os.path.realpath(root))
            if "packaging" not in root_real_path:
                return root[: len(root) - len("/build/bin")]
    return ""


def get_crash_gen_path():
    return get_script_dir() + os.sep


def get_home_dir():
    base_dir = get_script_dir()
    repo = detect_repo()
    if repo == "taos-community":
        return get_proj_path()
    return base_dir[: base_dir.find(repo)]


def get_start_taosd_cmd(dnode_nums=1, mnode_nums=0):
    script_dir = get_script_dir()
    cmd = "cd %s && python3 crash_gen_start_taosd.py" % script_dir
    if dnode_nums > 1:
        cmd += " -N %d" % dnode_nums
    if mnode_nums > 0:
        cmd += " -M %d" % mnode_nums
    return cmd
