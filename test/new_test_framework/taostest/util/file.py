from typing import Dict, Any

import yaml
import toml
import os


def read_yaml(path: str):
    with open(path, "r") as f:
        return yaml.full_load(f)


def dict2toml(path: str, file_name: str, data: Dict[str, Any]) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
    file = os.path.join(path, file_name)
    with open(file, "w") as f:
        toml.dump(data, f)


def dict2file(path, file_name, data: Dict) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
    file = os.path.join(path, file_name)
    with open(file, "w") as f:
        for k, v in data.items():
            if type(v) == list:
                for value in v:
                    f.write(f"{k}\t{value}\n")
            else:
                f.write(f"{k}\t{v}\n")


def dict2yaml(data: Dict, path: str, file_name: str):
    if not os.path.exists(path):
        os.makedirs(path)
    file = os.path.join(path, file_name)
    with open(file, "w", encoding="utf8") as f:
        yaml.dump(data, f)


def file2dict(path, file_name, delimiter="\t"):
    file = os.path.join(path, file_name)
    result = {}
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            ps = line.split(delimiter)
            if len(ps) == 2:
                result[ps[0]] = ps[1]
    return result
