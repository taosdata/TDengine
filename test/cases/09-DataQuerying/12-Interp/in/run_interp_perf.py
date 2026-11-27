import importlib.util
import json
import os
from pathlib import Path

import taos
from new_test_framework.utils import tdSql


def main():
    directory = Path(__file__).parent
    config = json.loads(
        (directory / "interp_table_scan_perf.json").read_text()
    )
    spec = importlib.util.spec_from_file_location(
        "interp_perf", directory.parent / "test_interp_table_scan_perf.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    connection = taos.connect(
        host=config["host"], user=config["user"], password=config["password"],
        port=int(os.environ.get("INTERP_PERF_PORT", config["port"])),
    )
    try:
        tdSql.init(connection.cursor())
        module.TestInterpTableScanPerf().do_interp_table_scan_perf()
    finally:
        tdSql.close()
        connection.close()


if __name__ == "__main__":
    main()
