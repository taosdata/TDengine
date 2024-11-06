import logging
import time

import pytest

from testng_taosx.env import ENV
from testng_taosx.task import Task
from testng_taosx.util import Util, TaosAdapter

conftest_logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def global_setup(request):
    conftest_logger.info("before all tests...")

    conftest_logger.info("delete all tasks...")
    env_data = Util.get_env_data()
    task = Task(env_data, None)
    # task.delete_all_tasks()
    start_time = int(time.time() * 1000)

    yield env_data

    conftest_logger.info("after all tests...")
    conftest_logger.info("delete all ci dbs...")
    # TaosAdapter.drop_ci_topics(env_data["taosd_host"])
    # TaosAdapter.drop_ci_dbs(env_data["taosd_host"])

    # For performance test, clean up database and send result to feishu
    is_perf_test = False
    for item in request.session.items:
        markers = item.own_markers
        for marker in markers:
            if marker.name == "performance":
                conftest_logger.info(
                    f"Found marker '{marker.name}' in test {item.nodeid}"
                )
                is_perf_test = True
                break
    if is_perf_test == True:
        log_host = ENV.taosd_log_host
        sql_str = f"""SELECT * FROM (
                      SELECT datasource,branch, scenario_id, last(end_time) as exec_time,
                             last(rows_per_second) as rows_per_second, last(points_per_second) as points_per_second
                      FROM  taosx_datain_perf.perf_result where end_time >= {start_time} GROUP by TBNAME
                  ) t order by datasource;"""
        result = TaosAdapter.run_sql(log_host, sql_str)
        assert result["code"] == 0, f"fail to get performance result: {result['desc']}"

        table_content = "Performance Test Result: \n"
        for header in result["column_meta"]:
            table_content += str(header[0]) + ","
        table_content = table_content.strip(",") + "\n"
        for row in result["data"]:
            table_content += str(row) + "\n"
        Util.send_message_to_feishu(table_content)
        # clean up database
        TaosAdapter.drop_ci_dbs(env_data["taosd_host"], "perf_")
