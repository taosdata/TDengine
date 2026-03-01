import logging
import time

import pytest

from testng_taosx.env import ENV
from testng_taosx.task import Task
from testng_taosx.util import Util, TaosAdapter
from packaging import version

setup_logger = logging.getLogger(__name__)

def test_setup():
    env_data = Util.get_env_data()
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    env_data = Util.get_env_data()
    task = Task(env_data, None)
    setup_logger.info("delete all tasks...")
    task.delete_all_tasks()

def test_teardown():
    env_data = Util.get_env_data()
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    env_data = Util.get_env_data()
    TaosAdapter.drop_ci_topics(env_data["taosd_host"])
    TaosAdapter.drop_ci_dbs(env_data["taosd_host"])