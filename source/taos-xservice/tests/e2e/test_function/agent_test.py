import logging
import pytest
import time
import re

from testng_taosx.agent import *
from testng_taosx.util import Util
from packaging import version


@pytest.mark.sanity
def test_create_agent():
    env_data = Util.get_env_data()
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    agent = Agent.create_agent("agent_test")
    assert agent
    time.sleep(2)
    agent_status = agent.get_status()
    assert agent_status == "created", f"agent_status is {agent_status}"
    agent.delete()


@pytest.mark.skip
def test_stop_start_windows_agent():
    env_data = Util.get_env_data()
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    "测试 windows agent 的相关操作，正常 CI 不必执行该用例，涉及到的函数会分散到其他用例里面执行"
    Agent.stop_windows_agent()
    print("start windows agent")
    Agent.start_windows_agent()


def test_re():
    env_data = Util.get_env_data()
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    resp = "Agent 10 is not alive"
    print(re.search(r"Agent \d* is not alive", resp))
    print(re.findall(r"\d+\.?\d*", resp))
