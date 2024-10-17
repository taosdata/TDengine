import logging
import pytest
import time
import re

from testng_taosx.agent import *


@pytest.mark.sanity
def test_create_agent():
    agent = Agent.create_agent("agent_test")
    assert agent
    time.sleep(2)
    agent_status = agent.get_status()
    assert agent_status == "created", f"agent_status is {agent_status}"
    agent.delete()


@pytest.mark.skip
def test_stop_start_windows_agent():
    "测试 windows agent 的相关操作，正常 CI 不必执行该用例，涉及到的函数会分散到其他用例里面执行"
    Agent.stop_windows_agent()
    print("start windows agent")
    Agent.start_windows_agent()


def test_re():
    resp = "Agent 10 is not alive"
    print(re.search(r"Agent \d* is not alive", resp))
    print(re.findall(r"\d+\.?\d*", resp))
