# Note: The current test script needs to manually deploy taosd and taosadapter according to the environment description in the test report
- Test Report: https://jira.taosdata.com:18090/pages/viewpage.action?pageId=127470422
# 1. Single instance centralized deployment 
## 1.1 Environment deployment: directly compile the develop branch on 192.168.1.85 and start taosd
## 1.2 Script running method: Run python3 /home/ubuntu/TDengine/tests/perftest-scripts/taosadapter_perftest/taosadapter_perftest.py directly in the 192.168.1.83 environment, which can output the test results in 3.4.1 and 3.5.1 in the test report. The results will be printed in real time to taosadapter_perftest.log in the same directory


# 2. Separate deployment of multiple instances
## 2.1 Environment deployment: Deploy taosd on 192.168.1.98, deploy taosadapter in 192.168.1.83, 84, 85, 86 environments, configure the corresponding /etc/hosts, and point the firstEp in /etc/taos/taos.cfg to taosd Address (vm98:6030)
## 2.2 Script running method: 
- Run according to the count of nodes in the 192.168.1.93 environment:
- jmeter -n -t opentsdb_insertRows83.jmx
- jmeter -n -t opentsdb_insertRows84.jmx
- jmeter -n -t opentsdb_insertRows85.jmx
- ...
- You need to monitor the CPU occupancy of each node in real time, and the jmeter results will be printed directly to the console.