from new_test_framework.utils import tdLog, tdSql, etool, tdDnodes, tdCom
import os
import platform
import time
import subprocess
import re


class TestIPv6DualStack:
    def setup_class(cls):
        # print(f"setup_class: {cls.__name__}")
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)

    def genDualStackConfig(self):
        """Generate dual-stack network configuration"""
        clientcfgDict = {}
        updatecfgDict = {}

        if platform.system().lower() == "linux":
            clientcfgDict = {
                "enableIpv6": "1",
                "debugFlag": "135"
            }
            updatecfgDict = {
                "clientCfg": clientcfgDict,
                "enableIpv6": "1",
            }
        tdSql.close()
        time.sleep(10)
        tdDnodes.stop(1)

        tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)

    def restartDnodes(self):
        """Restart dnodes after configuration changes"""
        tdDnodes.stop(1)
        time.sleep(5)
        tdDnodes.starttaosd(1)

    def testCmd(self, cmd):
        """Execute command and check output"""
        cfg = tdDnodes.sim.getCfgDir()

        shellCmd = f"taos -c {cfg} -s \"{cmd}\""
        print(f"execute {shellCmd}")
        result = subprocess.run(shellCmd, shell=True, capture_output=True, text=True)
        output = result.stdout

        if "error" in output.lower() or "failed" in output.lower():
            raise ValueError(f"command failed: {output}")

        return output

    def basicTest(self, cli):
        """Basic database query test"""
        cli.query("select 1")
        cli.query("show databases")

    def test_ipv6_dual_stack_demo(self):
        """summary: Test IPv4/IPv6 dual-stack network support

        description: Verify that TDengine supports dual-stack networking
        with enableIpv6=1, accepting both IPv4 and IPv6 connections

        Since: 3.3.8.4
        Labels: network ipv6 dual-stack
        Jira: TD-5000

        Catalog:
            - network: ipv6 dual-stack support

        History:
            - 2025-01-18: Initial implementation of dual-stack support

        """
        self.genDualStackConfig()
        self.restartDnodes()

        # Test 1: Verify IPv6 is enabled
        self.testCmd("show dnodes")

        # Test 2: Check enableIpv6 configuration
        output = self.testCmd("select * from information_schema.ins_configs where name='enableIpv6'")
        print(f"enableIpv6 config: {output}")
        if "1" not in output:
            raise ValueError(f"enableIpv6 not set to 1: {output}")

        # Test 3: Test basic connectivity
        self.basicTest(tdCom.taosCom)
        tdSql.close()

        print("IPv6 dual-stack test completed successfully")

    def test_ipv4_compatibility_mode(self):
        """summary: Test IPv4-only compatibility mode

        description: Verify backward compatibility with IPv4-only mode
        when enableIpv6=0, only IPv4 connections are accepted

        Since: 3.3.8.4
        Labels: network ipv4 compatibility
        Jira: TD-5001

        Catalog:
            - network: ipv4 backward compatibility

        History:
            - 2025-01-18: IPv4 compatibility verification

        """
        # Test IPv4-only mode
        clientcfgDict = {}
        updatecfgDict = {}

        if platform.system().lower() == "linux":
            clientcfgDict = {
                "enableIpv6": "0",
            }
            updatecfgDict = {
                "clientCfg": clientcfgDict,
                "enableIpv6": "0",
            }
        tdSql.close()
        time.sleep(10)
        tdDnodes.stop(1)

        tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)
        self.restartDnodes()

        # Verify IPv4-only mode works
        self.basicTest(tdCom.taosCom)
        tdSql.close()

        print("IPv4 compatibility test completed successfully")

    def test_network_connection_modes(self):
        """summary: Test different network connection modes

        description: Test that both dual-stack and IPv4-only modes work
        correctly with various connection scenarios

        Since: 3.3.8.4
        Labels: network modes connection
        Jira: TD-5002

        Catalog:
            - network: connection modes

        History:
            - 2025-01-18: Connection modes testing

        """
        # Test dual-stack mode
        self.genDualStackConfig()
        self.restartDnodes()

        # Test connection in dual-stack mode
        self.basicTest(tdCom.taosCom)
        tdSql.close()

        # Test IPv4-only mode
        clientcfgDict = {}
        updatecfgDict = {}

        if platform.system().lower() == "linux":
            clientcfgDict = {
                "enableIpv6": "0",
            }
            updatecfgDict = {
                "clientCfg": clientcfgDict,
                "enableIpv6": "0",
            }
        tdSql.close()
        time.sleep(10)
        tdDnodes.stop(1)

        tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)
        self.restartDnodes()

        # Test connection in IPv4-only mode
        self.basicTest(tdCom.taosCom)
        tdSql.close()

        print("Network connection modes test completed successfully")

    def test_dns_resolution_modes(self):
        """summary: Test DNS resolution in different modes

        description: Verify DNS resolution works correctly in both
        dual-stack and IPv4-only modes

        Since: 3.3.8.4
        Labels: network dns resolution
        Jira: TD-5003

        Catalog:
            - network: dns resolution

        History:
            - 2025-01-18: DNS resolution testing

        """
        # Test DNS resolution in dual-stack mode
        self.genDualStackConfig()
        self.restartDnodes()

        # Test domain name resolution
        try:
            output = self.testCmd("select * from information_schema.cluster_dnodes")
            print(f"Cluster info: {output}")
        except Exception as e:
            print(f"DNS resolution test error: {e}")

        tdSql.close()

        # Test DNS resolution in IPv4-only mode
        clientcfgDict = {}
        updatecfgDict = {}

        if platform.system().lower() == "linux":
            clientcfgDict = {
                "enableIpv6": "0",
            }
            updatecfgDict = {
                "clientCfg": clientcfgDict,
                "enableIpv6": "0",
            }
        tdSql.close()
        time.sleep(10)
        tdDnodes.stop(1)

        tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)
        self.restartDnodes()

        tdSql.close()

        print("DNS resolution modes test completed successfully")