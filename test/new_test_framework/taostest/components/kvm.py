from typing import Dict

from ..util.file import dict2yaml
from ..util.remote import Remote
from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame, MetricRangeDataFrame
import datetime
import pandas as pd
import socket
import re
from collections import defaultdict
import libvirt
from ..util.common import TDCom
import time
from ..util.common import TDSql


# vm info
class VMinfo:

    def __init__(self):
        self.NAME = ''  # vm name
        self.IP = ''
        self.cpu_count = 0  # cpu count
        self.cpurate = 0  # cpu rate
        self.memoryrate = 0  # mem rate
        self.memory = 0  # mem
        self.stats = ''
        self.node = 0


class KVMAPI:

    def __init__(self, remote: Remote, env_settings: dict):
        self.kvm_config = str()
        env_setting = env_settings["settings"]
        for setting in env_setting:
            if setting["name"] == "kvm":
                self.kvm_config = setting
        self.kvm_fqdn = self.kvm_config["fqdn"]
        self.kvm_img_dir = self.kvm_config["img_dir"]
        self.kvm_net = f'qemu+ssh://root@{self.kvm_fqdn}/system'
        self.conn = libvirt.open(self.kvm_net)
        self._remote: Remote = remote

        self._fqdn: str = None


        self.shutdown_wait = 5

    def create_img(self, name="test.qcow2", capacity="10G"):
        """ qemu-img create -f qcow2 test.qcow2 10G
        :param name: [image name], defaults to "test.qcow2"
        :type name: str, optional
        :param capacity: [image capacity], defaults to "10G"
        :type capacity: str, optional
        """
        self._remote.cmd(self.kvm_fqdn, [
            f'qemu-img create -f qcow2 {self.kvm_img_dir}/{str(name)} {capacity}'
        ])


    # def create_vm(self, cpu_count=1, mem=4096):
    def domstate(self, vm_name):
        """ virsh domstate vm
        :param vm_name: vm name
        :type vm_name: str
        :return: vm status
        :rtype: str
        """
        return self._remote.cmd(self.kvm_fqdn, [f'virsh domstate {vm_name}'])

    def get_all_kvm_list(self):
        """ virsh list --all
        :return: [virsh list --all]
        :rtype: [list]
        """
        kvm_list = list()
        res = self._remote.cmd(self.kvm_fqdn, [f'virsh list --all'])
        for kvm_info in res.split('\n')[2:]:
            kvm_dict = dict()
            tmp_l = [i for i in kvm_info.split(" ") if i != ""]
            kvm_dict["id"] = tmp_l[0]
            kvm_dict["name"] = tmp_l[1]
            kvm_dict["status"] = tmp_l[2]
            kvm_list.append(kvm_dict)
        return kvm_list

    def get_running_kvm_list(self):
        """ virsh list
        :return: [virsh list]
        :rtype: [list]
        """
        all_kvm_list = self.get_all_kvm_list()
        return [i for i in all_kvm_list if i['status'] == "running"]

    def start_kvm(self, vm_name):
        """ virsh start vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        vm_status = self.domstate(vm_name)
        if vm_status == "shut off" or vm_status == "关闭":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh start {vm_name}'])
            if res == f'Domain {vm_name} started' or f'域 {vm_name} 已开始':
                self._remote._logger.info(f'Start Domain {vm_name} successful')
            else:
                self._remote._logger.info(f'Start Domain {vm_name} failed')
        else:
            self._remote._logger.info(f'Domain {vm_name} is already active')

    def shutdown_kvm(self, vm_name):
        """ virsh shutdown vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        vm_status = self.domstate(vm_name)
        if vm_status == "running" or vm_status == "运行中":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh shutdown {vm_name}'])
            if res == f'Domain {vm_name} is being shutdown' or f'域 {vm_name} 被关闭':
                time.sleep(self.shutdown_wait)
                state = self.domstate(vm_name)
                if state == "shut off" or state == "关闭":
                    self._remote._logger.info(f'Shutdown Domain {vm_name} successful')
                else:
                    self._remote._logger.info(f'Shutdown Domain {vm_name} failed')
            else:
                self._remote._logger.info(f'Shutdown Domain {vm_name} failed')
        # elif vm_status == "paused" or vm_status == "暂停":
        #     self._remote._logger.info(f'Domain {vm_name} is not running')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not running')

    def destroy_kvm(self, vm_name):
        """ virsh destroy vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        # pow off vm if running or paused
        vm_status = self.domstate(vm_name)
        if vm_status == "running" or vm_status == "运行中" or vm_status == "paused" or vm_status == "暂停":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh destroy {vm_name}'])
            if res == f'Domain {vm_name} destroyed' or f'域 {vm_name} 被删除':
                self._remote._logger.info(f'Power off Domain {vm_name} successful')
            else:
                self._remote._logger.info(f'Power off Domain {vm_name} failed')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not running')

    def get_snapshot_list(self, vm_name):
        snapshot_list = list()
        res = self._remote.cmd(self.kvm_fqdn, [f'virsh snapshot-list {vm_name}'])
        for snapshot_info in res[2:]:
            snapshot_dict = dict()
            tmp_l = [i for i in snapshot_info.split(" ") if i != ""]
            snapshot_dict["name"] = tmp_l[0]
            snapshot_list.append(snapshot_dict)
        return snapshot_list

    def undefine_kvm(self, vm_name):
        """ virsh undefine vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        # undefine vm if shut off
        vm_status = self.domstate(vm_name)
        if vm_status == "shut off" or vm_status == "关闭":
            snapshot_list = self.get_snapshot_list(vm_name)
            if not snapshot_list:
                try:
                    res = self._remote.cmd(self.kvm_fqdn, [f'virsh undefine {vm_name}'])
                    if res == f'Domain {vm_name} has been undefined' or f'域 {vm_name} 已经被取消定义':
                        self._remote._logger.info(f'undefine Domain {vm_name} successful')
                    else:
                        self._remote._logger.info(f'undefine Domain {vm_name} failed')
                except Exception as e:
                    self._remote._logger.info(e)
            else:
                self._remote._logger.info(f'Domain {vm_name} has existed snapshot and unable to delete')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not running')

    def suspend_kvm(self, vm_name):
        """ virsh suspend vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        # pause vm if running or paused
        vm_status = self.domstate(vm_name)
        if vm_status == "running" or vm_status == "运行中" or vm_status == "paused" or vm_status == "暂停":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh suspend {vm_name}'])
            if res == f'Domain {vm_name} suspended' or f'域 {vm_name} 被挂起':
                self._remote._logger.info(f'Pause Domain {vm_name} successful')
            else:
                self._remote._logger.info(f'Pause Domain {vm_name} failed')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not running')

    def resume_kvm(self, vm_name):
        """ virsh resume vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        # resume vm if paused
        vm_status = self.domstate(vm_name)
        if vm_status == "paused" or vm_status == "暂停":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh resume {vm_name}'])
            if res == f'Domain {vm_name} resumed' or f'域 {vm_name} 被重新恢复':
                self._remote._logger.info(f'Resume Domain {vm_name} successful')
            else:
                self._remote._logger.info(f'Resume Domain {vm_name} failed')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not paused state')

    def reboot_kvm(self, vm_name):
        """ virsh reboot vm

        :param vm_name: [vm name]
        :type vm_name: [str]
        """
        # pause vm if running or paused
        vm_status = self.domstate(vm_name)
        if vm_status == "running" or vm_status == "运行中" or vm_status == "paused" or vm_status == "暂停":
            res = self._remote.cmd(self.kvm_fqdn, [f'virsh reboot {vm_name}'])
            if res == f'Domain {vm_name} is being rebooted' or f'域 {vm_name} 正在重新启动':
                self._remote._logger.info(f'Reboot Domain {vm_name} successful')
            else:
                self._remote._logger.info(f'Reboot Domain {vm_name} failed')
        else:
            self._remote._logger.info(f'Domain {vm_name} is not running')

    def clone_kvm(self, vm_name, new_vm_name, vm_path):
        vm_status = self.domstate(vm_name)
        if vm_status == "shut off" or vm_status == "关闭":
