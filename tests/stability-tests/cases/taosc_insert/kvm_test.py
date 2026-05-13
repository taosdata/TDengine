from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.components.kvm import KVMAPI
from taostest.util.remote import Remote


class TestKVM(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.KVM = KVMAPI(self._remote, self.env_setting)
        self.conn = self.KVM.conn

    def test(self):
        NODE_cpu_nums = self.conn.getCPUMap()[0]
        print(NODE_cpu_nums)
        NODE_free_memory = self.conn.getFreeMemory() / (1024 ** 2)  # Mb
        print(NODE_free_memory)
        print(self.KVM.domstate("node_222"))
        print(self.KVM.get_all_kvm_list())
        print(self.KVM.get_running_kvm_list())
        print(self.KVM.start_kvm("node_222"))

    def run(self) -> bool:
        self.test()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            vgroups check <jayden>: [TD-14991] : vgroups check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter
