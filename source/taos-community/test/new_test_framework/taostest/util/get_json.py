


from taostest.util.remote import Remote

class GetJson:
    def __init__(self, logger, run_log_dir,env_setting=None):
        self.logger = logger
        self.run_log_dir = run_log_dir
        self.env_setting = env_setting
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.fqdn = self.taosd_setting["fqdn"][0]
                self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        
    def get_vnode_json(self,db_vnode_kv_dict):
        remote = Remote(self.logger)
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + f"/vnode/vnode{db_vnode_kv_dict[0][0]}"
        remote.get(self.fqdn,f'{self.vnode_dir}/vnode.json',f'{self.run_log_dir}/vnode.json')
        file = open(f'{self.run_log_dir}/vnode.json')
        return file