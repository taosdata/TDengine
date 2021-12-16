from config.env_init import *
from src.common.common import Common
from src.common.dnodes import Dnodes
from src.common.monitor import Monitor
from src.util.jmeter import Jmeter

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_dir = os.path.join(current_dir, 'log')
    COM = Common()
    # COM.exec_local_cmd(f'rm -rf {log_dir}/*')
    DNODES = Dnodes()
    MONITOR = Monitor()
    JMETER = Jmeter() 
    if config['deploy_mode'] == "auto":
        if config['taosd_autodeploy']:
            DNODES.deployNodes()
        if config["prometheus"]["autodeploy"]:
            MONITOR.deployAllNodeExporters()
            MONITOR.deployAllProcessExporters()
            MONITOR.deployPrometheus()
            MONITOR.deployGrafana()
        if config["jmeter"]["autodeploy"]:
            JMETER.deployJmeter()
    COM.runJmeter()


