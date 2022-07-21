import os
from taostest.util import caseutil


class CloudCase:

    def set_env(self):
        os.environ.update(self.env_setting["env"])
        if self.case_param:
            param = caseutil.parse_param(self.case_param)
            if 'url' in param and 'token' in param:
                os.environ["TDENGINE_CLOUD_URL"] = param['url']
                os.environ["TDENGINE_CLOUD_TOKEN"] = param['token']
                os.environ["TDENGINE_CLOUD_DSN"] = param['url'] + "?token=" + param['token']
