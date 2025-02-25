###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import os
import time
from datetime import datetime, timedelta
class Start(TDCase):
    def init(self):
        start_time = datetime.utcnow()
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        workflow_config = self.tdCom.load_workflow_json(self._remote, f'{os.environ["TEST_ROOT"]}/env/workflow_config.json')
        print(workflow_config)
        end_time = start_time + timedelta(seconds=int(workflow_config["exec_time"]))
        url = (
            f"http://192.168.2.190:3000/d/dedq3n2zhlypsd/named-processes"
            f"?var-interval=10m&orgId=1&from={start_time.isoformat(timespec='milliseconds')}Z&to={end_time.isoformat(timespec='milliseconds')}Z"
            f"&timezone=browser&var-processes=$__all&refresh=5s"
        )
        print(url)
        pass

    def run(self) -> bool:
        pass

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            just start env;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T