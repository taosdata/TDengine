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

class TestTs3159(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb("test_ts3159")
        self.tdSql.execute('CREATE TABLE `wlwrtu` ( `ts` TIMESTAMP, `voltage` DOUBLE, `signalstrength` DOUBLE, `rsoc` DOUBLE, `databacklog` DOUBLE, `reportinginterval` DOUBLE, `longitude` DOUBLE, `productcode` NCHAR(64), `addreportinginterval` DOUBLE, `model` NCHAR(64), `iccid` NCHAR(64), `moduleversion` NCHAR(64), `powersupplymode` NCHAR(64), `workcount` DOUBLE, `latitude` DOUBLE, `hardwareversion` NCHAR(64), `firmwareversion` NCHAR(64), `case_out_humidity` NCHAR(64), `case_out_temperature` NCHAR(64), `case_in_humidity` NCHAR(64), `case_in_temperature` NCHAR(64), `pole_s_x` NCHAR(64), `pole_s_z` NCHAR(64), `pole_s_y` NCHAR(64), `box_door_status` NCHAR(64), `charge_electricity` NCHAR(64), `charge_voltage` NCHAR(64), `module_bs_1_version` NCHAR(64), `module_bs_1_location` NCHAR(64), `module_bs_1_type` NCHAR(64), `module_bs_2_location` NCHAR(64), `module_bs_3_type` NCHAR(64), `module_bs_2_type` NCHAR(64), `module_bs_3_version` NCHAR(64), `module_bs_2_version` NCHAR(64), `module_bs_3_location` NCHAR(64), `upgrade_num` DOUBLE ) TAGS ( `deviceid` NCHAR(50), `tenant` NCHAR(50), `site` NCHAR(50), `reserve1` NCHAR(50), `reserve2` NCHAR(50), `reserve3` NCHAR(50) );')
        self.tdSql.execute('CREATE TABLE `wlwrtu52000000006796` USING `wlwrtu` TAGS ("52000000006796",NULL,NULL,NULL,NULL,NULL);')
        self.tdSql.execute('insert into `wlwrtu52000000006796` (ts,signalstrength) values(now,91849208012064e-310);')
        self.tdSql.query('select ts,signalstrength,tbname from test_ts3159.wlwrtu52000000006796')
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], 9.1849208012064e-297)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts3159
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write