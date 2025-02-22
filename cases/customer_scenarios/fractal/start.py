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

class Start(TDCase):
    def init(self):
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