###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


import time
import taos
conn = taos.connect()
stb_name = "test_crash"
conn.execute("use test")
conn.execute(f"select * from {stb_name}")
time.sleep(4)
conn.execute(f"select * from {stb_name}")
