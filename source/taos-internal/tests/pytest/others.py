###################################################################
 #           Copyright (c) 2016 by TAOS Technologies, Inc.
 #                     All rights reserved.
 #
 #  This file is proprietary and confidential to TAOS Technologies.
 #  No part of this file may be reproduced, stored, transmitted, 
 #  disclosed or used in any form or by any means other than as 
 #  expressly provided by the written permission from Jianhui Tao
 #
###################################################################

# pip install driver/Python/python2/
# python2 ubuntu.sim
# python2 ubuntu.sim -f query/basic

# -*- coding: utf-8 -*-  

import sys
from util.log import *

if __name__=="__main__":
	# assert a tdengine is runnig
	tdLog.Notice("http test script")
	tdLog.PrintNoPrefix("deploy tdengine first")
	tdLog.PrintNoPrefix("go/httpTest_insert")
	tdLog.PrintNoPrefix("wait 20 seconds")
	tdLog.PrintNoPrefix("go/httpTest_query_st_all")
	tdLog.PrintNoPrefix("go/httpTest_query_st_in")
	tdLog.PrintNoPrefix("wait about 30 minutes, and see output of cmd 'top', then:")
	tdLog.PrintNoPrefix("curl -H 'Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04' -d 'select * from db.t1' 127.0.0.1:6020/rest/sql")
	