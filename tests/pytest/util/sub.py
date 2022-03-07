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

import sys
import os
import time
import datetime
from util.log import *

class TDSub:
	def __init__(self):
		self.consumedRows = 0
		self.consumedCols = 0

	def init(self, sub):
		self.sub = sub

	def close(self, keepProgress):
		self.sub.close(keepProgress)	

	def consume(self):
		self.data = self.sub.consume()
		self.consumedRows = len(self.data)
		self.consumedCols = len(self.sub.fields)
		return self.consumedRows

	def checkRows(self, expectRows):
		if self.consumedRows != expectRows:
			tdLog.exit("consumed rows:%d != expect:%d" % (self.consumedRows, expectRows))
		tdLog.info("consumed rows:%d == expect:%d" % (self.consumedRows, expectRows))
	

tdSub = TDSub()	
