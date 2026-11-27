# Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-  

import sys
import os
import time
import datetime
from .log import *

class TDSub:
	def __init__(self):
		self.consumedRows = 0
		self.consumedCols = 0

	def init(self, sub):
		self.sub = sub

	def close(self, keepProgress):
		self.sub.close(keepProgress)	

	def consume(self):
		self.result = self.sub.consume()
		self.result.fetch_all()
		self.consumedRows = self.result.row_count
		self.consumedCols = self.result.field_count
		return self.consumedRows

	def checkRows(self, expectRows):
		if self.consumedRows != expectRows:
			tdLog.exit("consumed rows:%d != expect:%d" % (self.consumedRows, expectRows))
		tdLog.info("consumed rows:%d == expect:%d" % (self.consumedRows, expectRows))
	

tdSub = TDSub()	
