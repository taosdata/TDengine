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

# -*- coding: utf-8 -*-  

from ctypes import *
from operator import itemgetter, attrgetter
import sys
import threading
import struct
from multiprocessing import *
import os
import time
import os.path
import datetime
import getopt

# dynamic library of TDengine client
# for linux
TaosModule = CDLL('libtaos.so')
# for windows
# TaosModule = CDLL('taos.dll')

# initialize configuration of TDengine client
# type 0 locale
# type 1 timezone
# type 2 config directory
TaosModule.taos_options(2, "/etc/taos")

# metadata which contain the types and properties of the columns in a ResultSet object.
# type 1 BOOL
# type 2 TINYINT
# type 3 SMALLINT
# type 4 INT
# type 5 BIGINT
# type 6 FLOAT
# type 7 DOUBLE
# type 8 BINARY
# type 9 TIMESTAMP
# type 10 NCHAR
class TaosField:
	def __init__(self, index, name, bytes, type):
		self.index = index
		self.name = name
		self.bytes = bytes
		self.type = type
   
	def Cast(self, valuePtr):
		# dispose null object
		if (valuePtr[0] == 0):
			return "NULL"
			
		if (self.type == 1):
			value = cast(valuePtr[0], POINTER(c_bool))[0]
		elif (self.type == 2):
			value = cast(valuePtr[0], POINTER(c_byte))[0]
		elif (self.type == 3):
			value = cast(valuePtr[0], POINTER(c_short))[0]
		elif (self.type == 4):
			value = cast(valuePtr[0], POINTER(c_int))[0]
		elif (self.type == 5):
			value = cast(valuePtr[0], POINTER(c_long))[0]
		elif (self.type == 6):
			value = cast(valuePtr[0], POINTER(c_float))[0]
		elif (self.type == 7):
			value = cast(valuePtr[0], POINTER(c_double))[0]
		elif (self.type == 8):
			value = cast(valuePtr[0], POINTER(c_char))[0:(self.bytes)]
		elif (self.type == 9):
			value = cast(valuePtr[0], POINTER(c_long))[0]
		elif (self.type == 10):
			value = cast(valuePtr[0], POINTER(c_char))[0:(self.bytes)]
			
		return (value)	

	def Print(self):
		print "field:%d, name:%s, bytes:%d, type:%d" % (self.index, self.name, self.bytes, self.type)
		
# The following code fragment show the usage of export functions of TDengine client  
class TaosConnection:
	def __init__(self, host, user, password):	
		self.host = host
		self.user = user
		self.password = password
		self.db = ""
		self.port = 0
		
		TaosModule.taos_init()
		print "TDengine Initialization finished"
		
	def Connect(self):
		self.connect = TaosModule.taos_connect(self.host, self.user, self.password, self.db, self.port)
		
		if (self.connect == 0):
			print "connect to TDengine failed"
			sys.exit(1)
		else:
			print "connect to TDengine success"
			
		return (self.connect)
		
	def Query(self, sql):
		return TaosModule.taos_query(self.connect, sql)		
		
	def FieldsCount(self):
		return TaosModule.taos_field_count(self.connect)
		
	def FetchFields(self):			
		fieldCount = TaosModule.taos_field_count(self.connect)
		result = TaosModule.taos_use_result(self.connect)
		ptrFields = TaosModule.taos_fetch_fields(result)
		fields = []
		
		for i in range(fieldCount):
			ptr = ptrFields + i * 68
			field = TaosField(i, cast(ptr, POINTER(c_char))[0 : 63], cast(ptr + 64, POINTER(c_short))[0], cast(ptr + 66, POINTER(c_byte))[0])
			fields.append(field)		
			
		return fields
			
	def UseResult(self):
		return TaosModule.taos_use_result(self.connect)
	
	def AffectedRows(self):
		return TaosModule.taos_affected_rows(self.connect)
		
	def FetchRow(self, result):
		return TaosModule.taos_fetch_row(result)
		
	def FreeResult(self, result):
		if (result != 0):
			TaosModule.taos_free_result(result)

	def Close(self):
		print "close TDengine connect"
		TaosModule.taos_close(self.connect)		
	
	def ErrorMsg(self):
		TaosModule.taos_errstr.restype = c_char_p
		return TaosModule.taos_errstr(self.connect)	

# The following code fragment give an example of insert and query data in TDegine
# call these functions in main function
class GwFile:
	def __init__(self, fileDate, fileName):
		self.fileDate = fileDate
		self.fileName = fileName
		
class GwDatabase:
	def __init__(self, dbName, replica):
		self.dbName = dbName
		self.replica = replica
		
	def CreateSql(self):
		sql = "create database if not exists %s replica %s rows 400000 cache 204800 ablocks 2000 tblocks 2000 tables 50" % (self.dbName, self.replica)
		return sql	
		
	def UseSql(self):
		sql = "use " + self.dbName
		return sql

class GwMetrics:
	def __init__(self, columns, metrics):
		self.columns = columns
		self.metrics = metrics
			
	def CreateSql(self):
		sql = "create table if not exists %s(ts timestamp" % (self.metrics)
		for i in range(3, len(self.columns)):
			sql += ", " + self.columns[i] + " float"
		sql += ") tags(wfid int, wtid int)"		
		return sql

class GwTable:
	def __init__(self, wfid, wtid, prefix, metrics):
		self.wfid = wfid
		self.wtid = wtid
		self.prefix = prefix
		self.metrics = metrics
	
	def CreateSql(self):
		sql = "create table if not exists %s%s using %s tags(%s, %s)" % (self.prefix, self.wtid, self.metrics, self.wfid, self.wtid)
		return sql

class GwData:
	def __init__(self, taos, csvFileName, databaseName, metricName, replica):
		self.taos = taos
		self.csvFileName = csvFileName
		self.metricName = metricName
		self.db = GwDatabase(databaseName, replica)
		
		self.tablePrefix = "t"
		self.csvFiles = []
		self.columnSize = 0
		self.tables = {}
		
		self.sql = "insert into"
		self.lastTable = ""
		self.lastTimestamp = ""
		self.parsedLines = 0
		self.insertLines = 0
		self.errorLines = 0
		self.parseFinished = 0
		
	def ParseFile(self):
		if os.path.isdir(self.csvFileName):
			list = os.listdir(self.csvFileName) 
			for i in range(len(list)): 
				if list[i].endswith(".csv"):
					fileName = os.path.join(self.csvFileName, list[i])
					fileDate = fileName[len(fileName) - 12 : len(fileName) - 4]
					gwfile = GwFile(fileDate, fileName)
					self.csvFiles.append(gwfile)
			print "start to dispose %d files in %s" % (len(self.csvFiles), self.csvFileName)
		else:
			if self.csvFileName.endswith(".csv"):
				fileName = self.csvFileName
				fileDate = fileName[len(fileName) - 12 : len(fileName) - 4]
				gwfile = GwFile(fileDate, fileName)
				self.csvFiles.append(gwfile)
			print "start to dispose %s" % (self.csvFileName	)
		
		if len(self.csvFiles) == 0:
			print "can not find any files in %s" % (self.csvFileName)
			sys.exit(1)
			
	def SortCsvFiles(self):
		self.csvFiles = sorted(self.csvFiles, key = attrgetter("fileDate"))	
		self.csvFiles = sorted(self.csvFiles, key = attrgetter("fileName"))	
		
	def ParseData(self):
		for i in range(len(self.csvFiles)): 
			filename = self.csvFiles[i].fileName
			print "%s parse file:%s, index:%d" % (datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S'), filename, i + 1)
			
			f = open(self.csvFiles[i].fileName, 'r')
			lines = f.readlines()			
			if len(lines) < 1:
				print "file:%s is empty" % (filename)
			
			self.ParseSchemaLine(lines[0])					
			for j in range(1, len(lines)): 
				self.ParseDataLine(lines[j])	
				self.parsedLines += 1
				
		self.parseFinished = 1
		if self.sql != "insert into":
			self.InsertData()
	
	def ParseSchemaLine(self, line):
		line = line.replace(".", "_")
		columns = line.split(',')		
		if self.FirstAccess():
			self.CreateDatabase()
			self.UseDatabase()
			self.CreateMetrics(columns)		
		self.AssertColumnSize(len(columns))
			
	def ParseDataLine(self, line):
		columns = line.split(',')
		self.AssertColumnSize(len(columns))		
		
		if self.lastTable == "" or self.lastTable != columns[2]:
			wtid = columns[2]
			self.lastTable = wtid
			if not self.tables.get(wtid):
				self.CreateTable(columns[1], wtid)
				self.tables[wtid] = 1
			self.sql += " %s%s values('%s',%s%s" % (self.tablePrefix, wtid, columns[0], ",".join(columns[3:self.columnSize]), ")")
		else:
			self.sql += "('%s',%s%s" % (columns[0], ",".join(columns[3:self.columnSize]), ")")
		
		self.InsertData()
		
	def AssertColumnSize(self, columnSize):
		if self.columnSize == 0:
			self.columnSize = columnSize
		if self.columnSize != columnSize:
			print "column size:%d, not matched with previous:%d" % (columnSize, self.columnSize)
			sys.exit(1)
	
	def FirstAccess(self):
		return self.columnSize == 0
		
	def CreateDatabase(self):
		sql = self.db.CreateSql()
		code = self.taos.Query(sql)	
		self.PrintCode(sql, code)
		
	def UseDatabase(self):
		sql = self.db.UseSql()
		code = self.taos.Query(sql)	
		self.PrintCode(sql, code)
		
	def CreateMetrics(self, columns):
		metric = GwMetrics(columns, self.metricName)
		sql = metric.CreateSql()
		code = self.taos.Query(sql)	
		if code != 0:
			print "failed to create metrics, code:%d, error:%s, sql:%s" % (code, self.taos.ErrorMsg(), sql)
			sys.exit(1)
		else:
			print "create metrics %s finished" % (self.metricName) 
	
	def CreateTable(self, wfid, wtid):
		table = GwTable(wfid, wtid, self.tablePrefix, self.metricName)
		sql = table.CreateSql()
		code = self.taos.Query(sql)
		if code != 0:
			print "failed code:%d, error:%s, sql:%s" % (code, self.taos.ErrorMsg(), sql)
			sys.exit(1)		
		else:
			print "create table %s finished" % (wtid) 
		
	def InsertData(self):
		if len(self.sql) > 60000 or self.parseFinished:
			for i in range(5): # retry 5 times
				code = self.taos.Query(self.sql)			
				if code != 0:
					# error
					time.sleep(1)
				else:	
					# success
					self.insertLines += self.taos.AffectedRows()
					self.sql = "insert into"
					self.lastTable = ""
					return		
			print "failed to insert table, code:%d, error:%s, sql:%s" % (code, self.taos.ErrorMsg(), self.sql)
	
	def PrintCode(self, sql, code):
		if code == 0:
			print "success code:%d, sql:%s" % (code, sql)
		else:		
			print "failed code:%d, error:%s, sql:%s" % (code, self.taos.ErrorMsg(), sql)
			sys.exit(1)
	
if __name__=="__main__":
	# program parameters
	csvFileName = "./gw/"
	databaseName = "db"	
	metricName = "mt"
	replica = "1"	
	opts, args = getopt.getopt(sys.argv[1:], 'f:d:m:r:h', ['file=', 'database=', 'metrics=', 'replica=', 'help'])
	for key, value in opts:
		if key in ['-h', '--help']:
			print 'this program import data from CSV files to TDengine, version 1.4.x'
			print '-f\t CSV file name or directory, default is ./'
			print '-d\t Database used to create table or import data, default is db'
			print '-m\t Metrics used to create table, default is mt'
			print '-r\t Replica of Database, default is 1'
			sys.exit(0)
		if key in ['-f', '--file']:
			csvFileName = value
		elif key in ['-d', '--database']:
			databaseName = value	
		elif key in ['-m', '--metrics']:
			metricName = value
		elif key in ['-r', '--replica']:
			replica = value
	
	# connect parameters
	# host = "192.168.0.1"
	host = ""
	user = "root"
	password = "taosdata"
	taos = TaosConnection(host, user, password)
	taos.Connect()
	
	data = GwData(taos, csvFileName, databaseName, metricName, replica)
	
	# record start time
	start = datetime.datetime.now()
	
	# start connect, create database, metrics and table
	data.ParseFile()
	data.SortCsvFiles()
	data.ParseData()
	
	# record end time
	end = datetime.datetime.now()
	print "total %d files disposed, parsed:%d, insert:%d, failed:%d, error:%d, time spend %s seconds" % (len(data.csvFiles), data.parsedLines, data.insertLines, data.parsedLines - data.insertLines, data.errorLines, end - start)
	
	# close tdengine
	taos.Close()
	