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
import sys
import threading
import struct
from multiprocessing import *
import os
import time

so = CDLL('./libtaos.so')


def ErrorInfo(conn, code, sql):
	so.taos_errstr.restype = c_char_p
	if(code == 0):
		print("\"" + sql + "\" success")
	else:
		print("\"" + sql + "\" failure, reason: " + str(so.taos_errstr(conn), 'ascii'))


def InitTDengine():
	so.taos_init()
	print("TDengine Initialization finished")


def ConnectTDengine(host, user, password, dbName, port):
	print(host)
	so.taos_connect.restype = int
	conn = int(so.taos_connect(host, user, password, dbName, port))
	# conn=68488878
	print(conn)
	if(conn == 0):
		print("Connect to TDengine failed")
		sys.exit(1)
	else:
		print("Connect to TDengine success")
	print("label")
	return(conn)


def CloseTDengine(conn):
	if (conn != 0):
		so.taos_close(conn)
		print("Close connect to TDengine success")
	

def DropDB(conn, dbName):
	sql = "drop database " + dbName
	code = so.taos_query(conn, bytes(sql, 'ascii'))
	ErrorInfo(conn, code, sql)


def CreateDB(conn, dbName):
	sql = "create database " + dbName
	code = so.taos_query(conn, bytes(sql, 'ascii'))
	ErrorInfo(conn, code, sql)


def UseDB(conn, dbName):
	sql = "use " + dbName
	code = so.taos_query(conn, bytes(sql, 'ascii'))
	ErrorInfo(conn, code, sql)


def CreateMetric(conn, metricTableName, tags1, tags2, tags3):
	sql = "create table " + metricTableName + " (ts timestamp, test0 BINARY(20), test1 INT, test2 BIGINT, test3 FLOAT, test4 DOUBLE, test5 BINARY(20), test6 SMALLINT, test7 TINYINT, test8 BOOL, test9 BINARY(20)) tags(" + tags1 + " binary(40), " + tags2 + " binary(40), " + tags3 + " binary(40) );";
	code = so.taos_query(conn, bytes(sql, 'ascii'))
	ErrorInfo(conn, code, sql)


def CreateTable(conn, metricTableName, tablePrefix, tableCount, tags1, tags2, tags3):
	for tableId in range(tableCount):
		 sql = "create table " + str(tablePrefix) + str(tableId) + " using " + metricTableName + " tags('" + tags1 + "', '" + tags2 + "', '" + tags3 + str(tableId) + "')"
		 code = so.taos_query(conn, bytes(sql, 'ascii'))
	ErrorInfo(conn, code, sql)


def NewMethod(timestamp, row, tableId, recordPerRow, dbName, tablePrefix):
	recordString = "insert into " + dbName + "." + tablePrefix + str(tableId) + " values(" + str(timestamp) + ", '" + str(row+1) + "', " + str(row+2) + ", " + str(row+3) + ", " + str(row+4) + ", " + str(row+5) + ", " + str(row+6) + ", " + str(row+7) + ", " + str(row+8) + ", " + str(row+9) + ", '" + str(row+10) + "')"
	for recordId in range(recordPerRow-1):
		timestampNew = timestamp + recordId+1
		recordString = recordString + ", values(" + str(timestampNew) + ", '" + str(row+1) + "', " + str(row+2) + ", " + str(row+3) + ", " + str(row+4) + ", " + str(row+5) + ", " + str(row+6) + ", " + str(row+7) + ", " + str(row+8) + ", " + str(row+9) + ", '" + str(row+10) + "')"
	recordString = recordString + ";"
	return(recordString)


def UserResult(conn):
	if (conn != 0):
		reslut = so.taos_use_result(conn)
		return(reslut)


def FreeResult(result):
	if (result != 0):
		so.taos_free_result(result)


def InsertCount(tableId, rowsCount, printRows, recordPerRow, host, user, password, dbName, port, tablePrefix):
	print(tableId)
	timestamp = 1512312345123 + tableId * 100
	print(timestamp)
	newConn = ConnectTDengine(host, user, password, bytes(dbName,'ascii'), "")
	print(newConn)
	successInsertRowCount = 0
	for row in range(rowsCount):
		if (row % printRows ==0):
			print(tablePrefix + str(tableId) + " " + str(row * recordPerRow) + " rows should be inserted")
			print(tablePrefix + str(tableId) + " " + str(successInsertRowCount * recordPerRow) + " rows insert successfully")
			QueryExecute(tableId, newConn, tablePrefix)
		sql = NewMethod(timestamp, row, tableId, recordPerRow, dbName, tablePrefix)
		code = so.taos_query(newConn, bytes(sql, 'ascii'))
		# if (code == 0):
		# 	ErrorInfo(newConn, code, bytes(sql, 'ascii'))
		affectrows = so.taos_affected_rows(newConn)
		# if (affectrows != recordPerRow):
		# 	ErrorInfo(newConn, code, bytes(sql, 'ascii'))
		if ((code == 0) & (affectrows == recordPerRow)):
			successInsertRowCount = successInsertRowCount + 1
		timestamp = timestamp + recordPerRow
	so.taos_close(newConn)
	print(str(tablePrefix) + str(tableId) + " insert finish")


class TAOS_FIELD(Structure):
	_fields_ = [
	("type", c_byte), 
	("name", c_char * 20), 
	("bytes", c_short)]


def FieldValueExt(meta, fieldPtr):
	if (meta.contents.type == 1):
		filedValue = cast(fieldPtr[0], POINTER(c_bool))[0]
	elif (meta.contents.type == 2):
		filedValue = cast(fieldPtr[0], POINTER(c_byte))[0]
	elif (meta.contents.type == 3):
		filedValue = cast(fieldPtr[0], POINTER(c_short))[0]
	elif (meta.contents.type == 4):
		filedValue = cast(fieldPtr[0], POINTER(c_int))[0]
	elif (meta.contents.type == 5):
		filedValue = cast(fieldPtr[0], POINTER(c_long))[0]
	elif (meta.contents.type == 6):
		filedValue = cast(fieldPtr[0], POINTER(c_float))[0]
	elif (meta.contents.type == 7):
		filedValue = cast(fieldPtr[0], POINTER(c_double))[0]
	elif (meta.contents.type == 8):
		filedValue = cast(fieldPtr[0], POINTER(c_char))[0:(meta.contents.bytes)]
	elif (meta.contents.type == 9):
		filedValue = cast(fieldPtr[0], POINTER(c_long))[0]
	return(filedValue)


def QueryExecute(tableId, conn, tablePrefix):
	sql = "select count(*) from " + tablePrefix + str(tableId);
	code = so.taos_query(conn, bytes(sql, 'ascii'))
	if (code != 0):
		ErrorInfo(conn, code, sql)
	fieldCount = so.taos_field_count(conn)
	print("field count: " + str(fieldCount))
	result = UserResult(conn)

	# extract field name with structure method
	so.taos_fetch_fields.restype = POINTER(TAOS_FIELD)
	metas = []
	filedNames = []
	for i in range(fieldCount):
		p = so.taos_fetch_fields(result + i*(sizeof(TAOS_FIELD)))
		metas.append(p)
		filedNames.append(str(p.contents.name, "ascii"))
	print("fieldNames: " + " --- ".join(filedNames))

	# # print field name with cast method
	# ptr = so.taos_fetch_fields(result)
	# ptrOffset = 24
	# for i in range(fieldCount):
	# 	ptr += i*ptrOffset
	# 	print ptr
	# 	fieldType = cast(ptr, POINTER(c_byte))
	# 	print "type:" + str(fieldType[0])
	# 	fieldName = cast(ptr+1, POINTER(c_char))
	# 	print "name:" + fieldName[0:19]
	# 	fieldBytes = cast(ptr+22, POINTER(c_short))
	# 	print "bytes:" + str(fieldBytes[0])

	# print query return result
	rowPtr = so.taos_fetch_row(result)
	if (rowPtr == 0):
		print(sql + " \"result set is null\"")
	elif(result > 0):
		while (rowPtr > 0):
			rowData = []
			filedNames = []
			for fields in range(fieldCount):
				meta = metas[fields]
				fieldPtr = cast(rowPtr, POINTER(c_long))

				# extract field value based on field type and bytes
				filedValue = FieldValueExt(meta, fieldPtr)
				rowPtr = rowPtr + 8
				rowData.append(str(filedValue))
			print("rowData:    " + " --- ".join(rowData))
			rowPtr = so.taos_fetch_row(result)
	FreeResult(result)


def MultipulTableInsertRead(tableCount, rowsCount, printRows, recordPerRow, host, user, password, dbName, port, tablePrefix):
	for tableId in range(tableCount):
		InsertCount(tableId, rowsCount, printRows, recordPerRow, host, user, password, dbName, port, tablePrefix)


if __name__=="__main__":
	host = bytes(str("192.168.1.120"), 'ascii')
	configDir = bytes(str("./cfg"), 'ascii')
	user = bytes(str("root"), 'ascii')
	password = bytes(str("taosdata"), 'ascii')
	port = bytes(str(''), 'ascii')

	#metrics parameters
	metricTableName = "experiment"
	tags1 = "experiment"
	tags2 = "channel_group"
	tags3 = "channel"

	#inser/read parameters
	tableCount = 10
	processNum  = tableCount
	rowsCount = 1000
	recordPerRow = 10
	printRows = 100
	dbName = "testdbpy3"
	port = ""
	tablePrefix = "table"

	#start connect, create database, metrics and table
	InitTDengine()
	conn = ConnectTDengine(host, user, password, "", "")
	DropDB(conn, dbName)
	CreateDB(conn, dbName)
	UseDB(conn, dbName)
	CreateMetric(conn, metricTableName, tags1, tags2, tags3)
	CreateTable(conn, metricTableName, tablePrefix, tableCount, tags1, tags2, tags3)
	CloseTDengine(conn)

	#start insert and read
	MultipulTableInsertRead(tableCount, rowsCount, printRows, recordPerRow, host, user, password, dbName, port, tablePrefix)

	# #multiprocess
	# pool = Pool(processes = 4)                                                
	# for tableId in range(tableCount):
	# 	pool.apply_async(InsertCount, args=(tableId, rowsCount, recordPerRow, printRows, host, user, password, dbName, port, tablePrefix))
	# 	# pool.apply_async(ConnectTDengine, args=(host, user, password, "", ""))

	# pool.close()
	# pool.join()