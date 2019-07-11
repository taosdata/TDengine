/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package taosSql
/*
#cgo CFLAGS : -I/usr/local/include/taos/
#cgo LDFLAGS: -L/usr/local/lib/taos -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"reflect"
)

type taosSqlField struct {
	tableName string
	name      string
	length    uint32
	flags     fieldFlag   // indicate whether this field can is null
	fieldType fieldType
	decimals  byte
	charSet   uint8
}

type resultSet struct {
	columns     []taosSqlField
	columnNames []string
	done        bool
}

type taosSqlRows struct {
	mc     *taosConn
	rs     resultSet
}

type binaryRows struct {
	taosSqlRows
}

type textRows struct {
	taosSqlRows
}

func (rows *taosSqlRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}

	columns := make([]string, len(rows.rs.columns))
	if rows.mc != nil && rows.mc.cfg.columnsWithAlias {
		for i := range columns {
			if tableName := rows.rs.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.rs.columns[i].name
			} else {
				columns[i] = rows.rs.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.rs.columns[i].name
		}
	}

	rows.rs.columnNames = columns

	return columns
}

func (rows *taosSqlRows) ColumnTypeDatabaseTypeName(i int) string {
	return rows.rs.columns[i].typeDatabaseName()
}

func (rows *taosSqlRows) ColumnTypeLength(i int) (length int64, ok bool) {
 	return int64(rows.rs.columns[i].length), true
}

func (rows *taosSqlRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return rows.rs.columns[i].flags&flagNotNULL == 0, true
}

func (rows *taosSqlRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	column := rows.rs.columns[i]
	decimals := int64(column.decimals)

	switch column.fieldType {
	case C.TSDB_DATA_TYPE_FLOAT:
        fallthrough
	case C.TSDB_DATA_TYPE_DOUBLE:
		if decimals == 0x1f {
			return math.MaxInt64, math.MaxInt64, true
		}
		return math.MaxInt64, decimals, true
	}

	return 0, 0, false
}

func (rows *taosSqlRows) ColumnTypeScanType(i int) reflect.Type {
	return rows.rs.columns[i].scanType()
}

func (rows *taosSqlRows) Close() (err error) {
	mc := rows.mc
	if mc == nil {
		return nil
	}

	rows.mc = nil
	return err
}

func (rows *taosSqlRows) HasNextResultSet() (b bool) {
	if rows.mc == nil {
		return false
	}
	return rows.mc.status&statusMoreResultsExists != 0
}

func (rows *taosSqlRows) nextResultSet() (int, error) {
	if rows.mc == nil {
		return 0, io.EOF
	}

	// Remove unread packets from stream
	if !rows.rs.done {
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		rows.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}
	return 0,nil
}

func (rows *taosSqlRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}

		if resLen > 0 {
			return resLen, nil
		}

		rows.rs.done = true
	}
}

func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

// stmt.Query return binary rows, and get row from this func
func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows *textRows) NextResultSet() (err error) {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

// db.Query return text rows, and get row from this func
func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (mf *taosSqlField) typeDatabaseName() string {
	//fmt.Println("######## (mf *taosSqlField) typeDatabaseName() mf.fieldType:", mf.fieldType)
	switch mf.fieldType {
	case C.TSDB_DATA_TYPE_BOOL:
		return "BOOL"

	case C.TSDB_DATA_TYPE_TINYINT:
		return "TINYINT"

	case C.TSDB_DATA_TYPE_SMALLINT:
		return "SMALLINT"

	case C.TSDB_DATA_TYPE_INT:
		return "INT"

	case C.TSDB_DATA_TYPE_BIGINT:
		return "BIGINT"

	case C.TSDB_DATA_TYPE_FLOAT:
		return "FLOAT"

	case C.TSDB_DATA_TYPE_DOUBLE:
		return "DOUBLE"

	case C.TSDB_DATA_TYPE_BINARY:
		return "BINARY"

	case C.TSDB_DATA_TYPE_NCHAR:
		return "NCHAR"

	case C.TSDB_DATA_TYPE_TIMESTAMP:
		return "TIMESTAMP"

	default:
		return ""
	}
}

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullTime  = reflect.TypeOf(NullTime{})
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
)

func (mf *taosSqlField) scanType() reflect.Type {
	//fmt.Println("######## (mf *taosSqlField) scanType() mf.fieldType:", mf.fieldType)
	switch mf.fieldType {
	case C.TSDB_DATA_TYPE_BOOL:
		return scanTypeInt8

	case C.TSDB_DATA_TYPE_TINYINT:
		return scanTypeInt8

	case C.TSDB_DATA_TYPE_SMALLINT:
		return scanTypeInt16

	case C.TSDB_DATA_TYPE_INT:
		return scanTypeInt32

	case C.TSDB_DATA_TYPE_BIGINT:
		return scanTypeInt64

	case C.TSDB_DATA_TYPE_FLOAT:
		return scanTypeFloat32

	case C.TSDB_DATA_TYPE_DOUBLE:
		return scanTypeFloat64

	case C.TSDB_DATA_TYPE_BINARY:
		return scanTypeRawBytes

	case C.TSDB_DATA_TYPE_NCHAR:
		return scanTypeRawBytes

	case C.TSDB_DATA_TYPE_TIMESTAMP:
		return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}
