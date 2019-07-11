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

import "C"
import (
	"context"
	"errors"
	"database/sql/driver"
	"unsafe"
	"strconv"
	"strings"
	"time"
)

type taosConn struct {
	taos             unsafe.Pointer
	affectedRows     int
	insertId         int
	cfg              *config
	status           statusFlag
	parseTime        bool
	reset            bool // set when the Go SQL package calls ResetSession
}

type taosSqlResult struct {
	affectedRows int64
	insertId     int64
}

func (res *taosSqlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *taosSqlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

func (mc *taosConn) Begin() (driver.Tx, error) {
	taosLog.Println("taosSql not support transaction")
	return nil, errors.New("taosSql not support transaction")
}

func (mc *taosConn) Close() (err error) {
	if mc.taos == nil {
		return errConnNoExist
	}
	mc.taos_close()
	return nil
}

func (mc *taosConn) Prepare(query string) (driver.Stmt, error) {
	if mc.taos == nil {		
		return nil, errInvalidConn
	}	
	
	stmt := &taosSqlStmt{
		mc:   mc,
		pSql: query,
	}

    // find ? count and save  to stmt.paramCount
	stmt.paramCount = strings.Count(query, "?")

	//fmt.Printf("prepare alloc stmt:%p, sql:%s\n", stmt, query)
	taosLog.Printf("prepare alloc stmt:%p, sql:%s\n", stmt, query)

	return stmt, nil
}

func (mc *taosConn) interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf := make([]byte, defaultBufSize)
	buf = buf[:0]    // clear buf
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				v := v.In(mc.cfg.loc)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if mc.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			//buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			//buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		//if len(buf)+4 > mc.maxAllowedPacket {
		if len(buf)+4 > maxTaosSqlLen {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func (mc *taosConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if mc.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	
	mc.affectedRows = 0
	mc.insertId     = 0
	_, err := mc.taosQuery(query)
	if err == nil {
		return &taosSqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, err
	}
	
	return nil, err
}

func (mc *taosConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return mc.query(query, args)
}

func (mc *taosConn) query(query string, args []driver.Value) (*textRows, error) {
	if mc.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	num_fields, err := mc.taosQuery(query)
	if err == nil {
		// Read Result
		rows := new(textRows)
		rows.mc = mc

		// Columns field
		rows.rs.columns, err = mc.readColumns(num_fields)
		return rows, err
	}
	return nil, err
}

// Ping implements driver.Pinger interface
func (mc *taosConn) Ping(ctx context.Context) (err error) {
	if mc.taos != nil {
	    return nil
	}
	return errInvalidConn
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *taosConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	taosLog.Println("taosSql not support transaction")
	return nil, errors.New("taosSql not support transaction")
}

func (mc *taosConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if mc.taos == nil {
		return nil, errInvalidConn
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	rows, err := mc.query(query, dargs)
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (mc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if mc.taos == nil {
		return nil, errInvalidConn
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	return mc.Exec(query, dargs)
}

func (mc *taosConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if mc.taos == nil {
		return nil, errInvalidConn
	}

	stmt, err := mc.Prepare(query)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func (stmt *taosSqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if stmt.mc == nil {
		return nil, errInvalidConn
	}
	dargs, err := namedValueToValue(args)

	if err != nil {
		return nil, err
	}

	rows, err := stmt.query(dargs)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (stmt *taosSqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if stmt.mc == nil {
		return nil, errInvalidConn
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	return stmt.Exec(dargs)
}

func (mc *taosConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

// ResetSession implements driver.SessionResetter.
// (From Go 1.10)
func (mc *taosConn) ResetSession(ctx context.Context) error {
	if mc.taos == nil {
		return driver.ErrBadConn
	}
	mc.reset = true
	return nil
}
