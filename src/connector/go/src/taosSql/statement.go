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

import (
	"database/sql/driver"
	"fmt"
	"reflect"
)

type taosSqlStmt struct {
	mc         *taosConn
	id         uint32
	pSql       string
	paramCount int
}

func (stmt *taosSqlStmt) Close() error {
	return nil
}

func (stmt *taosSqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *taosSqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc == nil || stmt.mc.taos == nil {
		return nil, errInvalidConn
	}
	return stmt.mc.Exec(stmt.pSql, args)
}

func (stmt *taosSqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	if  stmt.mc == nil || stmt.mc.taos == nil {
		return nil, errInvalidConn
	}
	return stmt.query(args)
}

func (stmt *taosSqlStmt) query(args []driver.Value) (*binaryRows, error) {
	mc := stmt.mc
	if  mc == nil || mc.taos == nil {
		return nil, errInvalidConn
	}

	querySql := stmt.pSql

	if len(args) != 0 {
		if !mc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(stmt.pSql, args)
		if err != nil {
			return nil, err
		}
		querySql = prepared
	}

	num_fields, err := mc.taosQuery(querySql)
	if err == nil {
		// Read Result
		rows := new(binaryRows)
		rows.mc = mc
		// Columns field
		rows.rs.columns, err = mc.readColumns(num_fields)
		return rows, err
	}
	return nil, err
}

type converter struct{}

// ConvertValue mirrors the reference/default converter in database/sql/driver
// with _one_ exception.  We support uint64 with their high bit and the default
// implementation does not.  This function should be kept in sync with
// database/sql/driver defaultConverter.ConvertValue() except for that
// deliberate difference.
func (c converter) ConvertValue(v interface{}) (driver.Value, error) {

	if driver.IsValue(v) {
		return v, nil
	}

	if vr, ok := v.(driver.Valuer); ok {
		sv, err := callValuerValue(vr)
		if err != nil {
			return nil, err
		}
		if !driver.IsValue(sv) {
			return nil, fmt.Errorf("non-Value type %T returned from Value", sv)
		}

		return sv, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		} else {
			return c.ConvertValue(rv.Elem().Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek == reflect.Uint8 {
			return rv.Bytes(), nil
		}
		return nil, fmt.Errorf("unsupported type %T, a slice of %s", v, ek)
	case reflect.String:
		return rv.String(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value(), with one exception:
// If vr.Value is an auto-generated method on a pointer type and the
// pointer is nil, it would panic at runtime in the panicwrap
// method. Treat it like nil instead.
//
// This is so people can implement driver.Value on value types and
// still use nil pointers to those types to mean nil/NULL, just like
// string/*string.
//
// This is an exact copy of the same-named unexported function from the
// database/sql package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}
