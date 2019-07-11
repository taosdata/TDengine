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
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

/******************************************************************************
*                           Time related utils                                *
******************************************************************************/

// NullTime represents a time.Time that may be NULL.
// NullTime implements the Scanner interface so
// it can be used as a scan destination:
//
//  var nt NullTime
//  err := db.QueryRow("SELECT time FROM foo WHERE id=?", id).Scan(&nt)
//  ...
//  if nt.Valid {
//     // use nt.Time
//  } else {
//     // NULL value
//  }
//
// This NullTime implementation is not driver-specific
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
// The value type must be time.Time or string / []byte (formatted time-string),
// otherwise Scan fails.
func (nt *NullTime) Scan(value interface{}) (err error) {
	if value == nil {
		nt.Time, nt.Valid = time.Time{}, false
		return
	}

	switch v := value.(type) {
	case time.Time:
		nt.Time, nt.Valid = v, true
		return
	case []byte:
		nt.Time, err = parseDateTime(string(v), time.UTC)
		nt.Valid = (err == nil)
		return
	case string:
		nt.Time, err = parseDateTime(v, time.UTC)
		nt.Valid = (err == nil)
		return
	}

	nt.Valid = false
	return fmt.Errorf("Can't convert %T to time.Time", value)
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func parseDateTime(str string, loc *time.Location) (t time.Time, err error) {
	base := "0000-00-00 00:00:00.0000000"
	switch len(str) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if str == base[:len(str)] {
			return
		}
		t, err = time.Parse(timeFormat[:len(str)], str)
	default:
		err = fmt.Errorf("invalid time string: %s", str)
		return
	}

	// Adjust location
	if err == nil && loc != time.UTC {
		y, mo, d := t.Date()
		h, mi, s := t.Clock()
		t, err = time.Date(y, mo, d, h, mi, s, t.Nanosecond(), loc), nil
	}

	return
}

// zeroDateTime is used in formatBinaryDateTime to avoid an allocation
// if the DATE or DATETIME has the zero value.
// It must never be changed.
// The current behavior depends on database/sql copying the result.
var zeroDateTime = []byte("0000-00-00 00:00:00.000000")

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func uint64ToString(n uint64) []byte {
	var a [20]byte
	i := 20

	// U+0030 = 0
	// ...
	// U+0039 = 9

	var q uint64
	for n >= 10 {
		i--
		q = n / 10
		a[i] = uint8(n-q*10) + 0x30
		n = q
	}

	i--
	a[i] = uint8(n) + 0x30

	return a[i:]
}

// treats string value as unsigned integer representation
func stringToInt(b []byte) int {
	val := 0
	for i := range b {
		val *= 10
		val += int(b[i] - 0x30)
	}
	return val
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)
	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}
	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		//case '\'':
		//	buf[pos] = '\\'
		//	buf[pos+1] = '\''
		//	pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

/******************************************************************************
*                               Sync utils                                    *
******************************************************************************/

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// atomicBool is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type atomicBool struct {
	_noCopy noCopy
	value   uint32
}

// IsSet returns whether the current boolean value is true
func (ab *atomicBool) IsSet() bool {
	return atomic.LoadUint32(&ab.value) > 0
}

// Set sets the value of the bool regardless of the previous value
func (ab *atomicBool) Set(value bool) {
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// TrySet sets the value of the bool and returns whether the value changed
func (ab *atomicBool) TrySet(value bool) bool {
	if value {
		return atomic.SwapUint32(&ab.value, 1) == 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}

// atomicError is a wrapper for atomically accessed error values
type atomicError struct {
	_noCopy noCopy
	value   atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (ae *atomicError) Set(value error) {
	ae.value.Store(value)
}

// Value returns the current error value
func (ae *atomicError) Value() error {
	if v := ae.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			// TODO: support the use of Named Parameters #561
			return nil, errors.New("taosSql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}


