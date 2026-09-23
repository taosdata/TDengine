// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package tool

import "strconv"

func ConvertBytesToInt8(bs []byte) (int8, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 8)
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

func ConvertBytesToInt16(bs []byte) (int16, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

func ConvertBytesToInt32(bs []byte) (int32, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func ConvertBytesToInt64(bs []byte) (int64, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func BoolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}
