// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

package pool

import (
	"bytes"
	"sync"
)

var bytesBufferPool sync.Pool

func init() {
	bytesBufferPool.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func BytesPoolGet() *bytes.Buffer {
	return bytesBufferPool.Get().(*bytes.Buffer)
}

func BytesPoolPut(b *bytes.Buffer) {
	b.Reset()
	bytesBufferPool.Put(b)
}
