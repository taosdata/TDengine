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

const (
	timeFormat              = "2006-01-02 15:04:05"
    maxTaosSqlLen           = 65380
	defaultBufSize          = maxTaosSqlLen + 32
)

type fieldType byte

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
)

type statusFlag uint16

const (
	statusInTrans statusFlag = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)

