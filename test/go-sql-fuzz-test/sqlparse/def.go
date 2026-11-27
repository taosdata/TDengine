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

package sqlparser

const (
	TSDB_AUTH_LEN                           = 16
	TSDB_PASSWORD_MIN_LEN                   = 8
	TSDB_PASSWORD_MAX_LEN                   = 255
	TSDB_PASSWORD_LEN                       = 32 // this is the length after encryption
	TSDB_PASSWORD_SALT_LEN                  = 31 // length of salt used in password encryption, excluding the terminator '\0'
	TSDB_USER_PASSWORD_LEN                  = 129
	TSDB_USER_PASSWORD_LONGLEN              = 256
	TSDB_TOTP_SECRET_LEN                    = 32
	TSDB_USER_TOTPSEED_MIN_LEN              = 8   // minimum length for TOTP seed, excluding the terminator '\0'
	TSDB_USER_TOTPSEED_MAX_LEN              = 255 // maximum length for TOTP seed, excluding the terminator '\0'
	TSDB_USER_SESSION_PER_USER_DEFAULT      = -1
	TSDB_USER_CONNECT_TIME_DEFAULT          = -1 // 480 minutes
	TSDB_USER_CONNECT_IDLE_TIME_DEFAULT     = -1 // 30 minutes
	TSDB_USER_CALL_PER_SESSION_DEFAULT      = -1
	TSDB_USER_VNODE_PER_CALL_DEFAULT        = -1
	TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT = 3
	TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT    = (1440 * 60)       // 1440 minutes
	TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT    = (90 * 1440 * 60)  // 90 days
	TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT   = (7 * 1440 * 60)   // 7 days
	TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT   = (30 * 1440 * 60)  // 30 days
	TSDB_USER_PASSWORD_REUSE_TIME_MAX       = (365 * 1440 * 60) // 365 days
	TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT    = 5
	TSDB_USER_PASSWORD_REUSE_MAX_MAX        = 100
	TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT = (90 * 1440 * 60) // 90 days
	TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT       = 3
)
