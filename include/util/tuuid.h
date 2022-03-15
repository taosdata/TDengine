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
#include "os.h"
#include "taoserror.h"
#include "thash.h"

/**
 * Generate an non-negative signed 32bit id
 *+------------+-----+-----------+---------------+
 *| uid|localIp| PId | timestamp | serial number |
 *+------------+-----+-----------+---------------+
 *|  6bit      |6bit |   12bit   |      8bit     |
 *+------------+-----+-----------+---------------+
 * @return
 */
int32_t tGenIdPI32(void);

/**
 * Generate an non-negative signed 64bit id
 *+------------+-----+-----------+---------------+
 *| uid|localIp| PId | timestamp | serial number |
 *+------------+-----+-----------+---------------+
 *| 12bit      |12bit|24bit      |16bit          |
 *+------------+-----+-----------+---------------+
 * @return
 */
int64_t tGenIdPI64(void);
