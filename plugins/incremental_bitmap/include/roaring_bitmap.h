/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_ROARING_BITMAP_H
#define TDENGINE_ROARING_BITMAP_H

#include "bitmap_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * 创建基于 RoaringBitmap 的位图接口
 * @return 位图接口实例，失败返回NULL
 */
SBitmapInterface* roaring_bitmap_interface_create(void);

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_ROARING_BITMAP_H 