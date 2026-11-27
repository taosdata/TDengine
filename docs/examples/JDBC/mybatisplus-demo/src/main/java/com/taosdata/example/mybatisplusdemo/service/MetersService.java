/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

package com.taosdata.example.mybatisplusdemo.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.taosdata.example.mybatisplusdemo.domain.Meters;

/**
 * Service interface for Meters entity.
 * Extends MyBatis-Plus IService to get built-in CRUD and batch operations.
 */
public interface MetersService extends IService<Meters> {
}
