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

package com.taosdata.example.mybatisplusdemo.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import com.taosdata.example.mybatisplusdemo.mapper.MetersMapper;
import com.taosdata.example.mybatisplusdemo.service.MetersService;
import org.springframework.stereotype.Service;

/**
 * Service implementation for Meters entity.
 * Extends ServiceImpl to get MyBatis-Plus batch operations like saveBatch().
 */
@Service
public class MetersServiceImpl extends ServiceImpl<MetersMapper, Meters> implements MetersService {
}
