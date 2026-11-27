/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taosdata.example.springbootdemo.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.Map;

@Aspect
@Component
public class TaosAspect {

    @Around("execution(java.util.Map<String,Object> com.taosdata.example.springbootdemo.dao.*.*(..))")
    public Object handleType(ProceedingJoinPoint joinPoint) {
        Map<String, Object> result = null;
        try {
            result = (Map<String, Object>) joinPoint.proceed();
            for (String key : result.keySet()) {
                Object obj = result.get(key);
                if (obj instanceof byte[]) {
                    obj = new String((byte[]) obj);
                    result.put(key, obj);
                }
                if (obj instanceof Timestamp) {
                    obj = ((Timestamp) obj).getTime();
                    result.put(key, obj);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return result;
    }
}
