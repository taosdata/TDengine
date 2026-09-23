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

package com.taosdata.taosdemo.utils;

import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorTest {

    @Test
    public void randomValue() {
        for (int i = 0; i < TaosConstants.DATA_TYPES.length; i++) {
            System.out.println(TaosConstants.DATA_TYPES[i] + " >>> " + DataGenerator.randomValue(TaosConstants.DATA_TYPES[i]));
        }
    }

    @Test
    public void randomNchar() {
        String s = DataGenerator.randomNchar(10);
        Assert.assertEquals(10, s.length());
    }
}