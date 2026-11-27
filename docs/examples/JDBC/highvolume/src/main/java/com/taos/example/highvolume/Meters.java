/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taos.example.highvolume;

import java.sql.Timestamp;

public class Meters {
    String tableName;
    Timestamp ts;
    float current;
    int voltage;
    float phase;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getCurrent() {
        return current;
    }

    public void setCurrent(float current) {
        this.current = current;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public float getPhase() {
        return phase;
    }

    public void setPhase(float phase) {
        this.phase = phase;
    }

    @Override
    public String toString() {
        return tableName + "," +
                ts.toString() + "," +
                current + "," +
                voltage + "," +
                phase;
    }

    public static Meters fromString(String str) {
        String[] parts = str.split(",");
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid input format");
        }
        Meters meters = new Meters();
        meters.setTableName(parts[0]);
        meters.setTs(Timestamp.valueOf(parts[1]));
        meters.setCurrent(Float.parseFloat(parts[2]));
        meters.setVoltage(Integer.parseInt(parts[3]));
        meters.setPhase(Float.parseFloat(parts[4]));
        return meters;
    }

}
