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

package com.taosdata.example.jdbcTemplate.domain;

import java.sql.Timestamp;

public class Weather {

    private Timestamp ts;
    private float temperature;
    private int humidity;

    public Weather() {
    }

    public Weather(Timestamp ts, float temperature, int humidity) {
        this.ts = ts;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "ts=" + ts +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }


}
