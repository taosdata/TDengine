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

package com.taosdata.example.springbootdemo.controller;

import com.taosdata.example.springbootdemo.domain.Weather;
import com.taosdata.example.springbootdemo.service.WeatherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequestMapping("/weather")
@RestController
public class WeatherController {

    @Autowired
    private WeatherService weatherService;

    @GetMapping("/lastOne")
    public Weather lastOne() {
        return weatherService.lastOne();
    }

    @GetMapping("/init")
    public int init() {
        return weatherService.init();
    }

    @GetMapping("/{limit}/{offset}")
    public List<Weather> queryWeather(@PathVariable Long limit, @PathVariable Long offset) {
        return weatherService.query(limit, offset);
    }

    @PostMapping("/{temperature}/{humidity}")
    public int saveWeather(@PathVariable float temperature, @PathVariable float humidity) {
        return weatherService.save(temperature, humidity);
    }

    @GetMapping("/count")
    public int count() {
        return weatherService.count();
    }

    @GetMapping("/subTables")
    public List<String> getSubTables() {
        return weatherService.getSubTables();
    }

    @GetMapping("/avg")
    public List<Weather> avg() {
        return weatherService.avg();
    }

}
