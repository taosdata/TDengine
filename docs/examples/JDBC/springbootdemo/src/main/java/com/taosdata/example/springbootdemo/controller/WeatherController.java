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
