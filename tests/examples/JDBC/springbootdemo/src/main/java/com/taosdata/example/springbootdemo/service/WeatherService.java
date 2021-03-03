package com.taosdata.example.springbootdemo.service;

import com.taosdata.example.springbootdemo.dao.WeatherMapper;
import com.taosdata.example.springbootdemo.domain.Weather;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Random;

@Service
public class WeatherService {

    @Autowired
    private WeatherMapper weatherMapper;
    private Random random = new Random(System.currentTimeMillis());

    public int init() {
        weatherMapper.createDB();
        weatherMapper.createSuperTable();
        weatherMapper.createTable();
        long ts = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < 10; i++) {
            count += weatherMapper.insert(new Weather(new Timestamp(ts + (1000 * i)), 30 * random.nextFloat(), random.nextInt(100)));
        }
        return count;
    }

    public List<Weather> query(Long limit, Long offset) {
        return weatherMapper.select(limit, offset);
    }

    public int save(float temperature, int humidity) {
        Weather weather = new Weather();
        weather.setTemperature(temperature);
        weather.setHumidity(humidity);

        return weatherMapper.insert(weather);
    }

    public int save(List<Weather> weatherList) {
        return weatherMapper.batchInsert(weatherList);
    }

    public int count() {
        return weatherMapper.count();
    }

    public List<String> getSubTables() {
        return weatherMapper.getSubTables();
    }

}
