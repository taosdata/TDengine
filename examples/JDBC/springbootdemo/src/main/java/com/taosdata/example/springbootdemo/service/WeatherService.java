package com.taosdata.example.springbootdemo.service;

import com.taosdata.example.springbootdemo.dao.WeatherMapper;
import com.taosdata.example.springbootdemo.domain.Weather;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Service
public class WeatherService {

    @Autowired
    private WeatherMapper weatherMapper;
    private Random random = new Random(System.currentTimeMillis());
    private String[] locations = {"北京", "上海", "广州", "深圳", "天津"};

    public int init() {
        weatherMapper.dropDB();
        weatherMapper.createDB();
        weatherMapper.createSuperTable();
        long ts = System.currentTimeMillis();
        long thirtySec = 1000 * 30;
        int count = 0;
        for (int i = 0; i < 20; i++) {
            Weather weather = new Weather(new Timestamp(ts + (thirtySec * i)), 30 * random.nextFloat(), random.nextInt(100));
            weather.setLocation(locations[random.nextInt(locations.length)]);
            weather.setGroupId(i % locations.length);
            weatherMapper.createTable(weather);
            count += weatherMapper.insert(weather);
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

    public int count() {
        return weatherMapper.count();
    }

    public List<String> getSubTables() {
        return weatherMapper.getSubTables();
    }

    public List<Weather> avg() {
        return weatherMapper.avg();
    }
}
