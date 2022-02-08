package com.taosdata.example.jdbcTemplate.dao;

import com.taosdata.example.jdbcTemplate.domain.Weather;

import java.util.List;

public interface WeatherDao {


    int add(Weather weather);

    int[] batchInsert(List<Weather> weatherList);

    List<Weather> queryForList(int limit, int offset);

    int count();
}
