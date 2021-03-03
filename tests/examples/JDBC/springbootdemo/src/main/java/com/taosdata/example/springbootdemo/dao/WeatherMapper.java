package com.taosdata.example.springbootdemo.dao;

import com.taosdata.example.springbootdemo.domain.Weather;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface WeatherMapper {

    void createDB();

    void createSuperTable();

    void createTable();

    List<Weather> select(@Param("limit") Long limit, @Param("offset") Long offset);

    int insert(Weather weather);

    int batchInsert(List<Weather> weatherList);

    int count();

    List<String> getSubTables();
}
