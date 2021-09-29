package com.taosdata.example.springbootdemo.dao;

import com.taosdata.example.springbootdemo.domain.Weather;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface WeatherMapper {

    Map<String, Object> lastOne();

    void dropDB();

    void createDB();

    void createSuperTable();

    void createTable(Weather weather);

    List<Weather> select(@Param("limit") Long limit, @Param("offset") Long offset);

    int insert(Weather weather);

    int count();

    List<String> getSubTables();

    List<Weather> avg();

}
