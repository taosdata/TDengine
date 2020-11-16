package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Temperature;

public interface TemperatureMapper extends BaseMapper<Temperature> {

    int createSuperTable();

    int createTable(String tbName, String location);

    void dropSuperTable();

    int insertOne(Temperature one);
}
