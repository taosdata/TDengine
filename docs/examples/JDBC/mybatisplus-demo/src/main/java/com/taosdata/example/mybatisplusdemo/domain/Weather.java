package com.taosdata.example.mybatisplusdemo.domain;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Weather {

    private Timestamp ts;
    private float temperature;
    private int humidity;
    private String location;

}
