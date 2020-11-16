package com.taosdata.example.mybatisplusdemo.domain;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Temperature {

    private Timestamp ts;
    private float temperature;
    private String location;
    private int tbIndex;

}
