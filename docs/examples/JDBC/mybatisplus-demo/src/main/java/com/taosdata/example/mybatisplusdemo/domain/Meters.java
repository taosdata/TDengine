package com.taosdata.example.mybatisplusdemo.domain;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Meters {
    private String tbname;
    private Timestamp ts;
    private float current;
    private int voltage;
    private float phase;
    private int groupid;
    private byte[] location;
}
