package com.taosdata.taosx.pspace;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

@Data
public class Point {
    private Long id;
    private String name;
    private String type; // psDigital, psAnalog, psStringType, psMultiDigitalType
    @SerializedName("long_name")
    private String longName;
    private String desc;
    @SerializedName("data_type")
    private String dataType;
}
