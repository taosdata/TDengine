package com.taosdata.taosx.pspace;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

@Data
public class Node {
    private Long id;
    private String name;
    @SerializedName("long_name")
    private String longName;
    @SerializedName("is_leaf")
    private Boolean isLeaf;
}
