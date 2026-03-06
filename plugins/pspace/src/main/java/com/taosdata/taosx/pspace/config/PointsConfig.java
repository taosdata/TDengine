package com.taosdata.taosx.pspace.config;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

import java.util.List;

@Data
public class PointsConfig {
    @SerializedName("name_filter")
    private String nameFilter;

    @SerializedName("include_data_type")
    private Boolean includeDataType;

    @SerializedName("point_ids")
    private List<Long> pointIds;
}
