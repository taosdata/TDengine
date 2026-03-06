package com.taosdata.taosx.pspace.config;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

/**
 * Maps the [run] section in the TOML configuration file.
 *
 * Supported modes: Query, Subscribe, QuerySync.
 */
@Data
public class RunConfig {
    private String mode;

    @SerializedName("start_time")
    private String startTime;

    @SerializedName("end_time")
    private String endTime;

    @SerializedName("time_window")
    private Long timeWindow;

    @SerializedName("time_excursion")
    private Long timeExcursion;

    @SerializedName("query_interval")
    private Long queryInterval;
}
