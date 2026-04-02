package com.taosdata.taosx.pspace.config;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

/**
 * Maps the [advanced_options] section in the TOML configuration file.
 *
 * All fields are optional — only present when set in the DSN by the user.
 * Fields read_concurrency, write_concurrency, batch_size, batch_timeout are
 * reserved for future use and are not consumed by the plugin at this time.
 */
@Data
public class AdvancedOptionsConfig {

    /**
     * Log level override: Error / Warn / Info / Debug / Trace.
     * When set, the plugin's root logger level is changed accordingly.
     */
    @SerializedName("log_level")
    private String logLevel;

    /** Read concurrency (reserved for future use). */
    @SerializedName("read_concurrency")
    private Long readConcurrency;

    /** Write concurrency (reserved for future use). */
    @SerializedName("write_concurrency")
    private Long writeConcurrency;

    /** Batch write size (reserved for future use). */
    @SerializedName("batch_size")
    private Long batchSize;

    /** Batch write timeout in milliseconds (reserved for future use). */
    @SerializedName("batch_timeout")
    private Long batchTimeout;

    /** Whether to save raw data to local files for auditing / debugging. */
    @SerializedName("keep_raw_data")
    private Boolean keepRawData;

    /** Number of days to retain raw data files. */
    @SerializedName("keep_raw_data_days")
    private Long keepRawDataDays;

    /** Directory for raw data files. */
    @SerializedName("keep_raw_data_dir")
    private String keepRawDataDir;

    // --- convenience helpers ---

    public boolean isKeepRawData() {
        return Boolean.TRUE.equals(keepRawData);
    }

    public long getKeepRawDataDaysOrDefault() {
        return keepRawDataDays != null ? keepRawDataDays : 30L;
    }
}
