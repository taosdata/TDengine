package com.taosdata.taosx.pspace.config;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

/**
 * Maps the [report] section in the TOML configuration file.
 *
 * Specifies IPC connection target and mode.
 * Raw data and batch settings have been moved to [advanced_options] section
 * (see {@link AdvancedOptionsConfig}).
 */
@Data
public class ReportConfig {
    private String remote;

    private Long concurrent;

    /** When true, skip IPC connection and only write to local files. */
    @SerializedName("local_only")
    private Boolean localOnly;

    // --- convenience helpers ---

    public boolean isLocalOnly() {
        return Boolean.TRUE.equals(localOnly);
    }
}
