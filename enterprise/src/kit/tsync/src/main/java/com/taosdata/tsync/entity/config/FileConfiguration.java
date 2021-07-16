package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class FileConfiguration extends Configuration {
    public static final String DEFAULT_PREFIX = "";

    private String directory;
    private String prefix;

    public FileConfiguration() {
        super(ConfigurationType.FILE);
    }

    // getter and setter
    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
