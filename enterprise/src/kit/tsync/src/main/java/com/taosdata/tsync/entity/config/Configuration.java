package com.taosdata.tsync.entity.config;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Configuration {

    private final UUID id;
    private final ConfigurationType configurationType;
    private final List<Configuration> children = new ArrayList<>(); // sub configurations

    public Configuration(ConfigurationType configurationType) {
        this.id = UUID.randomUUID();
        this.configurationType = configurationType;
    }

    public List<Configuration> find(ConfigurationType type) {
        List<Configuration> list = new ArrayList<>();
        recurseFind(type, list);
        return list;
    }

    private void recurseFind(ConfigurationType type, List<Configuration> list) {
        if (this.configurationType == type) {
            list.add(this);
            return;
        }
        for (Configuration child : children) {
            child.recurseFind(type, list);
        }
    }

    public Configuration findFirst(ConfigurationType type) {
        if (this.configurationType == type)
            return this;
        for (Configuration child : children) {
            Configuration config = child.findFirst(type);
            if (config != null)
                return config;
        }
        return null;
    }

    public Configuration find(UUID id) {
        if (this.id == id)
            return this;
        for (Configuration child : children) {
            Configuration config = child.find(id);
            if (config != null)
                return config;
        }
        return null;
    }

    public UUID getId() {
        return id;
    }

    public ConfigurationType getConfigurationType() {
        return configurationType;
    }

    public boolean contains(Configuration configuration) {
        if (this.id.equals(configuration.getId()))
            return true;
        for (Configuration child : children) {
            if (child.contains(configuration))
                return true;
        }
        return false;
    }

    public void add(Configuration configuration) {
        if (!contains(configuration))
            children.add(configuration);
    }
}