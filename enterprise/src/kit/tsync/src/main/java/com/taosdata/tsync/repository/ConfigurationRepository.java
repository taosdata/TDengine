package com.taosdata.tsync.repository;

import com.taosdata.tsync.entity.config.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class ConfigurationRepository {

    private static List<Configuration> configurations = new ArrayList<>();
    private static volatile ConfigurationRepository instance;
    private static final int NULL = -1;

    private ConfigurationRepository() {
    }

    public static ConfigurationRepository getInstance() {
        if (instance == null) {
            synchronized (ConfigurationRepository.class) {
                if (instance == null)
                    instance = new ConfigurationRepository();
            }
        }
        return instance;
    }

    public void add(Configuration configuration) {
        int index = findIndex(configuration.getId());
        if (index != NULL)
            configurations.add(configuration);
    }

    public void delete(UUID id) {
        int index = findIndex(id);
        if (index != NULL) {
            configurations.remove(id);
        }
    }

    private int findIndex(UUID id) {
        for (int i = 0; i < configurations.size(); i++) {
            if (id.equals(configurations.get(i).getId()))
                return i;
        }
        return NULL;
    }

    public Configuration find(UUID id) {
        int index = findIndex(id);
        return index == NULL ? null : configurations.get(index);
    }

//    public Configuration findFirst(ConfigurationType type) {
//        for (Configuration config : configurations) {
//            if (config.getType() == type)
//                return config;
//        }
//        return null;
//    }
}
