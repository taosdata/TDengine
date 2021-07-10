package com.taosdata.tsync.utils;

import java.util.HashMap;
import java.util.Map;

public class CommandLineUtil {

    public static Map<String, String> readCommandLine(String[] args, String[] configNames) {
        Map<String, String> configurations = new HashMap<>();
        for (String configName : configNames) {
            for (int i = 0; i < args.length; i++) {
                if (("--" + configName).equalsIgnoreCase(args[i]) && i < args.length - 1)
                    configurations.put(configName, args[++i]);
            }
        }
        return configurations;
    }
}
