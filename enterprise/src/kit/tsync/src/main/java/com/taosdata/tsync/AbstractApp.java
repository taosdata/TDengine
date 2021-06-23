package com.taosdata.tsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class AbstractApp {
    private static final Logger logger = LoggerFactory.getLogger(AbstractApp.class);

    protected static File readCommandLine(String[] args, String helpLine) {
        String configFilepath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equalsIgnoreCase(args[i]) && i < args.length - 1)
                configFilepath = args[++i];
        }
        if (configFilepath == null) {
            printHelp(helpLine);
            System.exit(0);
        }

        // read config file
        File file = new File(configFilepath);
        if (!file.exists()) {
            logger.error("cannot find config file: " + configFilepath);
            System.exit(-1);
        }

        return file;
    }

    private static void printHelp(String helpLine) {
        logger.error(helpLine);
    }

}
