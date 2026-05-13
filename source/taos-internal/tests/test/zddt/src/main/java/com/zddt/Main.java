package com.zddt;

import java.sql.Connection;

import com.zddt.internel.*;

public class Main {
    Connection connection;

    public static void main(String[] args) throws Exception {
        String configFile;
        if (args.length < 1) {
            configFile = String.format("%s/../zddt/jdbc.cfg", TDUtil.getAbsolutePath(""));
            TDLog.print(String.format("config file is required, default is %s", configFile));
        } else {
            configFile = args[0];
        }

        TDLog.print(String.format("use config file %s", configFile));

        TDConfig.init();
        if (!TDConfig.read(configFile)) {
            return;
        }

        if (!TDLogDb.init()) {
            return;
        }

        if (!TDDataDb.init()) {
            return;
        }

        if (TDConfig.jdbcSqls.size() != 0) {
            TDJdbcTasks.run();
        } else {
            TDCsvTasks.run();
        }
    }
}
