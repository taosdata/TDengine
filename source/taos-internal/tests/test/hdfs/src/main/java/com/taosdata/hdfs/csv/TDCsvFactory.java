package com.taosdata.hdfs.csv;

import com.taosdata.hdfs.csv.internal.*;

public class TDCsvFactory {
    public static boolean init(String configFile) {
        TDConfig.init();
        if (!TDConfig.read(configFile)) {
            return false;
        }

        if (!TDLogDb.init()) {
            return false;
        }

        if (!TDDataDb.init()) {
            return false;
        }

        return true;
    }

    public static TDCsv createCsv(String fileName) {
        TDCsv csv = null;
        if (TDConfig.sortBeforeInsert) {
            return new TDCsvCache(fileName);
        } else {
            return new TDCsvDirect(fileName);
        }
    }
}