package com.zddt.internel;

import java.util.ArrayList;

public class TDField {
    public String column;
    public String name;
    public String type;
    public ArrayList<Integer> columnsRead = new ArrayList<Integer>();
    public int columns[];
    public boolean isUseTableName;
    public boolean isUseFileName;
    public boolean isTypeBinary;
    public boolean isTypeTimestamp;

    public TDField(String c, String n, String t) {
        column = c;
        name = n;
        type = t;
        isUseFileName = (column.length() == 8 && column.substring(0, 8).equalsIgnoreCase("fileName"));
        isUseTableName = (column.length() == 9 && column.substring(0, 9).equalsIgnoreCase("tableName"));
        isTypeBinary = false;
        isTypeTimestamp = false;
        if ((type.length() >= 6 && type.substring(0, 6).equalsIgnoreCase("binary")) || (type.length() >= 5 && type.substring(0, 5).equalsIgnoreCase("nchar"))) {
            isTypeBinary = true;
        }
        if (type.length() >= 9 && type.substring(0, 9).equalsIgnoreCase("timestamp")) {
            if (TDConfig.timestampPattern.length() == 6 && TDConfig.timestampPattern.substring(0, 6).equalsIgnoreCase("bigint")) {
                isTypeBinary = true;
            } else {
                isTypeTimestamp = true;
            }
        }
    }

    public void normalize() {
        this.columns = new int[this.columnsRead.size()];
        for (int i = 0; i < this.columnsRead.size(); ++i) {
            this.columns[i] = this.columnsRead.get(i);
        }
    }
}
