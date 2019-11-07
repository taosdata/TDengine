package com.zddt.internel;

public class TDLine {
    public TDTask task;
    public String cols[];
    public int lineIndex;
    public String tableName = "";
    public long timestamp = 0;

    public TDLine(int lineIndex, TDTask ds) {
        this.task = ds;
        this.cols = new String[TDConfig.maxColSize];
        this.lineIndex = lineIndex;
    }

    public boolean parse(String line) {
        int len = 0;
        int start = 0;
        int col = 0;
        int end = 0;
        for (; end < line.length(); ++end) {
            char c = line.charAt(end);
            if (col >= TDConfig.maxColSize) {
                col--;
                break;
            }

            if (c == TDConfig.split) {
                this.cols[col++] = line.substring(start, end);
                start = end + 1;
                continue;
            }
        }

        if (col < 2) {
            lineIndex--;
            return false;
        }

        if (col < TDConfig.maxColSize) {
            this.cols[col] = line.substring(start, end);
        }
        col++;

        if (col != TDConfig.colSize) {
            TDLog.error(String.format("task:%d, line:%d, columnNum:%d != expect:%d ", task.getTaskIndex(), lineIndex, col, TDConfig.colSize));
            return false;
        }

        if (!this.parseTableName()) {
            TDLog.error(String.format("task:%d, line:%d, parse table name failed ", task.getTaskIndex(), lineIndex));
            return false;
        }

        this.timestamp = TDConfig.getAutoTimestamp();

        for (int c = 1; c < TDConfig.fields.length; ++c) {
            if (TDConfig.fields[c].isTypeTimestamp) {
                int colOfLine = TDConfig.fields[c].columns[0]; //only one col support
                long ts = TDUtil.getTimeMsFromFormat(cols[colOfLine], TDConfig.timestampPattern);
                if (ts < 0) {
                    ts = 0;
                }
                if (TDConfig.datadbMicroSecond) {
                    ts *= 1000;
                }
                cols[colOfLine] = String.valueOf(ts);
            }
        }

        return true;
    }

    public boolean parseTableName() {
        for (int col : TDConfig.tableNameColumns) {
            String colStr = cols[col];
            if (colStr.startsWith("\"") || colStr.startsWith("\'")) {
                tableName += colStr.substring(1, colStr.length() - 2);
            } else {
                tableName += cols[col];
            }
        }

       if (TDConfig.tableNameIgnoreFrontChars != 0) {
            int length = tableName.length();
            if (length - TDConfig.tableNameIgnoreFrontChars > 0) {
                tableName = tableName.substring(TDConfig.tableNameIgnoreFrontChars, length);
            } else {
                tableName = "";
            }
        }

        if (TDConfig.tableNameIgnoreBackChars != 0) {
            int length = tableName.length();
            if (length - TDConfig.tableNameIgnoreBackChars > 0) {
                tableName = tableName.substring(0, length - TDConfig.tableNameIgnoreBackChars);
            } else {
                tableName = "";
            }
        }

        //tableName = TDConfig.tablePrefix + tableName;
        if (tableName.length() > 22) {
            tableName = TDMD5Utils.MD5Encode(tableName, "utf8");
            tableName = tableName.substring(0, 16);
        }

        return true;
    }

    public String getSelectSql() {
        StringBuilder sqlBuffer = new StringBuilder();
        sqlBuffer.append("select ").append(TDConfig.fields[0].name).append(" from ")
                .append(TDConfig.datadbName).append('.').append(TDConfig.tablePrefix)
                .append(tableName).append(" where ")
                .append(TDConfig.fields[0].name).append('=').append(timestamp);
        return sqlBuffer.toString();
    }

    public String getInsertSql() {
        StringBuilder sqlBuffer = new StringBuilder();
        sqlBuffer.append("import into ").append(TDConfig.datadbName).append('.').append(TDConfig.tablePrefix)
                .append(tableName).append(" values(").append(timestamp);

        for (int i = 1; i < TDConfig.fields.length; ++i) {
            TDField field = TDConfig.fields[i];
            sqlBuffer.append(',');
            if (field.isTypeBinary) {
                sqlBuffer.append('\'');
            }

            for (int col : field.columns) {
                String colStr = cols[col];
                if (colStr == null) {
                    sqlBuffer.append("NULL");
                } else if (colStr.length() == 0) {
                    sqlBuffer.append("NULL");
                } else {
                    sqlBuffer.append(colStr);
                }
            }

            if (field.isTypeBinary) {
                sqlBuffer.append('\'');
            }
        }

        sqlBuffer.append(')');

        return sqlBuffer.toString();
    }
}
