package com.taosdata.hdfs.csv.internal;


import com.taosdata.hdfs.csv.*;

public class TDCsvLine {
    public TDCsv csv;
    public String cols[];
    public int lineIndex;
    public String tableName = "";
    public long timestamp = 0;

    public TDCsvLine(int lineIndex, TDCsv csv) {
        this.csv = csv;
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

            if (TDConfig.splitContainBlank && c == ' ') {
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

        if (TDConfig.splitContainQuotation) {
            for (int c = 0; c < TDConfig.colSize; ++c) {
                String colStr = this.cols[c];
                if (colStr.charAt(0) == '\'' || colStr.charAt(0) == '\"') {
                    this.cols[c] = colStr.substring(1, colStr.length() - 1);
                }
            }
        }

        if (TDConfig.splitContainRightBracket) {
            String colStr = this.cols[col-1];
            if (colStr == null) {
                return false;
            }
            int length = colStr.length();
            if (length > 1 && colStr.charAt(length - 1) == '}') {
                this.cols[col-1] = colStr.substring(0, length - 1);
            }
        }

        if (TDConfig.fileType == TDFileType.TD_FILE_TYPE_ZJXL) {
            if (col >= 6) {
                col = disposeZjxlFormat();
                if (col == 0) {
                    return false;
                }
            }

        }

        if (col != TDConfig.colSize) {
            TDLog.error(String.format("file:%s, line:%d, columnNum:%d != expect:%d ", csv.getFileName(), lineIndex, col, TDConfig.colSize));
            return false;
        }

        if (TDConfig.splitContainColon) {
            //{TYPE:0,RET:0,1:68214000,2:20915580,3
            for (int c = 0; c < TDConfig.colSize; ++c) {
                String colStr = this.cols[c];
                if (colStr == null) {
                    continue;
                }
                int length = colStr.length();
                for (int n = 0; n < length; ++n) {
                    if (colStr.charAt(n) == ':') {
                        this.cols[c] = colStr.substring(n + 1, length);
                    }
                }
            }
        }

        if (!this.parseTableName()) {
            //too many errors
            if (TDConfig.fileType != TDFileType.TD_FILE_TYPE_ZJXL) {
                TDLog.error(String.format("file:%s, line:%d, parse table name failed ", csv.getFileName(), lineIndex));
            }
            return false;
        }

        if (TDConfig.autoTimestamp) {
            this.timestamp = csv.beginTs++;
        } else if (!this.parseTimestamp()) {
            if (TDConfig.fileType != TDFileType.TD_FILE_TYPE_ZJXL) {
                TDLog.error(String.format("file:%s, line:%d, parse timestamp failed ", csv.getFileName(), lineIndex));
            }
            return false;
        } else {
        }

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
        if (TDConfig.tableNameUseFileName) {
            tableName = TDConfig.tablePrefix + csv.getFileBaseName();
        } else {
            for (int col : TDConfig.tableNameColumns) {
                String colStr = cols[col];
                if (colStr.startsWith("\"") || colStr.startsWith("\'")) {
                    tableName += colStr.substring(1, colStr.length() - 2);
                } else {
                    tableName += cols[col];
                }
            }

            if (TDConfig.tableRadix > 0) {
                int length = tableName.length();
                int remainLength = length - TDConfig.tableRadix;
                if (remainLength > 0) {
                    tableName = tableName.substring(0, remainLength);
                    timestamp += Integer.valueOf(tableName.substring(remainLength, length));
                }
            }

            if (TDConfig.tableIgnoreFrontNum != 0) {
                int length = tableName.length();
                if (length - TDConfig.tableIgnoreFrontNum > 0) {
                    tableName = tableName.substring(TDConfig.tableIgnoreFrontNum, length);
                } else {
                    tableName = "";
                }
            }
        }

        //tableName = TDConfig.tablePrefix + tableName;
        if (tableName.length() > 22 || TDConfig.tableNameMd5) {
            tableName = TDMD5Utils.MD5Encode(tableName, "utf8");
            tableName = tableName.substring(0, 16);
        }

        return true;
    }

    public boolean parseTimestamp() {
        String timestampStr = "";
        if (TDConfig.fields[0].isUseFileName) {
            timestampStr = csv.getFileBaseName();
        } else if (TDConfig.fields[0].isUseTableName) {
            timestampStr = tableName;
        } else {
            int columns[] = TDConfig.fields[0].columns;
            for (int col : columns) {
                timestampStr += cols[col];
            }
        }

        long parsedTs;
        if (TDConfig.isTimestampPatternBigInt)
            parsedTs = Long.valueOf(timestampStr);
        else {
            parsedTs = TDUtil.getTimeMsFromFormat(timestampStr, TDConfig.timestampPattern);
        }

        if (parsedTs < 0) {
            return false;
        }

        if (TDConfig.timestampPrecision == TDTimePrecision.TD_TIME_PRECISION_SECOND) {
            if (TDConfig.datadbMicroSecond) {
                timestamp = parsedTs * 1000000 + timestamp * TDConfig.tableTolerance;
            } else {
                timestamp = parsedTs * 1000 + timestamp * TDConfig.tableTolerance;
            }
        } else if (TDConfig.timestampPrecision == TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND) {
            if (TDConfig.datadbMicroSecond) {
                timestamp = parsedTs * 1000 + timestamp * TDConfig.tableTolerance;
            }
        } else {
        }

        if (timestamp < TDConfig.timestampMinValue || timestamp > TDConfig.timestampMaxValue) {
            if (TDConfig.fileType != TDFileType.TD_FILE_TYPE_ZJXL)
                TDLog.error(String.format("file:%s, line:%d ts:%s parsed:%d should in range[%d, %d]"
                        , csv.getFileName(), lineIndex, timestampStr, timestamp, TDConfig.timestampMinValue, TDConfig.timestampMaxValue));
            return false;
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
        sqlBuffer.append(TDConfig.insertStr).append(" into ").append(TDConfig.datadbName).append('.').append(TDConfig.tablePrefix)
                .append(tableName).append(" values(").append(timestamp);

        for (int i = 1; i < TDConfig.fields.length; ++i) {
            TDField field = TDConfig.fields[i];
            sqlBuffer.append(',');
            if (field.isTypeBinary && !TDConfig.binaryContainQuotation) {
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

            if (field.isTypeBinary && !TDConfig.binaryContainQuotation) {
                sqlBuffer.append('\'');
            }
        }

        sqlBuffer.append(')');

        return sqlBuffer.toString();
    }

    //for zjxl test
    private static int TD_INT_MAX = 2000000000;
    private static int TD_SMALLINT_MAX = 32767;
    private static int TD_TINYINT_MAX = 126;

    private int disposeZjxlFormat() {
        if (!(this.cols[5].equals("{TYPE:0") || this.cols[5].equals("{TYPE:7"))) {
            return 0;
        }

        String newCols[] = new String[TDConfig.maxColSize];
        for (int c = 0; c < 6; ++c) {
            newCols[c] = this.cols[c];
        }
        for (int c = 6; c < TDConfig.maxColSize; ++c) {
            String colStr = this.cols[c];
            if (colStr == null) {
                continue;
            }
            int length = colStr.length();

            if (length > 4 && colStr.substring(0, 4).equals("RET:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_TINYINT_MAX) {
                    newCols[6] = colStr.substring(4, 6);
                } else {
                    newCols[6] = colStr;
                }
            }
            else if (length > 2 && colStr.substring(0, 2).equals("1:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_INT_MAX) {
                    newCols[7] = colStr.substring(2, 10);
                } else {
                    newCols[7] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("2:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_INT_MAX) {
                    newCols[8] = colStr.substring(2, 10);
                } else {
                    newCols[8] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("3:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_SMALLINT_MAX) {
                    newCols[9] = colStr.substring(2, 6);
                } else {
                    newCols[9] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("4:")) {
                newCols[10] = colStr;
            } else if (length > 2 && colStr.substring(0, 2).equals("5:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_SMALLINT_MAX) {
                    newCols[11] = colStr.substring(2, 6);
                } else {
                    newCols[11] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("6:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_INT_MAX) {
                    newCols[12] = colStr.substring(2, 10);
                } else {
                    newCols[12] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("7:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_SMALLINT_MAX) {
                    newCols[13] = colStr.substring(2, 6);
                } else {
                    newCols[13] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("8:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_INT_MAX) {
                    newCols[14] = colStr.substring(2, 10);
                } else {
                    newCols[14] = colStr;
                }
            } else if (length > 2 && colStr.substring(0, 2).equals("9:")) {
                long t = Long.valueOf(colStr.substring(2));
                if (t > TD_INT_MAX) {
                    newCols[15] = colStr.substring(2, 10);
                } else {
                    newCols[15] = colStr;
                }
            } else if (length > 3 && colStr.substring(0, 3).equals("20:")) {
                long t = Long.valueOf(colStr.substring(3));
                if (t > TD_INT_MAX) {
                    newCols[16] = colStr.substring(3, 11);
                } else {
                    newCols[16] = colStr;
                }
            } else if (length > 3 && colStr.substring(0, 3).equals("24:")) {
                long t = Long.valueOf(colStr.substring(3));
                if (t > TD_INT_MAX) {
                    newCols[17] = colStr.substring(3, 11);
                } else {
                    newCols[17] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("500:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_INT_MAX) {
                    newCols[18] = colStr.substring(4, 12);
                } else {
                    newCols[18] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("519:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_INT_MAX) {
                    newCols[19] = colStr.substring(4, 12);
                } else {
                    newCols[19] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("700:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_TINYINT_MAX) {
                    newCols[20] = colStr.substring(4, 6);
                } else {
                    newCols[20] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("701:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_INT_MAX) {
                    newCols[21] = colStr.substring(4, 12);
                } else {
                    newCols[21] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("702:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_TINYINT_MAX) {
                    newCols[22] = colStr.substring(4, 6);
                } else {
                    newCols[22] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("703:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_TINYINT_MAX) {
                    newCols[23] = colStr.substring(4, 6);
                } else {
                    newCols[23] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("990:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_TINYINT_MAX) {
                    newCols[24] = colStr.substring(4, 6);
                } else {
                    newCols[24] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("104:")) {
                newCols[25] = colStr;
            } else if (length > 4 && colStr.substring(0, 4).equals("202:")) {
                long t = Long.valueOf(colStr.substring(4));
                if (t > TD_SMALLINT_MAX) {
                    newCols[26] = colStr.substring(4, 8);
                } else {
                    newCols[26] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("999:")) {
                newCols[27] = colStr;
            } else if (length > 5 && colStr.substring(0, 5).equals("1000:")) {
                long t = Long.valueOf(colStr.substring(5));
                if (t > TD_INT_MAX) {
                    newCols[28] = colStr.substring(5, 13);
                } else {
                    newCols[28] = colStr;
                }
            } else if (length > 4 && colStr.substring(0, 4).equals("vid:")) {
                newCols[29] = colStr;
            } else {
            }
        }

        this.cols = newCols;
        return 30;
    }
}
