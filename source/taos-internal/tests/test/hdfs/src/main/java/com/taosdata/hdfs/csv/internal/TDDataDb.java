package com.taosdata.hdfs.csv.internal;


import com.taosdata.hdfs.csv.*;

import java.util.HashMap;
import java.util.HashSet;

public class TDDataDb {
    private static TDConnection connection = null;
    private static TDConnection[] connections = null;
    private static String stableName = "st";
    private static HashMap<String, TDTable> tbMap = new HashMap<String, TDTable>();
    private static long lastTs = 0;

    public static boolean init() {
        connection = new TDConnection(TDConfig.host, TDConfig.user, TDConfig.password);
        if (!connection.connect()) {
            TDLog.print(String.format("datadb connect to tdengine failed, user:%s, password:%s, host:%s, code:%d, reason:%s", TDConfig.user, TDConfig.password, TDConfig.host));
            return false;
        }

        TDLog.print("datadb connect to tdengine success");

        connections = new TDConnection[TDConfig.csvThreadNum];
        for (int t = 0; t < TDConfig.csvThreadNum; ++t) {
            connections[t] = new TDConnection(TDConfig.host, TDConfig.user, TDConfig.password);
            if (!connections[t].connect()) {
                TDLog.error(String.format("datadb thread:%d connect to tdengine failed, user:%s, password:%s, host:%s", t, TDConfig.user, TDConfig.password, TDConfig.host));
                return false;
            } else {
                TDLog.print(String.format("datadb thread:%d connect to tdengine success", t));
            }
        }

        return createSchema();
    }

    public static void close() {
        if (connection != null) {
            connection.close();
        }
        if (connections != null) {
            for (int t = 0; t < TDConfig.csvThreadNum; ++t) {
                connections[t].close();
            }
        }
    }

    public static boolean createSchema() {
        String sql = String.format("create database if not exists %s replica %d days %d keep %d rows %d cache %d ablocks %f tblocks %d tables %d precision %s"
                , TDConfig.datadbName
                , TDConfig.datadbReplica
                , TDConfig.datadbDays
                , TDConfig.datadbKeep
                , TDConfig.datadbRows
                , TDConfig.datadbCache
                , TDConfig.datadbAblocks
                , TDConfig.datadbTblocks
                , TDConfig.datadbTables
                , TDConfig.datadbMicroSecond ? "us" : "ms");
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create database:%s, code:%d, error:%s, sql:%s", TDConfig.datadbName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create database:%s finished, sql:%s", TDConfig.datadbName, sql));
        }

        sql = String.format("use %s", TDConfig.datadbName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to use database:%s, code:%d, error:%s, sql:%s", TDConfig.datadbName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        }

        stableName = String.format("%s.%s", TDConfig.datadbName, TDConfig.stableName);
        sql = String.format("create table if not exists %s (%s %s", stableName, TDConfig.fields[0].name, TDConfig.fields[0].type);
        for (int i = 1; i < (int) TDConfig.fields.length; ++i) {
            TDField field = TDConfig.fields[i];
            sql += String.format(", %s %s", field.name, field.type);
        }
        sql += String.format(") tags(%s %s", TDConfig.tags[0].name, TDConfig.tags[0].type);

        for (int i = 1; i < (int) TDConfig.tags.length; ++i) {
            TDField tag = TDConfig.tags[i];
            sql += String.format(", %s %s", tag.name, tag.type);
        }
        sql += String.format(")");

        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create stable:%s, code:%d, error:%s, sql:%s", stableName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create stable:%s finished, sql:%s", stableName, sql));
        }

        return true;
    }

    public static TDConnection getConnection(int threadIndex) {
        return connections[threadIndex];
    }

    public static synchronized void createTb(TDCsvCache csv) {
        long beginTs = TDUtil.getTimeStampMs();

        int size = (int) csv.lines.size();
        for (int l = 0; l < size; ++l) {
            TDCsvLine line = csv.lines.get(l);
            if (tbMap.containsKey(line.tableName)) {
                continue;
            }
            tbMap.put(line.tableName, null);

            //create table
            StringBuilder sql = new StringBuilder();
            sql.append("create table if not exists ").append(TDConfig.datadbName).append(".")
                    .append(TDConfig.tablePrefix).append(line.tableName)
                    .append(" using ")
                    .append(TDConfig.datadbName).append(".").append(TDConfig.stableName)
                    .append(" tags(");

            for (int t = 0; t < (int) TDConfig.tags.length; ++t) {
                if (t != 0) {
                    sql.append(',');
                }
                TDField tag = TDConfig.tags[t];

                if (tag.isUseTableName) {
                    if (tag.isTypeBinary) {
                        sql.append('\'');
                    }
                    sql.append(line.tableName);
                    if (tag.isTypeBinary) {
                        sql.append('\'');
                    }
                } else if (tag.isUseFileName) {
                    if (tag.isTypeBinary) {
                        sql.append('\'');
                    }
                    sql.append(line.csv.getFileName());
                    if (tag.isTypeBinary) {
                        sql.append('\'');
                    }
                } else {
                    if (tag.isTypeBinary && !TDConfig.binaryContainQuotation) {
                        sql.append('\'');
                    }
                    for (int col : tag.columns) {
                        String tagValue = line.cols[col];
                        if (tagValue == null) {
                            sql.append("NULL");
                        } else if (tagValue.length() == 0) {
                            sql.append("NULL");
                        } else {
                            sql.append(tagValue);
                        }
                    }
                    if (tag.isTypeBinary && !TDConfig.binaryContainQuotation) {
                        sql.append('\'');
                    }
                }
            }
            sql.append(')');

            if (!connection.executeUpdate(sql.toString())) {
                TDLog.error(String.format("file:%s,failed to create table:%s, code:%d, error:%s, sql:%s", csv.getFileName(), line.tableName, connection.getErrorCode(), connection.getErrorStr(), sql.toString()));
            } else {
                TDLog.trace(String.format("file:%s,create table:%s finished, sql:%s", csv.getFileName(), line.tableName, sql.toString()));
            }
        }

        TDLog.print(String.format("file:%s,%d tables already created", csv.getFileName(), (int) tbMap.size()));

        long endTs = TDUtil.getTimeStampMs();
        csv.addCreatetbTimeSec((float) (endTs - beginTs) / 1000);
    }

    public static synchronized TDTable getTbThread(TDCsvLine line) {
        TDTable value = tbMap.get(line.tableName);
        if (value != null) {
            return value;
        }

        value = new TDTable(tbMap.size() % TDConfig.csvThreadNum, 0);
        tbMap.put(line.tableName, value);

        //create table
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists ").append(TDConfig.datadbName).append(".")
                .append(TDConfig.tablePrefix).append(line.tableName)
                .append(" using ")
                .append(TDConfig.datadbName).append(".").append(TDConfig.stableName)
                .append(" tags(");

        for (int t = 0; t < (int) TDConfig.tags.length; ++t) {
            if (t != 0) {
                sql.append(',');
            }
            TDField tag = TDConfig.tags[t];

            if (tag.isUseTableName) {
                if (tag.isTypeBinary) {
                    sql.append('\'');
                }
                sql.append(line.tableName);
                if (tag.isTypeBinary) {
                    sql.append('\'');
                }
            } else if (tag.isUseFileName) {
                if (tag.isTypeBinary) {
                    sql.append('\'');
                }
                sql.append(line.csv.getFileName());
                if (tag.isTypeBinary) {
                    sql.append('\'');
                }
            } else {
                if (tag.isTypeBinary && !TDConfig.binaryContainQuotation) {
                    sql.append('\'');
                }
                for (int col : tag.columns) {
                    String tagValue = line.cols[col];
                    if (tagValue == null) {
                        sql.append("NULL");
                    } else if (tagValue.length() == 0) {
                        sql.append("NULL");
                    } else {
                        sql.append(tagValue);
                    }
                }
                if (tag.isTypeBinary && !TDConfig.binaryContainQuotation) {
                    sql.append('\'');
                }
            }
        }
        sql.append(')');

        if (!connection.executeUpdate(sql.toString())) {
            TDLog.error(String.format("file:%s,failed to create table:%s, code:%d, error:%s, sql:%s", line.csv.getFileName(), line.tableName, connection.getErrorCode(), connection.getErrorStr(), sql.toString()));
        } else {
            //TDLog.trace(String.format("file:%s,create table:%s finished, sql:%s", csv.getFileName(), line.tableName, sql.toString()));
        }

        return value;
    }
}
