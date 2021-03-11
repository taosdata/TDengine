package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

public class TSDBJNIConnectorTest {

    private static TSDBResultSetRowData rowData;

    public static void main(String[] args) {
        try {
            TSDBJNIConnector.init("/etc/taos/taos.cfg", "en_US.UTF-8", "", "");
            TSDBJNIConnector connector = new TSDBJNIConnector();
            connector.connect("127.0.0.1", 6030, "test", "root", "taosdata");
            long pSql = connector.executeQuery("show dnodes");
            // if pSql is create/insert/update/delete/alter SQL
            if (connector.isUpdateQuery(pSql)) {
                connector.freeResultSet(pSql);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY);
            }

            List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
            int code = connector.getSchemaMetaData(pSql, columnMetaDataList);
            if (code == TSDBConstants.JNI_CONNECTION_NULL) {
                throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
            }
            if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
            }
            if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
                throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_NUM_OF_FIELDS_0));
            }
            int columnSize = columnMetaDataList.size();
            // print metadata
            for (int i = 0; i < columnSize; i++) {
                System.out.println(columnMetaDataList.get(i));
            }
            rowData = new TSDBResultSetRowData(columnSize);
            // iterate resultSet
            while (next(connector, pSql)) {
                System.out.println(rowData.getColSize());
                rowData.getData().stream().forEach(System.out::println);
            }
            // close resultSet
            code = connector.freeResultSet(pSql);
            if (code == TSDBConstants.JNI_CONNECTION_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
            }
            // close statement
            // close connection
            connector.closeConnection();

        } catch (SQLWarning throwables) {
            throwables.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean next(TSDBJNIConnector connector, long pSql) throws SQLException {
        if (rowData != null)
            rowData.clear();

        int code = connector.fetchRow(pSql, rowData);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        } else if (code == TSDBConstants.JNI_FETCH_END) {
            return false;
        } else {
            return true;
        }
    }

}