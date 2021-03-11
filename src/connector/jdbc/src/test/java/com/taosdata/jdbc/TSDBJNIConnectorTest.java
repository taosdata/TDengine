package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TSDBJNIConnectorTest {

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

        } catch (SQLWarning throwables) {
            throwables.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void isClosed() {
    }

    @Test
    public void isResultsetClosed() {
    }

    @Test
    public void init() {
    }

    @Test
    public void initImp() {
    }

    @Test
    public void setOptions() {
    }

    @Test
    public void getTsCharset() {
    }

    @Test
    public void connect() {
    }

    @Test
    public void executeQuery() {
    }

    @Test
    public void getErrCode() {
    }

    @Test
    public void getErrMsg() {
    }

    @Test
    public void isUpdateQuery() {
    }

    @Test
    public void freeResultSet() {
    }

    @Test
    public void getAffectedRows() {
    }

    @Test
    public void getSchemaMetaData() {
    }

    @Test
    public void fetchRow() {
    }

    @Test
    public void fetchBlock() {
    }

    @Test
    public void closeConnection() {
    }

    @Test
    public void subscribe() {
    }

    @Test
    public void consume() {
    }

    @Test
    public void unsubscribe() {
    }

    @Test
    public void validateCreateTableSql() {
    }
}