import com.taosdata.jdbc.ColumnMetaData;
import com.taosdata.jdbc.DatabaseMetaDataResultSet;
import com.taosdata.jdbc.TSDBResultSetRowData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestTSDBResultSetRowData {
    public static void main(String[] args) throws SQLException {
        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        List<ColumnMetaData> columnMetaDataList = new ArrayList(1);
        ColumnMetaData colMetaData = new ColumnMetaData();
        colMetaData.setColIndex(0);
        colMetaData.setColName("TABLE_TYPE");
        colMetaData.setColSize(10);
        colMetaData.setColType(8);
        columnMetaDataList.add(colMetaData);

        List<TSDBResultSetRowData> rowDataList = new ArrayList(2);
        TSDBResultSetRowData rowData = new TSDBResultSetRowData(2);
        rowData.setString(0, "TABLE");
        rowDataList.add(rowData);
        rowData = new TSDBResultSetRowData(2);
        rowData.setString(0, "STABLE");
        rowDataList.add(rowData);

        resultSet.setColumnMetaDataList(columnMetaDataList);
        resultSet.setRowDataList(rowDataList);

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
