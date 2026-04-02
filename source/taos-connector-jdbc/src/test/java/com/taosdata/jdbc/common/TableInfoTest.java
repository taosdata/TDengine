import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.TableInfo;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class TableInfoTest {

    private List<ColumnInfo> mockDataList;
    private ByteBuffer testTableNameBuffer;
    private List<ColumnInfo> mockTagInfo;
    private ColumnInfo mockColumn1;
    private ColumnInfo mockColumn2;

    @Before
    public void setUp() {
        mockColumn1 = mock(ColumnInfo.class);
        mockColumn2 = mock(ColumnInfo.class);

        mockDataList = new ArrayList<>();
        mockDataList.add(mockColumn1);
        mockDataList.add(mockColumn2);

        testTableNameBuffer = ByteBuffer.wrap("sensor_table_001".getBytes());

        mockTagInfo = new ArrayList<>();
        mockTagInfo.add(mockColumn1);
    }

    @Test
    public void testConstructor_InitPropertiesCorrectly() {
        TableInfo tableInfo = new TableInfo(mockDataList, testTableNameBuffer, mockTagInfo);

        assertSame(mockDataList, tableInfo.getDataList());
        assertEquals(2, tableInfo.getDataList().size());
        assertSame(mockColumn1, tableInfo.getDataList().get(0));

        ByteBuffer retrievedTableName = tableInfo.getTableName();
        assertArrayEquals(testTableNameBuffer.array(), retrievedTableName.array());

        assertSame(mockTagInfo, tableInfo.getTagInfo());
        assertEquals(1, tableInfo.getTagInfo().size());
    }

    @Test
    public void testGetEmptyTableInfo_ReturnsEmptyInstance() {
        TableInfo emptyTable = TableInfo.getEmptyTableInfo();

        assertNotNull(emptyTable.getDataList());
        assertTrue(emptyTable.getDataList().isEmpty());

        assertNotNull(emptyTable.getTableName());
        assertEquals(0, emptyTable.getTableName().remaining());
        assertArrayEquals(new byte[0], emptyTable.getTableName().array());

        assertNotNull(emptyTable.getTagInfo());
        assertTrue(emptyTable.getTagInfo().isEmpty());
    }

    @Test
    public void testSetDataList_UpdatesDataList() {
        TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        assertTrue(tableInfo.getDataList().isEmpty());

        List<ColumnInfo> newDataList = new ArrayList<>();
        newDataList.add(mockColumn2);
        tableInfo.setDataList(newDataList);

        assertSame(newDataList, tableInfo.getDataList());
        assertEquals(1, tableInfo.getDataList().size());
        assertSame(mockColumn2, tableInfo.getDataList().get(0));
    }

    @Test
    public void testSetTableName_UpdatesTableName() {
        TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        assertEquals(0, tableInfo.getTableName().remaining());

        ByteBuffer newTableName = ByteBuffer.wrap("new_sensor_table".getBytes());
        tableInfo.setTableName(newTableName);

        assertSame(newTableName, tableInfo.getTableName());
        assertArrayEquals(newTableName.array(), tableInfo.getTableName().array());
    }

    @Test
    public void testSetTagInfo_UpdatesTagInfo() {
        TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        assertTrue(tableInfo.getTagInfo().isEmpty());

        List<ColumnInfo> newTagInfo = new ArrayList<>();
        newTagInfo.add(mockColumn1);
        newTagInfo.add(mockColumn2);
        tableInfo.setTagInfo(newTagInfo);

        assertSame(newTagInfo, tableInfo.getTagInfo());
        assertEquals(2, tableInfo.getTagInfo().size());
    }

    @Test
    public void testSetters_WithNullValues() {
        TableInfo tableInfo = TableInfo.getEmptyTableInfo();

        tableInfo.setDataList(null);
        tableInfo.setTableName(null);
        tableInfo.setTagInfo(null);

        assertNull(tableInfo.getDataList());
        assertNull(tableInfo.getTableName());
        assertNull(tableInfo.getTagInfo());

        TableInfo nullConstructorTable = new TableInfo(null, null, null);
        assertNull(nullConstructorTable.getDataList());
        assertNull(nullConstructorTable.getTableName());
        assertNull(nullConstructorTable.getTagInfo());
    }
}