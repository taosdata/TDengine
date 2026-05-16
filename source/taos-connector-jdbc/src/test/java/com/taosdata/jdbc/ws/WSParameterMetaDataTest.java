package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import org.junit.Before;
import org.junit.Test;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WSParameterMetaDataTest {

    private WSParameterMetaData parameterMetaData;
    private List<Field> fields;
    private ArrayList<Byte> colTypeList;

    @Before
    public void setUp() {
        // create test data
        fields = new ArrayList<>();

        // first field is table name field
        Field field1 = new Field();
        field1.setBindType((byte)FieldBindType.TAOS_FIELD_TBNAME.getValue());
        field1.setPrecision((byte) 10);
        field1.setScale((byte) 2);
        fields.add(field1);

        // second field is TINYINT type
        Field field2 = new Field();
        field2.setBindType((byte)DataType.TINYINT.getTaosTypeValue());
        field2.setPrecision((byte) 5);
        field2.setScale((byte) 0);
        fields.add(field2);

        // third field is DOUBLE type
        Field field3 = new Field();
        field3.setBindType((byte)DataType.DOUBLE.getTaosTypeValue());
        field3.setPrecision((byte) 15);
        field3.setScale((byte) 5);
        fields.add(field3);

        colTypeList = new ArrayList<>();
        colTypeList.add((byte)DataType.TINYINT.getTaosTypeValue());
        colTypeList.add((byte)DataType.DOUBLE.getTaosTypeValue());
        colTypeList.add((byte)DataType.VARCHAR.getTaosTypeValue());

        // create test object (insert mode)
        parameterMetaData = new WSParameterMetaData(true, fields, colTypeList);
    }

    @Test
    public void testGetParameterCount() throws SQLException {
        assertEquals(3, parameterMetaData.getParameterCount());
    }

    @Test
    public void testGetParameterCountWithNullList() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(true, fields, null);
        assertEquals(0, metaData.getParameterCount());
    }

    @Test
    public void testIsNullableForInsert() throws SQLException {
        // first field is table name field, should not allow null
        assertEquals(ParameterMetaData.parameterNoNulls, parameterMetaData.isNullable(1));

        // other fields should return unknown
        assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData.isNullable(2));
        assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData.isNullable(3));
    }

    @Test
    public void testIsNullableForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, all parameters should be nullable
        assertEquals(ParameterMetaData.parameterNullable, metaData.isNullable(1));
        assertEquals(ParameterMetaData.parameterNullable, metaData.isNullable(2));
        assertEquals(ParameterMetaData.parameterNullable, metaData.isNullable(3));
    }

    @Test(expected = SQLException.class)
    public void testIsNullableWithInvalidIndexLow() throws SQLException {
        parameterMetaData.isNullable(0);
    }

    @Test(expected = SQLException.class)
    public void testIsNullableWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.isNullable(4);
    }

    @Test
    public void testIsSignedForInsert() throws SQLException {
        assertTrue(parameterMetaData.isSigned(1));
        assertTrue(parameterMetaData.isSigned(2));

        assertFalse(parameterMetaData.isSigned(3));
    }

    @Test
    public void testIsSignedForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, all parameters should be unsigned
        assertFalse(metaData.isSigned(1));
        assertFalse(metaData.isSigned(2));
        assertFalse(metaData.isSigned(3));
    }

    @Test(expected = SQLException.class)
    public void testIsSignedWithInvalidIndexLow() throws SQLException {
        parameterMetaData.isSigned(0);
    }

    @Test(expected = SQLException.class)
    public void testIsSignedWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.isSigned(4);
    }

    @Test
    public void testGetPrecisionForInsert() throws SQLException {
        assertEquals(10, parameterMetaData.getPrecision(1));
        assertEquals(5, parameterMetaData.getPrecision(2));
        assertEquals(15, parameterMetaData.getPrecision(3));
    }

    @Test
    public void testGetPrecisionForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, precision should return 0
        assertEquals(0, metaData.getPrecision(1));
        assertEquals(0, metaData.getPrecision(2));
        assertEquals(0, metaData.getPrecision(3));
    }

    @Test(expected = SQLException.class)
    public void testGetPrecisionWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getPrecision(0);
    }

    @Test(expected = SQLException.class)
    public void testGetPrecisionWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getPrecision(4);
    }

    @Test
    public void testGetScaleForInsert() throws SQLException {
        assertEquals(2, parameterMetaData.getScale(1));
        assertEquals(0, parameterMetaData.getScale(2));
        assertEquals(5, parameterMetaData.getScale(3));
    }

    @Test
    public void testGetScaleForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, scale should return 0
        assertEquals(0, metaData.getScale(1));
        assertEquals(0, metaData.getScale(2));
        assertEquals(0, metaData.getScale(3));
    }

    @Test(expected = SQLException.class)
    public void testGetScaleWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getScale(0);
    }

    @Test(expected = SQLException.class)
    public void testGetScaleWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getScale(4);
    }

    @Test
    public void testGetParameterTypeForInsert() throws SQLException {
        assertEquals(DataType.TINYINT.getJdbcTypeValue(), parameterMetaData.getParameterType(1));
        assertEquals(DataType.DOUBLE.getJdbcTypeValue(), parameterMetaData.getParameterType(2));
        assertEquals(DataType.VARCHAR.getJdbcTypeValue(), parameterMetaData.getParameterType(3));
    }

    @Test
    public void testGetParameterTypeForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, should return Types.OTHER
        assertEquals(Types.OTHER, metaData.getParameterType(1));
        assertEquals(Types.OTHER, metaData.getParameterType(2));
        assertEquals(Types.OTHER, metaData.getParameterType(3));
    }

    @Test(expected = SQLException.class)
    public void testGetParameterTypeWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getParameterType(0);
    }

    @Test(expected = SQLException.class)
    public void testGetParameterTypeWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getParameterType(4);
    }

    @Test
    public void testGetParameterTypeNameForInsert() throws SQLException {
        assertEquals(DataType.TINYINT.getTypeName(), parameterMetaData.getParameterTypeName(1));
        assertEquals(DataType.DOUBLE.getTypeName(), parameterMetaData.getParameterTypeName(2));
        assertEquals(DataType.BINARY.getTypeName(), parameterMetaData.getParameterTypeName(3));
    }

    @Test
    public void testGetParameterTypeNameForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, should return empty string
        assertEquals("", metaData.getParameterTypeName(1));
        assertEquals("", metaData.getParameterTypeName(2));
        assertEquals("", metaData.getParameterTypeName(3));
    }

    @Test(expected = SQLException.class)
    public void testGetParameterTypeNameWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getParameterTypeName(0);
    }

    @Test(expected = SQLException.class)
    public void testGetParameterTypeNameWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getParameterTypeName(4);
    }

    @Test
    public void testGetParameterClassNameForInsert() throws SQLException {
        assertEquals(DataType.TINYINT.getClassName(), parameterMetaData.getParameterClassName(1));
        assertEquals(DataType.DOUBLE.getClassName(), parameterMetaData.getParameterClassName(2));
        assertEquals(DataType.VARCHAR.getClassName(), parameterMetaData.getParameterClassName(3));
    }

    @Test
    public void testGetParameterClassNameForNonInsert() throws SQLException {
        WSParameterMetaData metaData = new WSParameterMetaData(false, fields, colTypeList);
        // non-insert mode, should return empty string
        assertEquals("", metaData.getParameterClassName(1));
        assertEquals("", metaData.getParameterClassName(2));
        assertEquals("", metaData.getParameterClassName(3));
    }

    @Test(expected = SQLException.class)
    public void testGetParameterClassNameWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getParameterClassName(0);
    }

    @Test(expected = SQLException.class)
    public void testGetParameterClassNameWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getParameterClassName(4);
    }

    @Test
    public void testGetParameterMode() throws SQLException {
        // all parameters should be in mode
        assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(1));
        assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(2));
        assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(3));
    }

    @Test(expected = SQLException.class)
    public void testGetParameterModeWithInvalidIndexLow() throws SQLException {
        parameterMetaData.getParameterMode(0);
    }

    @Test(expected = SQLException.class)
    public void testGetParameterModeWithInvalidIndexHigh() throws SQLException {
        parameterMetaData.getParameterMode(4);
    }

    @Test
    public void testErrorNumbers() {
        // verify error code constants
        assertTrue(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE > 0);
    }

    @Test
    public void testFieldProperties() {
        Field field = new Field();
        field.setName("test_field");
        field.setFieldType((byte) 1);
        field.setPrecision((byte) 10);
        field.setScale((byte) 2);
        field.setBytes(20);
        field.setBindType((byte) 3);

        assertEquals("test_field", field.getName());
        assertEquals(1, field.getFieldType());
        assertEquals(10, field.getPrecision());
        assertEquals(2, field.getScale());
        assertEquals(20, field.getBytes());
        assertEquals(3, field.getBindType());
    }
}