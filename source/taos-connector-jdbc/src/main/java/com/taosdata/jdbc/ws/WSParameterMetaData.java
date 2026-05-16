package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractParameterMetaData;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.entity.Field;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class WSParameterMetaData extends AbstractParameterMetaData {

    private final ArrayList<Byte> colTypeList;
    private final boolean isInsert;
    private final List<Field> fields;

    public WSParameterMetaData(boolean isInsert, List<Field> fields, ArrayList<Byte> parameters) {
        super(null);
        this.colTypeList = parameters;
        this.isInsert = isInsert;
        this.fields = fields;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return colTypeList == null ? 0 : colTypeList.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        if (!isInsert) {
            return ParameterMetaData.parameterNullable;
        }
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (fields.get(param - 1).getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
            return ParameterMetaData.parameterNoNulls;
        }

        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return false;
        }

        return colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_TINYINT
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_SMALLINT
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_INT
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_BIGINT
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_FLOAT
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_DOUBLE
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_DECIMAL64
            || colTypeList.get(param - 1) == TSDBConstants.TSDB_DATA_TYPE_DECIMAL128;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return 0;
        }

        return fields.get(param - 1).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return 0;
        }

        return fields.get(param - 1).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return Types.OTHER;
        }

        byte colType = colTypeList.get(param - 1);
        return DataType.convertTaosType2DataType(colType).getJdbcTypeValue();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return "";
        }

        byte colType = colTypeList.get(param - 1);
        return DataType.convertTaosType2DataType(colType).getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (!isInsert){
            return "";
        }

        byte colType = colTypeList.get(param - 1);
        return DataType.convertTaosType2DataType(colType).getClassName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        if (param < 1 || param > colTypeList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return ParameterMetaData.parameterModeIn;
    }
}
