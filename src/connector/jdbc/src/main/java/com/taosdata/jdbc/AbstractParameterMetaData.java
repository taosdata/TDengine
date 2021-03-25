package com.taosdata.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public abstract class AbstractParameterMetaData extends WrapperImpl implements ParameterMetaData {

    private final Object[] parameters;

    public AbstractParameterMetaData(Object[] parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameters.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Byte)
            return true;
        if (parameters[param] instanceof Short)
            return true;
        if (parameters[param] instanceof Integer)
            return true;
        if (parameters[param] instanceof Long)
            return true;
        if (parameters[param] instanceof Float)
            return true;
        if (parameters[param] instanceof Double)
            return true;

        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof String)
            return ((String) parameters[param]).length();
        if (parameters[param - 1] instanceof byte[])
            return ((byte[]) parameters[param]).length;
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Timestamp)
            return Types.TIMESTAMP;
        if (parameters[param - 1] instanceof Byte)
            return Types.TINYINT;
        if (parameters[param - 1] instanceof Short)
            return Types.SMALLINT;
        if (parameters[param - 1] instanceof Integer)
            return Types.INTEGER;
        if (parameters[param - 1] instanceof Long)
            return Types.BIGINT;
        if (parameters[param - 1] instanceof Float)
            return Types.FLOAT;
        if (parameters[param - 1] instanceof Double)
            return Types.DOUBLE;
        if (parameters[param - 1] instanceof String)
            return Types.NCHAR;
        if (parameters[param - 1] instanceof byte[])
            return Types.BINARY;
        if (parameters[param - 1] instanceof Boolean)
            return Types.BOOLEAN;
        return Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (parameters[param - 1] instanceof Timestamp)
            return TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP);
        if (parameters[param - 1] instanceof Byte)
            return TSDBConstants.jdbcType2TaosTypeName(Types.TINYINT);
        if (parameters[param - 1] instanceof Short)
            return TSDBConstants.jdbcType2TaosTypeName(Types.SMALLINT);
        if (parameters[param - 1] instanceof Integer)
            return TSDBConstants.jdbcType2TaosTypeName(Types.INTEGER);
        if (parameters[param - 1] instanceof Long)
            return TSDBConstants.jdbcType2TaosTypeName(Types.BIGINT);
        if (parameters[param - 1] instanceof Float)
            return TSDBConstants.jdbcType2TaosTypeName(Types.FLOAT);
        if (parameters[param - 1] instanceof Double)
            return TSDBConstants.jdbcType2TaosTypeName(Types.DOUBLE);
        if (parameters[param - 1] instanceof String)
            return TSDBConstants.jdbcType2TaosTypeName(Types.NCHAR);
        if (parameters[param - 1] instanceof byte[])
            return TSDBConstants.jdbcType2TaosTypeName(Types.BINARY);
        if (parameters[param - 1] instanceof Boolean)
            return TSDBConstants.jdbcType2TaosTypeName(Types.BOOLEAN);

        return parameters[param - 1].getClass().getName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return parameters[param - 1].getClass().getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        if (param < 1 && param >= parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return ParameterMetaData.parameterModeUnknown;
    }
}
