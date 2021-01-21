/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * TDengine only supports a subset of the standard SQL, thus this implemetation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class CatalogResultSet extends TSDBResultSetWrapper {

    public CatalogResultSet(ResultSet resultSet) {
        super.setOriginalResultSet(resultSet);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (columnIndex <= 1) {
            return super.getString(columnIndex);
        } else {
            return null;
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (columnIndex <= 1) {
            return super.getBoolean(columnIndex);
        } else {
            return false;
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (columnIndex <= 1) {
            return super.getBytes(columnIndex);
        } else {
            return null;
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (columnIndex <= 1) {
            return super.getObject(columnIndex);
        } else {
            return null;
        }
    }

}
