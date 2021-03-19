package com.taosdata.jdbc;

import com.taosdata.jdbc.bean.TSDBPreparedParam;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * this class is used to precompile the sql of tdengine insert or import ops
 */
public class SavedPreparedStatement {

    private TSDBPreparedStatement tsdbPreparedStatement;

    /**
     * sql param List
     */
    private List<TSDBPreparedParam> sqlParamList;

    /**
     * init param according the sql
     */
    private TSDBPreparedParam initPreparedParam;

    /**
     * is table name dynamic in the prepared sql
     */
    private boolean isTableNameDynamic;

    /**
     * insert or import sql template pattern, the template are the following:
     * <p>
     * insert/import into tableName [(field1, field2, ...)] [using stables tags(?, ?, ...) ] values(?, ?, ...) (?, ?, ...)
     * <p>
     * we split it to three part:
     * 1. prefix, insert/import
     * 2. middle, tableName [(field1, field2, ...)] [using stables tags(?, ?, ...) ]
     * 3. valueList, the content after values, for example (?, ?, ...) (?, ?, ...)
     */
    private Pattern sqlPattern = Pattern.compile("(?s)(?i)^\\s*(INSERT|IMPORT)\\s+INTO\\s+((?<tablename>\\S+)\\s*(\\(.*\\))?\\s+(USING\\s+(?<stableName>\\S+)\\s+TAGS\\s*\\((?<tagValue>.+)\\))?)\\s*VALUES\\s*(?<valueList>\\(.*\\)).*");

    /**
     * the raw sql template
     */
    private String sql;

    /**
     * the prefix part of sql
     */
    private String prefix;

    /**
     * the middle part of sql
     */
    private String middle;

    private int middleParamSize;

    /**
     * the valueList part of sql
     */
    private String valueList;

    private int valueListSize;

    /**
     * default param value
     */
    private static final String DEFAULT_VALUE = "NULL";

    private static final String PLACEHOLDER = "?";

    private String tableName;

    /**
     * is the parameter add to batch list
     */
    private boolean isAddBatch;

    public SavedPreparedStatement(String sql, TSDBPreparedStatement tsdbPreparedStatement) throws SQLException {
        this.sql = sql;
        this.tsdbPreparedStatement = tsdbPreparedStatement;
        this.sqlParamList = new ArrayList<>();

        parsePreparedParam(this.sql);
    }

    /**
     * parse the init param according the sql param
     *
     * @param sql
     */
    private void parsePreparedParam(String sql) throws SQLException {

        Matcher matcher = sqlPattern.matcher(sql);

        if (matcher.find()) {

            tableName = matcher.group("tablename");

            if (tableName != null && PLACEHOLDER.equals(tableName)) {
                // the table name is dynamic
                this.isTableNameDynamic = true;
            }

            prefix = matcher.group(1);
            middle = matcher.group(2);
            valueList = matcher.group("valueList");

            if (middle != null && !"".equals(middle)) {
                middleParamSize = parsePlaceholder(middle);
            }

            if (valueList != null && !"".equals(valueList)) {
                valueListSize = parsePlaceholder(valueList);
            }

            initPreparedParam = initDefaultParam(tableName, middleParamSize, valueListSize);

        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_SQL);
        }

    }

    private TSDBPreparedParam initDefaultParam(String tableName, int middleParamSize, int valueListSize) {

        TSDBPreparedParam tsdbPreparedParam = new TSDBPreparedParam(tableName);

        tsdbPreparedParam.setMiddleParamList(getDefaultParamList(middleParamSize));

        tsdbPreparedParam.setValueList(getDefaultParamList(valueListSize));

        return tsdbPreparedParam;
    }

    /**
     * generate the default param value list
     *
     * @param paramSize
     * @return
     */
    private List<Object> getDefaultParamList(int paramSize) {

        List<Object> paramList = new ArrayList<>(paramSize);
        if (paramSize > 0) {
            for (int i = 0; i < paramSize; i++) {
                paramList.add(i, DEFAULT_VALUE);
            }
        }

        return paramList;
    }

    /**
     * calculate the placeholder num
     *
     * @param value
     * @return
     */
    private int parsePlaceholder(String value) {

        Pattern pattern = Pattern.compile("[?]");

        Matcher matcher = pattern.matcher(value);

        int result = 0;
        while (matcher.find()) {
            result++;
        }
        return result;
    }

    /**
     * set current row params
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     */
    public void setParam(int parameterIndex, Object x) throws SQLException {

        int paramSize = this.middleParamSize + this.valueListSize;

        String errorMsg = String.format("the parameterIndex %s out of the range [1, %s]", parameterIndex, paramSize);

        if (parameterIndex < 1 || parameterIndex > paramSize) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE,errorMsg);
        }

        this.isAddBatch = false; //set isAddBatch to false

        if (x == null) {
            x = DEFAULT_VALUE; // set default null string
        }

        parameterIndex = parameterIndex - 1; // start from 0 in param list

        if (this.middleParamSize > 0 && parameterIndex >= 0 && parameterIndex < this.middleParamSize) {

            this.initPreparedParam.setMiddleParam(parameterIndex, x);
            return;
        }

        if (this.valueListSize > 0 && parameterIndex >= this.middleParamSize && parameterIndex < paramSize) {

            this.initPreparedParam.setValueParam(parameterIndex - this.middleParamSize, x);
            return;
        }

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE,errorMsg);
    }

    public void addBatch() {

        addCurrentRowParamToList();
        this.initPreparedParam = initDefaultParam(tableName, middleParamSize, valueListSize);
    }

    /**
     * add current param to batch list
     */
    private void addCurrentRowParamToList() {

        if (initPreparedParam != null && (this.middleParamSize > 0 || this.valueListSize > 0)) {
            this.sqlParamList.add(initPreparedParam); // add current param to batch list
        }
        this.isAddBatch = true;
    }


    /**
     * execute the sql with batch sql
     *
     * @return
     * @throws SQLException
     */
    public int[] executeBatch() throws SQLException {

        int result = executeBatchInternal();

        return new int[]{result};
    }


    public int executeBatchInternal() throws SQLException {

        if (!isAddBatch) {
            addCurrentRowParamToList(); // add current param to batch list
        }

        //1. generate batch sql
        String sql = generateExecuteSql();
        //2. execute batch sql
        int result = executeSql(sql);

        //3. clear batch param list
        this.sqlParamList.clear();

        return result;
    }

    /**
     * generate the batch sql
     *
     * @return
     */
    private String generateExecuteSql() {

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(prefix);
        stringBuilder.append(" into ");

        if (!isTableNameDynamic) {
            // tablename will not need to be replaced
            String middleValue = replaceMiddleListParam(middle, sqlParamList);
            stringBuilder.append(middleValue);
            stringBuilder.append(" values");

            stringBuilder.append(replaceValueListParam(valueList, sqlParamList));

        } else {
            // need to replace tablename

            if (sqlParamList.size() > 0) {

                TSDBPreparedParam firstPreparedParam = sqlParamList.get(0);

                //replace middle part and value part of first row
                String firstRow = replaceMiddleAndValuePart(firstPreparedParam);
                stringBuilder.append(firstRow);

                //the first param in the middleParamList is the tableName
                String lastTableName = firstPreparedParam.getMiddleParamList().get(0).toString();

                if (sqlParamList.size() > 1) {

                    for (int i = 1; i < sqlParamList.size(); i++) {
                        TSDBPreparedParam currentParam = sqlParamList.get(i);
                        String currentTableName = currentParam.getMiddleParamList().get(0).toString();
                        if (lastTableName.equalsIgnoreCase(currentTableName)) {
                            // tablename is same with the last row ,so only need to append the part of value

                            String values = replaceTemplateParam(valueList, currentParam.getValueList());
                            stringBuilder.append(values);

                        } else {
                            // tablename difference with the last row
                            //need to replace middle part and value part
                            String row = replaceMiddleAndValuePart(currentParam);
                            stringBuilder.append(row);
                            lastTableName = currentTableName;
                        }
                    }
                }

            } else {

                stringBuilder.append(middle);
                stringBuilder.append(" values");
                stringBuilder.append(valueList);
            }

        }

        return stringBuilder.toString();
    }

    /**
     * replace the middle and value part
     *
     * @param tsdbPreparedParam
     * @return
     */
    private String replaceMiddleAndValuePart(TSDBPreparedParam tsdbPreparedParam) {

        StringBuilder stringBuilder = new StringBuilder(" ");

        String middlePart = replaceTemplateParam(middle, tsdbPreparedParam.getMiddleParamList());

        stringBuilder.append(middlePart);
        stringBuilder.append(" values ");

        String valuePart = replaceTemplateParam(valueList, tsdbPreparedParam.getValueList());
        stringBuilder.append(valuePart);
        stringBuilder.append(" ");

        return stringBuilder.toString();
    }

    /**
     * replace the placeholder of the middle part of sql template with TSDBPreparedParam list
     *
     * @param template
     * @param sqlParamList
     * @return
     */
    private String replaceMiddleListParam(String template, List<TSDBPreparedParam> sqlParamList) {

        if (sqlParamList.size() > 0) {

            //becase once the subTableName is static then  will be ignore the tag which after the first setTag
            return replaceTemplateParam(template, sqlParamList.get(0).getMiddleParamList());

        }

        return template;
    }


    /**
     * replace the placeholder of the template with TSDBPreparedParam list
     *
     * @param template
     * @param sqlParamList
     * @return
     */
    private String replaceValueListParam(String template, List<TSDBPreparedParam> sqlParamList) {

        StringBuilder stringBuilder = new StringBuilder();

        if (sqlParamList.size() > 0) {

            for (TSDBPreparedParam tsdbPreparedParam : sqlParamList) {

                String tmp = replaceTemplateParam(template, tsdbPreparedParam.getValueList());

                stringBuilder.append(tmp);
            }

        } else {
            stringBuilder.append(template);
        }

        return stringBuilder.toString();
    }

    /**
     * replace the placeholder of the template with paramList
     *
     * @param template
     * @param paramList
     * @return
     */
    private String replaceTemplateParam(String template, List<Object> paramList) {

        if (paramList.size() > 0) {

            String tmp = template;

            for (int i = 0; i < paramList.size(); ++i) {

                String paraStr = getParamString(paramList.get(i));

                tmp = tmp.replaceFirst("[" + PLACEHOLDER + "]", paraStr);

            }

            return tmp;

        } else {
            return template;
        }

    }

    /**
     * get the string of param object
     *
     * @param paramObj
     * @return
     */
    private String getParamString(Object paramObj) {

        String paraStr = paramObj.toString();
        if (paramObj instanceof Timestamp || (paramObj instanceof String && !DEFAULT_VALUE.equalsIgnoreCase(paraStr))) {
            paraStr = "'" + paraStr + "'";
        }
        return paraStr;
    }


    private int executeSql(String sql) throws SQLException {

        return tsdbPreparedStatement.executeUpdate(sql);
    }

}
