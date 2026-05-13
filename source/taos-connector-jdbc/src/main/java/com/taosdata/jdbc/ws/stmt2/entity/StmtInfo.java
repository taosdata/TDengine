package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.enums.FieldBindType;

import java.util.ArrayList;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;

public class StmtInfo {
    private long stmtId = 0;
    private int toBeBindTableNameIndex = -1;
    private int toBeBindTagCount;
    private int toBeBindColCount;
    private int precision;
    private List<Field> fields;
    private final String sql;

    protected final ArrayList<Byte> tagTypeList = new ArrayList<>();
    protected ArrayList<Byte> colTypeList = new ArrayList<>();
    protected boolean isInsert = false;
    public StmtInfo(String sql) {
        this.sql = sql;
    }
    public StmtInfo(Stmt2PrepareResp prepareResp, String sql) {
        this.stmtId = prepareResp.getStmtId();
        this.sql = sql;

        isInsert = prepareResp.isInsert();
        if (isInsert){
            fields = prepareResp.getFields();
            for (int i = 0; i < fields.size(); i++){
                Field field = fields.get(i);

                // timestamp precision is same in one database
                if (field.getFieldType() == TSDB_DATA_TYPE_TIMESTAMP){
                    precision = field.getPrecision();
                }

                if (field.getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                    toBeBindTableNameIndex = i;
                }
                if (field.getBindType() == FieldBindType.TAOS_FIELD_TAG.getValue()){
                    toBeBindTagCount++;
                    tagTypeList.add(field.getFieldType());
                }
                if (field.getBindType() == FieldBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount++;
                    colTypeList.add(field.getFieldType());
                }
            }
        } else if (prepareResp.getFieldsCount() > 0){
            toBeBindColCount = prepareResp.getFieldsCount();

            fields = new ArrayList<>();
            for (int i = 0; i < toBeBindColCount; i++){
                colTypeList.add((byte)-1);
                Field field = new Field();
                field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
                fields.add(field);
            }
        }
    }

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public int getToBeBindTableNameIndex() {
        return toBeBindTableNameIndex;
    }

    public void setToBeBindTableNameIndex(int toBeBindTableNameIndex) {
        this.toBeBindTableNameIndex = toBeBindTableNameIndex;
    }

    public int getToBeBindTagCount() {
        return toBeBindTagCount;
    }

    public void setToBeBindTagCount(int toBeBindTagCount) {
        this.toBeBindTagCount = toBeBindTagCount;
    }

    public int getToBeBindColCount() {
        return toBeBindColCount;
    }

    public void setToBeBindColCount(int toBeBindColCount) {
        this.toBeBindColCount = toBeBindColCount;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
    public List<Field> getFields() {
        return fields;
    }
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public String getSql() {
        return sql;
    }

    public ArrayList<Byte> getTagTypeList() {
        return tagTypeList;
    }

    public ArrayList<Byte> getColTypeList() {
        return colTypeList;
    }
    public void setColTypeList(ArrayList<Byte> colTypeList) {
        this.colTypeList = colTypeList;
    }

    public boolean isInsert() {
        return isInsert;
    }
}
