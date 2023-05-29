package com.taosdata.utils.arrow;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.List;

/**
 * arrow初始化使用的实体类
 *
 * @author ZYP
 */
@Data
public class ArrowInitDto {

    private String name;
    private List<Column> columns;
    private List<Tag> tags;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }

    /**
     * 转换为rust支持的类型
     *
     * @param type
     * @return
     */
    private String convertType(String type) {
        switch (type) {
            case "boolean":
                return "bool";
            case "integer":
            case "long":
                return "bigint";
            case "float":
            case "double":
                return "double";
            case "date":
            case "timestamp":
                return "timestamp";
            case "string":
            default: {
                return "nchar(1000)";
            }
        }
    }

    @Data
    class Column {
        private String name;
        private String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = convertType(type);
        }
    }

    @Data
    class Tag {
        private String name;
        private String type;

        public Tag(String name, String type) {
            this.name = name;
            this.type = convertType(type);
        }
    }
}
