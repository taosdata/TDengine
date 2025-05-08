
struct DatabaseMeta {
    struct {
        std::string name;
        bool drop;
        int vgroups;
        std::string precision;
    } dbinfo;
    
    struct SuperTableMeta {
        std::string name;
        bool child_table_exists;
        int childtable_count;
        std::string childtable_prefix;
        bool auto_create_table;
        int batch_create_tbl_num;
        std::string data_source;
        std::string insert_mode;
        bool non_stop_mode;
        std::string line_protocol;
        int insert_rows;
        // ...其他字段
        std::vector<ColumnMeta> columns;
        std::vector<TagMeta> tags;
    };
    
    std::vector<SuperTableMeta> super_tables;
};



// 自定义结构体定义
struct ColumnMeta {
    std::string type;
    std::string name;
    int count;
    double max;
    double min;
};



struct TagMeta {
    std::string type;
    std::string name;
    
    // 字符串类型专用字段
    std::optional<int> length;
    std::optional<std::vector<std::string>> values;
    
    // 数值类型专用字段
    std::optional<double> max;
    std::optional<double> min;
    std::optional<int> count;
  
    // 类型校验方法
    void validate() const {
        const std::string lower_type = to_lower(type);
        
        if (is_numeric_type(lower_type)) {
            if (!max || !min) {
                throw ConfigError("Numeric tag requires max/min values");
            }
            if (length || values) {
                throw ConfigError("Numeric tag cannot have string attributes");
            }
        } else if (is_string_type(lower_type)) {
            if (!length && !values) {
                throw ConfigError("String tag requires length or values");
            }
            if (max || min || count) {
                throw ConfigError("String tag cannot have numeric attributes");
            }
        }
    }
  
  private:
    static bool is_numeric_type(const std::string& t) {
        return t.find("int") != std::string::npos || t == "float" || t == "double";
    }
  
    static bool is_string_type(const std::string& t) {
        return t == "binary" || t == "nchar" || t == "json";
    }
  };


