package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.mapper.SuperTableMapper;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Service
public class SuperTableService {

    private static Logger logger = Logger.getLogger(SuperTableService.class);
    @Autowired
    private SuperTableMapper superTableMapper;

    private DataSource dataSource;

    public SuperTableService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // 创建超级表，指定每个field的名称和类型，每个tag的名称和类型
    public int create(SuperTableMeta superTableMeta) {
        int result = 0;
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = sql(superTableMeta);
            logger.info(">>> " + sql);
            result = statement.executeUpdate(sql);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
//        return superTableMapper.createSuperTable(superTableMeta);
    }

    private String sql(SuperTableMeta superTableMeta) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table " + superTableMeta.getDatabase() + "." + superTableMeta.getName() + "(");
        List<FieldMeta> fields = superTableMeta.getFields();
        for (int i = 0; i < fields.size(); i++) {
            FieldMeta fieldMeta = fields.get(i);
            if (i == 0) {
                sb.append(fieldMeta.getName() + " " + fieldMeta.getType());
            } else {
                sb.append(", " + fieldMeta.getName() + " " + fieldMeta.getType());
            }
        }
        sb.append(") tags(");
        List<TagMeta> tags = superTableMeta.getTags();
        for (int i = 0; i < tags.size(); i++) {
            TagMeta tagMeta = tags.get(i);
            if (i == 0) {
                sb.append(tagMeta.getName() + " " + tagMeta.getType());
            } else {
                sb.append(", " + tagMeta.getName() + " " + tagMeta.getType() + "");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}
