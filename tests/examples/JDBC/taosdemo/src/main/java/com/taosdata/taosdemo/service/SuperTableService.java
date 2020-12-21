package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.SuperTableMapper;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
            String sql = SqlSpeller.createSuperTable(superTableMeta);
            logger.info(">>> " + sql);
            result = statement.executeUpdate(sql);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}
