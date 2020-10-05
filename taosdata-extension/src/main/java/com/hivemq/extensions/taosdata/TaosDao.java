/*
 * Copyright Copyright 2020-present Marvin Liao <coldljy@163.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.taosdata;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.hivemq.extensions.taosdata.configuration.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库访问类
 */
public class TaosDao {
    private static final Logger log = LoggerFactory.getLogger(TaosDao.class);

    private Properties properties;
    private Connection conn;
    private DataSource ds;

    public TaosDao(Properties properties) throws Exception {
        if (properties == null || properties.isEmpty()) {
            properties = getDefaultProperties();
        }

        this.properties = properties;
        ds = DruidDataSourceFactory.createDataSource(properties);
        conn = open();
    }

    public DataSource getDataSource() {
        return ds;
    }

    public Connection open() throws SQLException {
        return ds.getConnection();
    }

    public void close(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    /**
     * 保存到数据库
     * @param topic 主题
     * @param values 值
     * @return 是否成功
     * @throws SQLException
     */
    public boolean save(Topic topic, Map values) throws SQLException {
        int n = 0;
        try (PreparedStatement statement = conn.prepareStatement(topic.getInsertSql())) {
            List<String> fields = topic.getFields();
            for (int i = 0; i < fields.size(); i++) {
                statement.setObject(i + 1, values.get(fields.get(i)));
            }

            n = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("failed to save data in topic {}", topic.getId(), e);
        }

        return n >= 1;
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();

        properties.put("driverClassName","com.taosdata.jdbc.TSDBDriver");
        properties.put("url","jdbc:TAOS://127.0.0.1:6030/test");
        properties.put("username","root");
        properties.put("password","taosdata");
        properties.put("maxActive","10"); //maximum number of connection in the pool
        properties.put("initialSize","3");//initial number of connection
        properties.put("maxWait","10000");//maximum wait milliseconds for get connection from pool
        properties.put("minIdle","3");//minimum number of connection in the pool
        properties.put("timeBetweenEvictionRunsMillis","3000");// the interval milliseconds to test connection
        properties.put("minEvictableIdleTimeMillis","60000");//the minimum milliseconds to keep idle
        properties.put("maxEvictableIdleTimeMillis","90000");//the maximum milliseconds to keep idle
        properties.put("validationQuery","describe log.dn"); //validation query
        properties.put("testWhileIdle","true"); // test connection while idle
        properties.put("testOnBorrow","false"); // don't need while testWhileIdle is true
        properties.put("testOnReturn","false"); // don't need while testWhileIdle is true

        return properties;
    }
}
