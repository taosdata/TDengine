package com.taosdata.iot.springbootdemo.dao;

import com.google.common.base.Strings;
import com.taosdata.iot.springbootdemo.entity.Dev;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * @author Jiangyi Hou
 * @since 18-12-13
 */

@Repository
public class DevDaoImpl implements DevDao {
    @Autowired
    private DataSource dataSource;

    public Dev findDevByTs(String ts) {
        Dev dev = new Dev();
        String sql = "select * from db.dev where ts = '" + ts + "'";
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement stmt = connection.createStatement();
            ResultSet resSet = stmt.executeQuery(sql);
            ResultSetMetaData metaData = resSet.getMetaData();
            if (resSet.next()) {
                dev.setTs(resSet.getTimestamp(1));
                dev.setC1(resSet.getInt(2));
            }
            resSet.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }
        return dev;
    }

    public void displayAllDevs() {

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement stmt = connection.createStatement();
            ResultSet resSet = stmt.executeQuery("select * from db.tb");
            ResultSetMetaData metaData = resSet.getMetaData();

            String display = "";
            int cellLength = 0;
            while (resSet.next()) {
                StringBuffer strBuff = new StringBuffer();
                for (int col = 1; col <= metaData.getColumnCount(); col++) {
                    display = String.valueOf(resSet.getObject(col));
                    cellLength = metaData.getColumnDisplaySize(col);
                    if ("TIMESTAMP".equalsIgnoreCase(metaData.getColumnTypeName(col))) {
//                        display = sdf.format(new Timestamp(Long.valueOf(display)));
                        cellLength = 24;
                    }
                    System.out.printf("%s|", Strings.padEnd(display,
                            cellLength, ' '));
                }
                System.out.printf("\n");
            }
            resSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
