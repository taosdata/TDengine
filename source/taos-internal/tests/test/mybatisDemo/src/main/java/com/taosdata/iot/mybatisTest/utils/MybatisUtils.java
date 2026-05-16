package com.taosdata.iot.mybatisTest.utils;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;

/**
 * @author Jiangyi Hou
 * @since 18-11-9
 */

public class MybatisUtils {
    private static SqlSessionFactory sqlSessionFactory;
    public static SqlSession openSession() {
        SqlSession sqlSession = null;
        if (sqlSessionFactory == null) {
            try {
                String resource = "mybatis-config.xml";
                InputStream inputStream = Resources.getResourceAsStream(resource);
                sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sqlSession = sqlSessionFactory.openSession();
        return sqlSession;
    }

    public static SqlSession openSession(ExecutorType executorType) {
        SqlSession sqlSession = null;
        if (sqlSessionFactory == null) {
            try {
                String resource = "mybatis-config.xml";
                InputStream inputStream = Resources.getResourceAsStream(resource);
                sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sqlSession = sqlSessionFactory.openSession(executorType);
        return sqlSession;
    }
}
