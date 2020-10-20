package com.hivemq.extensions.taosdata.configuration;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * 配置类，包括消息主题及写表项
 */
public class Config {
    private static final String INSERT_SQL_FORMAT = "INSERT INTO %s (%s) VALUES (%s);";

    private List<Topic> topics;
    private Map<String, Topic> map;

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
        map = new HashMap<>();
        if (topics != null) {
            topics.forEach(t -> {
                // 设置默认表名，替换 "/" => "_"
                if (Strings.isNullOrEmpty(t.getTable())) {
                    t.setTable(t.getId().replaceAll("/", "_"));
                }

                // 生成 INSERT SQL
                StringJoiner sjf = new StringJoiner(",");
                StringJoiner sjq = new StringJoiner(",");
                t.getFields().forEach(f -> {
                    sjf.add(f);
                    sjq.add("?");
                });

                if (Strings.isNullOrEmpty(t.getInsertSql())) {
                    String sql = String.format(INSERT_SQL_FORMAT,
                            t.getTable(),
                            sjf.toString(),
                            sjq.toString());
                    t.setInsertSql(sql);
                }

                map.put(t.getId(), t);
            });
        }
    }

    public boolean contains(String topic) {
        if (map != null) {
            return map.containsKey(topic);
        }

        return false;
    }

    public Topic get(String topic) {
        if (map != null) {
            return map.get(topic);
        }

        return null;
    }
}
