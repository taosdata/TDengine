package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SuperTableMeta;
import org.springframework.stereotype.Repository;

@Repository
public interface SuperTableMapper {

    // 创建超级表 create table if not exists xxx.xxx (f1 type1, f2 type2, ... ) tags( t1 type1, t2 type2 ...)
    void createSuperTable(SuperTableMeta tableMetadata);

    // 删除超级表 drop table if exists xxx;
    void dropSuperTable(String database, String name);

    //<!-- TODO:查询所有超级表信息 show stables -->

    //<!-- TODO:查询表结构 describe stable -->

    //<!-- TODO:增加列 alter table ${tablename} add column fieldName dataType -->

    //<!-- TODO:删除列 alter table ${tablename} drop column fieldName -->

    //<!-- TODO:添加标签 alter table ${tablename} add tag new_tagName tag_type -->

    //<!-- TODO:删除标签 alter table ${tablename} drop tag_name -->

    //<!-- TODO:修改标签名 alter table ${tablename} change tag old_tagName new_tagName -->

}
