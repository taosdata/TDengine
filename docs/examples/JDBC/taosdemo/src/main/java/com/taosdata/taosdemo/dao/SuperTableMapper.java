package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SuperTableMeta;
import org.springframework.stereotype.Repository;

@Repository
public interface SuperTableMapper {

    // Create super table: create table if not exists xxx.xxx (f1 type1, f2 type2,
    // ... ) tags( t1 type1, t2 type2 ...)
    void createSuperTable(SuperTableMeta tableMetadata);

    // Drop super table: drop table if exists xxx;
    void dropSuperTable(String database, String name);

    // <!-- TODO: Query all super table information show stables -->

    // <!-- TODO: Query table structure describe stable -->

    // <!-- TODO: Add column alter table ${tablename} add column fieldName dataType
    // -->

    // <!-- TODO: Drop column alter table ${tablename} drop column fieldName -->

    // <!-- TODO: Add tag alter table ${tablename} add tag new_tagName tag_type -->

    // <!-- TODO: Drop tag alter table ${tablename} drop tag_name -->

    // <!-- TODO: Change tag name alter table ${tablename} change tag old_tagName
    // new_tagName -->
}
