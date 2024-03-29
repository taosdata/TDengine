#! encoding = utf-8
import taos

LOCATIONS = ['California.SanFrancisco', 'California.LosAngles', 'California.SanDiego', 'California.SanJose',
             'California.PaloAlto', 'California.Campbell', 'California.MountainView', 'California.Sunnyvale',
             'California.SantaClara', 'California.Cupertino']

CREATE_DATABASE_SQL = 'create database if not exists {} keep 36500 duration 10 buffer 16 wal_level 1 wal_retention_period 3600'
USE_DATABASE_SQL = 'use {}'
DROP_TABLE_SQL = 'drop table if exists meters'
DROP_DATABASE_SQL = 'drop database if exists {}'
CREATE_STABLE_SQL = 'create stable meters (ts timestamp, current float, voltage int, phase float) tags ' \
                    '(location binary(64), groupId int)'
CREATE_TABLE_SQL = 'create table if not exists {} using meters tags (\'{}\', {})'


def create_database_and_tables(host, port, user, password, db, table_count):
    tags_tables = _init_tags_table_names(table_count=table_count)
    conn = taos.connect(host=host, port=port, user=user, password=password)

    conn.execute(DROP_DATABASE_SQL.format(db))
    conn.execute(CREATE_DATABASE_SQL.format(db))
    conn.execute(USE_DATABASE_SQL.format(db))
    conn.execute(DROP_TABLE_SQL)
    conn.execute(CREATE_STABLE_SQL)
    for tags in tags_tables:
        location, group_id = _get_location_and_group(tags)
        tables = tags_tables[tags]
        for table_name in tables:
            conn.execute(CREATE_TABLE_SQL.format(table_name, location, group_id))
    conn.close()


def clean(host, port, user, password, db):
    conn = taos.connect(host=host, port=port, user=user, password=password)
    conn.execute(DROP_DATABASE_SQL.format(db))
    conn.close()


def _init_tags_table_names(table_count):
    tags_table_names = {}
    group_id = 0
    for i in range(table_count):
        table_name = 'd{}'.format(i)
        location_idx = i % len(LOCATIONS)
        location = LOCATIONS[location_idx]
        if location_idx == 0:
            group_id += 1
            if group_id > 10:
                group_id -= 10
        key = _tag_table_mapping_key(location=location, group_id=group_id)
        if key not in tags_table_names:
            tags_table_names[key] = []
        tags_table_names[key].append(table_name)

    return tags_table_names


def _tag_table_mapping_key(location, group_id):
    return '{}_{}'.format(location, group_id)


def _get_location_and_group(key):
    fields = key.split('_')
    return fields[0], fields[1]
