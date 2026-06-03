from new_test_framework.utils import tdLog, tdSql


class TestManyUnion:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _tag_value(self, tag_idx: int, table_idx: int) -> str:
        # Keep tag value length within binary(10).
        return f"{tag_idx:02d}{table_idx:08d}"

    def _build_insert_values(self, start_ts: int, row_count: int) -> str:
        values = []
        for row_idx in range(row_count):
            ts = start_ts + row_idx * 1000
            c1 = row_idx
            c2 = row_idx + 0.5
            c3 = row_idx * 1.25
            values.append(f"({ts}, {c1}, {c2}, {c3})")
        return ",".join(values)

    def test_many_union(self):
        """many union with many subtables

        1. create one stable with >=10 binary(10) tags and 3 numeric columns
        2. create 20000 subtables with different tag values
        3. insert 100 rows into each subtable
        4. build one union query with 50 select statements filtering unique subtable by tags
        5. execute the union query

        Catalog:
            - Query:Union

        Since: v3.3.6.0

        Labels: common,ci

        Jira: Feishu-7001864385

        History:
            - 2024-06-10, Bomin Zhang created

        """

        db = "many_union_db"
        stable = "st_many_union"
        table_count = 20000
        row_count = 100
        union_select_count = 50

        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")

        tag_defs = ", ".join([f"t{i} binary(10)" for i in range(1, 11)])
        tdSql.execute(
            f"create table if not exists {stable} (ts timestamp, c1 int, c2 float, c3 double) tags ({tag_defs})"
        )

        tdLog.info(f"start creating {table_count} subtables and inserting {row_count} rows each")
        base_ts = 1704067200000

        for tb_idx in range(table_count):
            tb_name = f"d_{tb_idx:05d}"
            tag_values = ", ".join(
                [f"'{self._tag_value(tag_idx, tb_idx)}'" for tag_idx in range(1, 11)]
            )
            tdSql.execute(
                f"create table {tb_name} using {stable} tags ({tag_values})"
            )

            start_ts = base_ts + tb_idx * row_count * 1000
            values_sql = self._build_insert_values(start_ts, row_count)
            tdSql.execute(f"insert into {tb_name} values {values_sql}")

            if (tb_idx + 1) % 1000 == 0:
                tdLog.info(f"created and inserted {(tb_idx + 1)} subtables")

        tdLog.info(f"start building {union_select_count}-way union sql")
        selected_table_indexes = [((idx + 1) * 577) % table_count for idx in range(union_select_count)]
        filter_tag_indexes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        union_selects = []
        for tb_idx in selected_table_indexes:
            conditions = []
            for tag_idx in filter_tag_indexes:
                conditions.append(f"t{tag_idx} = '{self._tag_value(tag_idx, tb_idx)}'")

            where_clause = " and ".join(conditions)
            union_selects.append(
                f"select * from {stable} where {where_clause}"
            )

        union_sql = " union ".join(union_selects)

        tdLog.info(f"execute {union_select_count}-way union sql")
        tdSql.query(union_sql)
        tdSql.checkRows(union_select_count * row_count)
