from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableAggregate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_aggregate(self):
        """Normal table aggregate

        1. Create a table with 256 columns
        2. Insert data
        3. Execute projection queries
        4. Execute filter queries
        5. Execute aggregate queries
        6. Kill the process and restart the database

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/column/table.sim

        """

        tdLog.info(f"=============== step1")
        tdSql.prepare("d1", drop=True)
        tdSql.execute(f"use d1")
        tdSql.execute(
            f"create table d1.t1 (ts timestamp, c000 int, c001 int, c002 int, c003 int, c004 int, c005 int, c006 int, c007 int, c008 int, c009 int, c010 int, c011 int, c012 int, c013 int, c014 int, c015 int, c016 int, c017 int, c018 int, c019 int, c020 int, c021 int, c022 int, c023 int, c024 int, c025 int, c026 int, c027 int, c028 int, c029 int, c030 int, c031 int, c032 int, c033 int, c034 int, c035 int, c036 int, c037 int, c038 int, c039 int, c040 int, c041 int, c042 int, c043 int, c044 int, c045 int, c046 int, c047 int, c048 int, c049 int, c050 int, c051 int, c052 int, c053 int, c054 int, c055 int, c056 int, c057 int, c058 int, c059 int, c060 int, c061 int, c062 int, c063 int, c064 int, c065 int, c066 int, c067 int, c068 int, c069 int, c070 int, c071 int, c072 int, c073 int, c074 int, c075 int, c076 int, c077 int, c078 int, c079 int, c080 int, c081 int, c082 int, c083 int, c084 int, c085 int, c086 int, c087 int, c088 int, c089 int, c090 int, c091 int, c092 int, c093 int, c094 int, c095 int, c096 int, c097 int, c098 int, c099 int, c100 int, c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int, c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int, c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int, c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int, c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int, c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int, c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int, c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int, c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int, c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int, c201 int, c202 int, c203 int, c204 int, c205 int, c206 int, c207 int, c208 int, c209 int, c210 int, c211 int, c212 int, c213 int, c214 int, c215 int, c216 int, c217 int, c218 int, c219 int, c220 int, c221 int, c222 int, c223 int, c224 int, c225 int, c226 int, c227 int, c228 int, c229 int, c230 int, c231 int, c232 int, c233 int, c234 int, c235 int, c236 int, c237 int, c238 int, c239 int, c240 int, c241 int, c242 int, c243 int, c244 int, c245 int, c246 int, c247 int, c248 int, c249 int, c250 int)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into d1.t1 values (now,0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+1m,1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+2m,2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+3m,3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+4m,4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 )"
        )
        tdSql.execute(
            f"insert into d1.t1 values (now+5m,5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+6m,6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+7m,7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+8m,8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 )"
        )

        tdSql.execute(
            f"insert into d1.t1 values (now+9m,9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 )"
        )

        tdLog.info(f"======= step3")
        tdSql.query(f"select * from d1.t1")
        tdLog.info(f"select * from d1.t1 => rows {tdSql.getRows()}")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d1.t1 where ts < now + 4m")
        tdLog.info(f"select * from d1.t1 where ts < now + 4m => rows {tdSql.getRows()}")
        tdSql.checkRows(5)

        tdSql.query(f"select * from d1.t1 where c001 = 1")
        tdLog.info(f"select * from d1.t1 where c001 = 1 => rows {tdSql.getRows()}")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d1.t1 where c002 = 2 and c003 = 2")
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 and ts < now + 4m"
        )
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 and ts < now + 4m => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from d1.t1")
        tdLog.info(f"select count(*) from d1.t1 => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d1.t1"
        )

        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"==============  step5")

        tdSql.query(f"select * from d1.t1")
        tdLog.info(f"select * from d1.t1 => rows {tdSql.getRows()}")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d1.t1 where c001 = 1")
        tdLog.info(f"select * from d1.t1 where c001 = 1 => rows {tdSql.getRows()}")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d1.t1 where c002 = 2 and c003 = 2")
        tdLog.info(
            f"select * from d1.t1 where c002 = 2 and c003 = 2 => rows {tdSql.getRows()}"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from d1.t1")
        tdLog.info(f"select count(*) from d1.t1 => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)

        tdSql.query(
            f"select count(c001), count(c250), avg(c001), avg(c250), sum(c001), max(c001), min(c250), stddev(c250), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d1.t1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 2.872281323)
