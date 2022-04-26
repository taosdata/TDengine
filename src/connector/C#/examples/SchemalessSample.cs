/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Collections;
using Sample.UtilsTools;
using TDengineDriver;

namespace Example
{
    class SchemalessSample
    {

        private IntPtr conn = IntPtr.Zero;
        private string dbName = "csharp_schemaless_example";
        public void RunSchemaless()
        {
            string[] lines = {
                "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
            };
            string[] jsonStr = {
                "{"
                   +"\"metric\": \"stb0_0\","
                   +"\"timestamp\": 1626006833,"
                   +"\"value\": 10,"
                   +"\"tags\": {"
                       +" \"t1\": true,"
                       +"\"t2\": false,"
                       +"\"t3\": 10,"
                       +"\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
                    +"}"
                +"}"
            };
            StringBuilder querySql = new StringBuilder();
            Console.WriteLine(querySql.ToString());
            this.conn = UtilsTools.TDConnection(this.dbName);

            schemalessInsert(lines, 2, (int)TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS);
            querySql.Append("select * from ").Append(this.dbName).Append(".").Append("stg");
            UtilsTools.DisplayRes(UtilsTools.ExecuteQuery(this.conn, querySql.ToString()));

            schemalessInsert(jsonStr, 1, (int)TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_SECONDS);
            querySql.Clear();
            querySql.Append("select * from ").Append(this.dbName).Append(".").Append("stb0_0");
            UtilsTools.DisplayRes(UtilsTools.ExecuteQuery(this.conn, querySql.ToString()));

            querySql.Clear();
            querySql.Append("drop database if exists ").Append(this.dbName);
            UtilsTools.ExecuteUpdate(this.conn, querySql.ToString());
            UtilsTools.CloseConnection(this.conn);

        }
        public void schemalessInsert(string[] sqlstr, int lineCnt, int protocol, int precision)
        {

            IntPtr res = TDengine.SchemalessInsert(this.conn, sqlstr, lineCnt, protocol, precision);

            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("schemaless_insert failed:{0}", TDengine.Error(res));
                Console.WriteLine("line string:{0}", sqlstr);
                Console.WriteLine("");
                System.Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("else");
                Console.WriteLine("schemaless insert success:{0}", TDengine.ErrorNo(res));
            }

        }

    }
}
