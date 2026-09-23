// Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class OptsJsonExample
    {
        // ANCHOR: main
        public static void Main(string[] args)
        {
            var host = "127.0.0.1";

            var lineDemo =
                "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";

            var telnetDemo = "metric_telnet 1707095283260 4 host=host0 interface=eth0";

            var jsonDemo =
                "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";
            try
            {
                var builder =
                    new ConnectionStringBuilder(
                        $"protocol=WebSocket;host={host};port=6041;username=root;password=taosdata");
                using (var client = DbDriver.Open(builder))
                {
                    // create database
                    client.Exec("CREATE DATABASE IF NOT EXISTS power");
                    // use database
                    client.Exec("USE power");
                    // insert influx line protocol data
                    client.SchemalessInsert(new[]{lineDemo}, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
                    // insert opentsdb telnet protocol data
                    client.SchemalessInsert(new[]{telnetDemo}, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
                    // insert json data
                    client.SchemalessInsert(new []{jsonDemo}, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }

                Console.WriteLine("Inserted data with schemaless successfully.");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to insert data with schemaless, host:" + host + "; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to insert data with schemaless, host:" + host + "; ErrMessage: " + e.Message);
                throw;
            }
        }
        // ANCHOR_END: main
    }
}