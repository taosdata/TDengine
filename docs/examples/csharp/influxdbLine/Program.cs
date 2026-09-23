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
    internal class InfluxDBLineExample
    {
        static void Main()
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                client.Exec("CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
                client.Exec("use test");
                string[] lines = {
                    "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                    "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                    "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                    "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"
                };
                client.SchemalessInsert(lines,
                    TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            }
        }
    }

}
