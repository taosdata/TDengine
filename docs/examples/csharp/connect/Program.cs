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
    internal class ConnectExample
    {
        // ANCHOR: main
        static void Main(String[] args)
        {
            var connectionString = "host=127.0.0.1;port=6030;username=root;password=taosdata";
            try
            {
                // Connect to TDengine server using Native
                var builder = new ConnectionStringBuilder(connectionString);
                // Open connection with using block, it will close the connection automatically
                using (var client = DbDriver.Open(builder))
                {
                    Console.WriteLine("Connected to " + connectionString + " successfully.");
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to connect to " + connectionString + "; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to connect to " + connectionString + "; Err:" + e.Message);
                throw;
            }
        }
        // ANCHOR_END: main
    }
}