using System;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using TDengineDriver;
using Example;
using System.Collections.Generic;

namespace AsyncQueryExample
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            IntPtr conn = UtilsTools.TDConnection();

            AsyncQuerySample asyncQuery = new AsyncQuerySample();
            asyncQuery.RunQueryAsync(conn,"query_async");


            UtilsTools.CloseConnection(conn);
        }
    }
}
