using System;
using System.Linq;
using System.Windows.Forms;
using Newtonsoft.Json.Linq;

using Excel = Microsoft.Office.Interop.Excel;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using System.Threading.Tasks;

namespace TDengineExcelPlugins
{
    public class TDUtil
    {
        public String ErrorPrefix = "TDengine Error: ";
        public String ExceptionPrefix = "TDengine Exception: ";
        public String InfoPrefix = "TDengine Info: ";
        public String SqlPrefix = "TDengine Sql: ";

        private void Show(String msg, String type, MessageBoxIcon icon)
        {
            MessageBoxButtons buttons = MessageBoxButtons.OK;
            DialogResult result = MessageBox.Show(msg + "\n\nClick Confirm button to copy to clipboard", type, buttons, icon);
            if (result == System.Windows.Forms.DialogResult.OK)
            {
                Clipboard.SetText(msg);
            }
        }
        public void ShowError(String error)
        {
            this.Show(this.ErrorPrefix + error, "Error", MessageBoxIcon.Error);
        }

        public void ShowInfo(String info)
        {
            this.Show(this.InfoPrefix + info, "Information", MessageBoxIcon.Information);
        }

        public void ShowException(Exception e)
        {
            this.Show(this.ExceptionPrefix + e.Message, "Error", MessageBoxIcon.Error);
        }

        public String GetLoginUrl()
        {
            String url = TDFactory.Persist.connectURL + "/rest/login/" + TDFactory.Persist.connectUSER + "/" + TDFactory.Persist.connectPASS;
            return url;
        }

        public String GetSqlUrl(TDHttpTimestampType timestampType)
        {
            if (timestampType == TDHttpTimestampType.TD_SHOW_TIMESTSAMP)
            {
                return TDFactory.Persist.connectURL + "/rest/sqlt";
            }
            else
            {
                return TDFactory.Persist.connectURL + "/rest/sql";
            }
        }
        
        public bool IsMetricsName(String mtName)
        {
            mtName = mtName.ToLower();
            String sql = "show " + TDFactory.Persist.connectDB + ".stables like '" + mtName.ToLower() + "'";
            TDHttpReturn resp = TDFactory.Http.Request(sql, TDHttpTimestampType.TD_SHOW_TIME_STRING);
            JObject jo = resp.jo;
            if (jo == null)
            {
                return false;
            }

            Array heads = jo.GetValue("head").ToArray();
            Array datas = jo.GetValue("data").ToArray();
            int headLength = heads.GetLength(0);
            int dataLength = datas.GetLength(0);

            for (int row = 0; row < dataLength; ++row)
            {
                Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                String stableName = dataCols.GetValue(0).ToString();

                if (stableName == mtName)
                {
                    return true;
                }
            }

            return false;
        }

        public bool IsTablesName(String tbname)
        {
            tbname = tbname.ToLower();
            String sql = "show " + TDFactory.Persist.connectDB + ".tables like '" + tbname.ToLower() + "'";
            TDHttpReturn resp = TDFactory.Http.Request(sql, TDHttpTimestampType.TD_SHOW_TIME_STRING);
            JObject jo = resp.jo;
            if (jo == null)
            {
                return false;
            }

            Array heads = jo.GetValue("head").ToArray();
            Array datas = jo.GetValue("data").ToArray();
            int headLength = heads.GetLength(0);
            int dataLength = datas.GetLength(0);

            for (int row = 0; row < dataLength; ++row)
            {
                Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                String res = dataCols.GetValue(0).ToString();

                if (res == tbname)
                {
                    return true;
                }
            }

            return false;
        }

        public String GetMetricsNameOfTable(String tablename)
        {
            String sql = "show " + TDFactory.Persist.connectDB + ".tables like '" + tablename.ToLower() + "'";
            TDHttpReturn resp = TDFactory.Http.Request(sql, TDHttpTimestampType.TD_SHOW_TIME_STRING);
            JObject jo = resp.jo;
            if (jo == null)
            {
                return String.Empty;
            }

            Array heads = jo.GetValue("head").ToArray();
            Array datas = jo.GetValue("data").ToArray();
            int headLength = heads.GetLength(0);
            int dataLength = datas.GetLength(0);

            if (dataLength > 0)
            {
                Array dataCols = (datas.GetValue(0) as JToken).ToArray();
                String stableName = dataCols.GetValue(3).ToString();
                return stableName.ToLower();
            }
            else
            {
                return TD_TABLE_NOT_EXIST;
            }
        }

        public String[] aggFunctions = { "count", "sum", "avg", "twa", "max", "min", "first", "last", "spread", "stddev" };
        public String[] aggFunctionsMustGroupByTbname = { "twa" };
        public String[] aggFunctionsCannotUsedToMetrics = { "stddev" };
        public String[] intervalTimeUnits = { "seconds", "minutes", "hours", "days", "weeks", "months", "years" };
        public String[] intervalTimeUnitTypes = { "s", "m", "h", "d", "w", "n", "y" };
        public String[] fillMethods = { "none", "null", "value", "prev"};

        public String TD_TABLE_EMPTY_SELECTION = "empty selection";
        public String TD_TOO_MANY_FIELDS_SELECT = "too many select fields";
        public String TD_INVALID_AGG_FUNCTIONS = "invalid functions";
        public String TD_TABLE_NOT_EXIST = "table not exist";
        public String TD_TABLE_NAME_NOT_INPUT = "table name not input";
        public String TD_TABLE_NOT_EXIST_SURFIX = " not exist";
        public String TD_TABLE_NOT_FROM_ANY_STABLE_SURFIX = " not from any stable";
        public String TD_IS_TABLE_ONLY_SUPPORT_STABLE_SURFIX = " is stable, only support table in multi-query";

        public String TD_INVALID_RESPONSE_FROM_SERVER = "invalid response from server";
        public String TD_TABLE_DESCRIBE_FAILED = "describe table failed";
        public String TD_OUTPUT_NOT_SELECT = "output columns not select";

        public String TD_FUNC_CANNOT_USED_TO_METRICS_SURFIX = " function can't be used to stabe";
        public String TD_FUNC_MUST_GROUP_BY_TBNAME_SURFIX = " function can only group by tbname";
        public String TD_SQL_TOO_LONG_PREFIX = "sql too long: ";
        public String TD_SQL_FIRST_COL_ONLY_SUPPROT_COUNT = "first column only support count function";
        public String TD_SQL_BINARY_COL_ONLY_SUPPROT_COUNT = "binary column only support count function";
        public String TD_DATA_FROM_SERVER_EMPTY = "rows of response is 0";
        public String TD_TOO_MANY_TABLES = "only support one table";
        public String TD_ASCENDING_ONLY_SUPPORT_TABLE = "stable not support ascending operation, use descending";

        public String TD_NO_CELL_INPUT = "no cell input";
        public String TD_INVALID_CELL_ADDRESS_SHOULD_ONLY_ONE = "cell address should contain only one cell";
        public String TD_INVALID_CELL_ADDRESS = "invalid cell address";
        public String TD_NO_TABLE_INPUT = "no table input";
        public String TD_INVALID_TABLE_ADDRESS_SHOULD_ONLY_ONE = "table address should contain only one cell";
        public String TD_NO_BEGINTIMESTAMP_INPUT = "no begin timestamp input";
        public String TD_INVALID_BEGIN_TS_ADDRESS_SHOULD_ONLY_ONE = "begin timestamp should contain only one cell";
        public String TD_NO_ENDTIMESTAMP_INPUT = "no end timestamp input";
        public String TD_INVALID_END_TS_ADDRESS_SHOULD_ONLY_ONE = "end timestamp should contain only one cell";
        public String TD_NO_TIMESTAMP_INPUT = "no timestamp input";
        public String TD_INVALID_TS_ADDRESS_SHOULD_ONLY_ONE = "timestamp should contain only one cell";
        public String TD_NO_FUNCTIONS_SELECTED = "no function selected";
        public String TD_NO_INTERPOLATION_SELECTED = "no interpolation method selected";
        public String TD_NO_FIELDS_SELECT = "no fields selected";
        public String TD_FUNCTION_CAN_NOT_CALCULATE = "function can not be calculated";


        public String TD_SELECT_AGG_FUNCTIONS_COLUMN = "Selected Aggregation Names";
        public String TD_FIELD_NAME_COLUMN = "Field Name";
        public String TD_FIELD_TYPE_COLUMN = "Field Type";
        public String TD_SELECT_FIELD_COLUMN = "Selected Field";

        public int TD_MAX_SQL_COUNT = 900;

        public async Task<TDTable> ExplainTables(String tableNames)
        {
            TDTable table = new TDTable();

            if (tableNames == String.Empty)
            {
                table.error = TD_TABLE_NAME_NOT_INPUT;
                return table;
            }
            
            tableNames = tableNames.Replace("\r", String.Empty);
            tableNames = tableNames.Replace("\n", String.Empty);
            table.tables = tableNames.Split(',');
            if (table.tables == null || table.tables.Length == 0)
            {
                table.error = TD_TABLE_NAME_NOT_INPUT;
                return table;
            }

            table.table = table.tables[0];
            if (table.tables.Length == 1)
            {
                bool isStable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsMetricsName(table.table); });
                if (isStable)
                {
                    table.stable = table.table;
                    return table;
                }

                bool isTable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsTablesName(table.table); });
                if (isTable)
                {
                    return table;
                }
                else
                {
                    table.error = table.table + TD_TABLE_NOT_EXIST_SURFIX;
                    return table;
                }
            }
            else
            {
                bool isStable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsMetricsName(table.table); });
                if (isStable)
                {
                    table.error = table.table + TD_IS_TABLE_ONLY_SUPPORT_STABLE_SURFIX;
                    return table;
                }
                
                table.stable = await Task.Factory.StartNew<String>(() => { return TDFactory.Util.GetMetricsNameOfTable(table.table); });
                if (table.stable == TD_TABLE_NOT_EXIST)
                {
                    table.error = table.table + TD_TABLE_NOT_EXIST_SURFIX;
                    return table;
                }
                else if (table.stable == String.Empty)
                {
                    table.error = table.table + TD_TABLE_NOT_FROM_ANY_STABLE_SURFIX;
                    return table;
                }
                else { }
            }

            return table;
        }

        public String GenerateDescribeSql(TDTable table)
        {
            if (table.stable != String.Empty)
            {
                return "describe " + TDFactory.Persist.connectDB + "." + table.stable;
            }
            else
            {
                return "describe " + TDFactory.Persist.connectDB + "." + table.table;
            }
        }

        public async Task<TDSTable> ExplainSTables(String tableNames)
        {
            TDSTable stable = new TDSTable();

            if (tableNames == String.Empty)
            {
                return stable;
            }

            tableNames = tableNames.Replace("\r", String.Empty);
            tableNames = tableNames.Replace("\n", String.Empty);
            String[] tables = tableNames.Split(',');
            if (tables.Length == 0)
            {
                return stable;
            }
            if (tables.Length > 1)
            {
                stable.error = TD_TOO_MANY_TABLES;
                return stable;
            }

            String table = tables[0];
            bool isStable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsMetricsName(table); });

            if (isStable)
            {
                stable.stable = table;
                return stable;
            }
            
            stable.stable = await Task.Factory.StartNew<String>(() => { return TDFactory.Util.GetMetricsNameOfTable(table); });
            if (stable.stable == TD_TABLE_NOT_EXIST)
            {
                stable.error = table + TD_TABLE_NOT_EXIST_SURFIX;
                return stable;
            }
            else if (stable.stable == String.Empty)
            {
                stable.error = table + TD_TABLE_NOT_FROM_ANY_STABLE_SURFIX;
                return stable;
            }

            return stable;
        }

        public async Task<TDSingleTable> ExplainSingleTable(String tableNames)
        {
            TDSingleTable table = new TDSingleTable();

            if (tableNames == String.Empty)
            {
                table.error = TD_TABLE_NAME_NOT_INPUT;
                return table;
            }

            tableNames = tableNames.Replace("\r", String.Empty);
            tableNames = tableNames.Replace("\n", String.Empty);
            String[] tables = tableNames.Split(',');
            if (tables.Length == 0)
            {
                table.error = TD_TABLE_NAME_NOT_INPUT;
                return table;
            }
            if (tables.Length > 1)
            {
                table.error = TD_TOO_MANY_TABLES;
                return table;
            }

            table.table = tables[0];
            table.isStable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsMetricsName(table.table); });

            if (table.isStable)
            {
                return table;
            }

            bool isTable = await Task.Factory.StartNew<bool>(() => { return TDFactory.Util.IsTablesName(table.table); });
            if (!isTable)
            {
                table.error = table.table + TD_TABLE_NOT_EXIST_SURFIX;
                return table;
            }
           
            return table;
        }

        public async Task<TDHttpReturn> DoRequest(String sql, TDHttpTimestampType timestampType)
        {
            return await Task.Factory.StartNew<TDHttpReturn>(() => TDFactory.Http.Request(sql, timestampType));
        }
    }


    public class TDTable
    {
        public String stable = String.Empty;
        public String table = String.Empty;
        public String[] tables = null;
        public String error = String.Empty;
    }

    public class TDSTable
    {
        public String stable = String.Empty;
        public String error = String.Empty;
    }

    public class TDSingleTable
    {
        public bool isStable = false;
        public String table = String.Empty;
        public String error = String.Empty;
    }

}
