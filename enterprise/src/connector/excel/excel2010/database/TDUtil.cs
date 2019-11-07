using System;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace excel2010
{
    public class TDUtil
    {
        public void ShowError(String error)
        {
            MessageBox.Show("TDengine Error: " + error, "Error");
        }

        public void ShowInfo(String info)
        {
            MessageBox.Show("TDengine Info: " + info, "Information");
        }

        public void ShowException(Exception e)
        {
            MessageBox.Show("TDengine Exception: " + e.Message, "Error");
        }

        public String GetLoginUrl()
        {
            String url = Globals.ThisAddIn.tdPersist.URL + "/rest/login/" + Globals.ThisAddIn.tdPersist.USER + "/" + Globals.ThisAddIn.tdPersist.PASS;
            return url;
        }

        public String GetSqlUrl(bool displayTimestamp)
        {
            if (displayTimestamp)
            {
                return Globals.ThisAddIn.tdPersist.URL + "/rest/sqlt";
            }
            else
            {
                return Globals.ThisAddIn.tdPersist.URL + "/rest/sql";
            }
        }

        public int[] GetUnHiddenColumns(int beginColumn, int columnLength)
        {
            Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;

            int[] columnArray = new int[columnLength];
            int col = 0, columnArrayIndex = 0;

            while (columnArrayIndex < columnLength)
            {
                if (!activeWorksheet.Cells[1, col + beginColumn].EntireColumn.Hidden)
                {
                    columnArray[columnArrayIndex++] = col + beginColumn;
                }
                col++;
            }

            return columnArray;
        }

        public int[] GetUnHiddenRows(int beginRow, int rowLength)
        {
            Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;

            int[] rowArray = new int[rowLength];
            int row = 0, rowArrayIndex = 0;

            while (rowArrayIndex < rowLength)
            {
                if (!activeWorksheet.Cells[row + beginRow, 1].EntireRow.Hidden)
                {
                    rowArray[rowArrayIndex++] = row + beginRow;
                }
                row++;
            }

            return rowArray;
        }

        public String SelectRangeByString(String rangeAddress, TDFormSelectType selectType)
        {
            try
            {
                Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;
                Range target = (Excel.Range)activeWorksheet.Range[rangeAddress];
                if (target != null)
                {
                    if (selectType == TDFormSelectType.TD_FORM_SELECT_CELL)
                    {
                        int beginRow = target.Row;
                        int beginCol = target.Column;
                        Range selectedTargt = activeWorksheet.Cells[beginRow, beginCol];
                        selectedTargt.Select();
                        return selectedTargt.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
                    }
                    else if (selectType == TDFormSelectType.TD_FORM_SELECT_ROW)
                    {
                        Globals.ThisAddIn.tdUtil.ShowError("select row");
                    }
                    else
                    {
                        Globals.ThisAddIn.tdUtil.ShowError("invalid select type");
                    }

                }
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }

            return "invalid range";
        }

        public Range GetRange(string rangeAddress)
        {
            try
            {
                Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;
                Range target = (Excel.Range)activeWorksheet.Range[rangeAddress];
                return target;
            }
            catch (Exception)
            {
            }
            finally { }

            return null;
        }

        public String GetRangeFirstValue(Range range)
        {
            try
            {
                Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;

                int beginRow = range.Row;
                int beginCol = range.Column;

                return activeWorksheet.Cells[beginRow, beginCol].Value2;
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }

            return "";
        }

        public String GetRangeValuesIncludeHidden(Range range)
        {
            try
            {
                if (range == null)
                {
                    return "";
                }

                if (range.Value2 == null)
                {
                    return "";
                }

                if (range.Value2.GetType() == typeof(Object[,]))
                {
                    String ret = "";
                    bool firstRow = true;
                    Object[,] rangeArray = range.Value2 as Object[,];
                    foreach (Object rangeValue in rangeArray)
                    {
                        if (rangeValue == null)
                        {
                            continue;
                        }

                        if (firstRow)
                        {
                            firstRow = false;
                            ret = rangeValue.ToString();
                        }
                        else
                        {
                            ret = ret + "," + rangeValue.ToString();
                        }
                    }

                    return ret;
                }
                else
                {
                    return range.Value2.ToString;
                }
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }

            return "";
        }

        public String GetRangeValues(Range ranges)
        {
            try
            {
                if (ranges == null)
                {
                    return "";
                }

                bool firstRow = true;
                String ret = "";
                foreach (Range range in ranges)
                {
                    if (range == null)
                    {
                        continue;
                    }

                    if (range.Value2 == null)
                    {
                        continue;
                    }

                    if (range.EntireRow.Hidden || range.EntireColumn.Hidden)
                    {
                        continue;
                    }

                    if (firstRow)
                    {
                        firstRow = false;
                        ret = range.Value2.ToString();
                    }
                    else
                    {
                        ret = ret + "," + range.Value2.ToString();
                    }
                }

                return ret;
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }

            return "";
        }

        public bool IsMetricsName(String mtName)
        {
            mtName = mtName.ToLower();
            String sql = "show " + Globals.ThisAddIn.tdPersist.DB + ".stables like '" + mtName.ToLower() + "'";
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
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
            String sql = "show " + Globals.ThisAddIn.tdPersist.DB + ".tables like '" + tbname.ToLower() + "'";
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
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
            String sql = "show " + Globals.ThisAddIn.tdPersist.DB + ".tables like '" + tablename.ToLower() + "'";
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
            if (jo == null)
            {
                return "";
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

            return "";
        }

        public String[] aggFunctions = { "count", "avg", "twa", "max", "min", "first", "last", "spread", "stddev", "   >>  " };
        public String[] intervalTimeUnits = { "seconds", "minutes", "hours", "days", "weeks", "months", "years" };
        public String[] intervalTimeUnitTypes = { "s", "m", "h", "d", "w", "n", "y" };
        public String[] fillMethods = { "none", "null", "value", /*"linear", */"prev"};
        public String[] sliceFillMethods = { "none", "null", /*"linear", */"prev" };
    }
}
