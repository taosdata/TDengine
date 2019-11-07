using System;
using System.Text;
using ExcelDna.Integration;
using Range = Microsoft.Office.Interop.Excel.Range;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// ExcelDNA 自定义函数
    /// </summary>
    public static class ExcelDnaUDF
    {
        private static System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));

        private static String GetResultsByCallerFormula()
        {
            try
            {
                ExcelReference caller = (ExcelReference)XlCall.Excel(XlCall.xlfCaller);
                //if (caller != null)
                //    return (String)XlCall.Excel(XlCall.xlfGetFormula, caller);
                String refText = (String)XlCall.Excel(XlCall.xlfReftext, caller, true);
                if (refText != String.Empty && TDFactory.Application() != null)
                {
                    Range range = TDFactory.Application().Range[refText];
                    if (range != null)
                        return TDFactory.Formula.GetFormulaResult(range.Formula);
                }
            }
            catch (Exception) { }
            finally { }

            return String.Empty;
        }

        private static object GetTDAggSql(object Param1, object Param2, object Param3, object Param4, object Param5, object Param6, out String error)
        {
            //try
            //{
            if (ExcelDnaUtil.IsInFunctionWizard())
            {
                error = "wait input finished.";
                return error;
            }

            if (Param1 is ExcelDna.Integration.ExcelMissing || Param1 is ExcelDna.Integration.ExcelEmpty)
            {
                error = "function name not input";
                return error;
            }
            if (Param2 is ExcelDna.Integration.ExcelMissing || Param2 is ExcelDna.Integration.ExcelEmpty)
            {
                error = "column name not input";
                return error;
            }

            if (Param3 is ExcelDna.Integration.ExcelMissing)
            {
                error = "begin timestamp not input";
                return error;
            }
            if (Param4 is ExcelDna.Integration.ExcelMissing)
            {
                error = "end timestamp not input";
                return error;
            }
            if (Param5 is ExcelDna.Integration.ExcelMissing)
            {
                error = "database not input";
                return error;
            }
            if (Param6 is ExcelDna.Integration.ExcelMissing)
            {
                error = "table name not input";
                return error;
            }

            String beginTime = String.Empty;
            if (Param3 is System.Double)
            {
                beginTime = DateTime.FromOADate(Convert.ToDouble(Param3)).ToString("yyyy-MM-dd HH:mm:ss");
            }
            else if (Param3 is System.String)
            {
                beginTime = Param3 as String;
            }
            else
            {
                error = "invalid format of begin timestamp";
                return error;
            }

            String endTime = String.Empty;
            if (Param4 is System.Double)
            {
                endTime = DateTime.FromOADate(Convert.ToDouble(Param4)).ToString("yyyy-MM-dd HH:mm:ss");
            }
            else if (Param4 is System.String)
            {
                endTime = Param4 as String;
            }
            else
            {
                error = "invalid format of begin timestamp";
                return error;
            }

            StringBuilder builder = new StringBuilder(200);
            builder.Append("select ").Append(Param1.ToString()).Append("(").Append(Param2.ToString()).Append(") from ");
            builder.Append(Param5.ToString()).Append(".").Append(Param6.ToString()).Append(" where _c0>'");
            builder.Append(beginTime).Append("' and _c0<'").Append(endTime).Append("'");

            error = String.Empty;
            return builder.ToString();
            //}
            //catch (Exception e)
            //{
            //    error = TDFactory.Util.ExceptionPrefix + e.Message;
            //    return error;
            //}
            //finally { }
        }

        //*****************************************************************************
        //让自定义函数在工作表函数中可见：必须有返回类型
        //void类型的函数在工作表函数向导中是不可见的,也不可在VBA中执行。
        //viod类型的函数，如果带有参数的话，在宏表函数向导中可见；
        //非viod函数或Void不带参数的话，在宏表函数向导中不可见。（实际上是命令,Type=2）
        //IsMacroType = true ，ReferenceToRange函数才起作用，否则运行时报错
        //VBA中调用方法：ret = Application.Run("Addthem", 1, 2, Range("A1"))
        //Application.ExecuteExcel4Macro ("xxxx()")        
        //Application.ExecuteExcel4Macro ("UDF(""" & ThisWorkbook.Path & Application.PathSeparator & "mystring" & """)")  //传递字符串变量
        //Application.ExecuteExcel4Macro ("UNREGISTER(""" & "xxxx.XLL" & """)") //传递字符串变量

        //https://msdn.microsoft.com/zh-cn/library/office/bb687837.aspx  C API 函数参考 ，Excel 2013 XLL SDK API 函数引用

        //在XP和Office2003中，VBA的Application.RegisteredFunctions函数、Application.RegisterXLL函数
        //区分大小写，所以要确保加载XLL函数的名称字母大小写与XLL文件的名字一致
        //*****************************************************************************

        //http://yi-lee.blog.163.com/blog/static/4955152620151171395919/
        /// <summary>
        /// Addition UDF function
        /// </summary>
        /// <param name="Param1"></param>
        /// <param name="Param2"></param>
        /// <param name="Param3"></param>
        /// <param name="Param4"></param>
        /// <param name="Param5"></param>
        /// <param name="Param6"></param>
        /// <returns></returns>
        [ExcelFunction(Description = "Query of Aggregation",
                       Category = "TDengine Query Function",
                       HelpTopic = "http://www.taosdata.com", //HelpTopic="MyHelp.chm!102"
                       IsHidden = false,
                       IsVolatile = true,
                       IsMacroType = true,
                       Name = "TDAgg")]
        public static object TDAgg(  [ExcelArgument(Description = "the aggregation function name, such as count, sum, max, avg ...", Name = "function")] object Param1,
                                     [ExcelArgument(Description = "the name of selected column", Name = "column")] object Param2,
                                     [ExcelArgument(Description = "begin timestamp of this query", Name = "begin")] object Param3,
                                     [ExcelArgument(Description = "end timestamp of this query", Name = "end")] object Param4,
                                     [ExcelArgument(Description = "the name of table or stable", Name = "table")] object Param5,
                                     [ExcelArgument(Description = "the name of database", Name = "database")] object Param6)
        {
            if (TDFactory.Formula.CalcType == TDFormulaCalcType.TD_FORMULA_CALC_SQL)
            {
                String error;
                String sql = GetTDAggSql(Param1, Param2, Param3, Param4, Param5, Param6, out error) as String;
                if (error != String.Empty) return error;
                return sql;
            }
            else
            {
                String formulaCacheResults = GetResultsByCallerFormula();
                if (formulaCacheResults != String.Empty)
                    return formulaCacheResults;

                String error;
                String sql = GetTDAggSql(Param1, Param2, Param3, Param4, Param5, Param6, out error) as String;
                if (error != String.Empty) return error;
               
                String sqlCacheResults = TDFactory.Formula.GetSqlResult(sql);
                if (sqlCacheResults != String.Empty)
                    return sqlCacheResults;

                return TDFactory.Formula.CalculateResult(sql);
            }

            //called from FCalc form
            //String lastValue = String.Empty;
            //try
            //{
            //    ExcelReference caller = (ExcelReference)XlCall.Excel(XlCall.xlfCaller);
            //    String refText = (String)XlCall.Excel(XlCall.xlfReftext, caller, true);
            //    Range range = TDFactory.Application().Range[refText];
            //    if (range != null && range.Value2 != null && range.Value2.GetType() == typeof(String))
            //        lastValue = range.Value2;
            //}
            //catch (Exception) { }
            //finally { }

            //String lastValue = String.Empty;
            //try
            //{
            //    ExcelReference caller = (ExcelReference)XlCall.Excel(XlCall.xlfCaller);
            //    lastValue = (String)XlCall.Excel(XlCall.xlfValue, caller, true);
            //}
            //catch (Exception) { }
            //finally { }
        }

        private static object GetTDSliceSql(object Param1, object Param2, object Param3, object Param4, object Param5, object Param6, out String error)
        {
            if (ExcelDnaUtil.IsInFunctionWizard())
            {
                error = "wait input finished.";
                return error;
            }

            if (Param1 is ExcelDna.Integration.ExcelMissing || Param1 is ExcelDna.Integration.ExcelEmpty)
            {
                error = "column name not input";
                return error;
            }
            if (Param2 is ExcelDna.Integration.ExcelMissing)
            {
                error = "column name not input";
                return error;
            }
            if (Param3 is ExcelDna.Integration.ExcelMissing)
            {
                error = "fill method not input";
                return error;
            }
            if (Param4 is ExcelDna.Integration.ExcelMissing)
            {
                error = "fill value not input";
                return error;
            }
            if (Param5 is ExcelDna.Integration.ExcelMissing)
            {
                error = "database not input";
                return error;
            }
            if (Param6 is ExcelDna.Integration.ExcelMissing)
            {
                error = "table name not input";
                return error;
            }
            
            String beginTime = String.Empty;
            if (Param2 is System.Double)
            {
                beginTime = DateTime.FromOADate(Convert.ToDouble(Param2)).ToString("yyyy-MM-dd HH:mm:ss");
            }
            else if (Param2 is System.String)
            {
                beginTime = Param2 as String;
            }
            else
            {
                error = "invalid format of timestamp";
                return error;
            }

            StringBuilder builder = new StringBuilder(200);
            builder.Append("select interp(").Append(Param1.ToString()).Append(") from ");
            builder.Append(Param5.ToString()).Append(".").Append(Param6.ToString()).Append(" where _c0='");
            builder.Append(beginTime).Append("'");

            if (!(Param3 is ExcelDna.Integration.ExcelEmpty))
            {
                String intervalMethod = Param3.ToString();
                if (intervalMethod != "value")
                {
                    builder.Append(" fill(").Append(intervalMethod).Append(")");
                }
                else
                {
                    String intervalValue = "0";
                    if (!(Param4 is ExcelDna.Integration.ExcelEmpty)) intervalValue = Param4.ToString();
                    builder.Append(" fill(").Append(intervalMethod).Append(",").Append(intervalValue).Append(")");
                }
            }

            error = String.Empty;
            return builder.ToString();
        }

        //http://yi-lee.blog.163.com/blog/static/4955152620151171395919/
        /// <summary>
        /// Addition UDF function
        /// </summary>
        /// <param name="Param1"></param>
        /// <param name="Param2"></param>
        /// <param name="Param3"></param>
        /// <param name="Param4"></param>
        /// <param name="Param5"></param>
        /// <param name="Param6"></param>
        /// <returns></returns>
        [ExcelFunction(Description = "Query Slice Data of Table",
                       Category = "TDengine Query Function",
                       HelpTopic = "http://www.taosdata.com", //HelpTopic="MyHelp.chm!102"
                       IsHidden = false,
                       IsVolatile = true,
                       IsMacroType = true,
                       Name = "TDSlice")]
        public static object TDSlice([ExcelArgument(Description = "the name of selected column", Name = "column")] object Param1,
                                     [ExcelArgument(Description = "timestamp of this query", Name = "time")] object Param2,
                                     [ExcelArgument(Description = "interpolation method of the slice query", Name = "fill_method")] object Param3,
                                     [ExcelArgument(Description = "the fill value of interpolation", Name = "fill_value")] object Param4,
                                     [ExcelArgument(Description = "the name of database", Name = "database")] object Param5,
                                     [ExcelArgument(Description = "the name of table", Name = "table")] object Param6
                                     )
        {

            if (TDFactory.Formula.CalcType == TDFormulaCalcType.TD_FORMULA_CALC_SQL)
            {
                String error;
                String sql = GetTDSliceSql(Param1, Param2, Param3, Param4, Param5, Param6, out error) as String;
                if (error != String.Empty) return error;
                return sql;
            }
            else
            {
                String formulaCacheResults = GetResultsByCallerFormula();
                if (formulaCacheResults != String.Empty)
                    return formulaCacheResults;

                String error;
                String sql = GetTDSliceSql(Param1, Param2, Param3, Param4, Param5, Param6, out error) as String;
                if (error != String.Empty) return error;

                String sqlCacheResults = TDFactory.Formula.GetSqlResult(sql);
                if (sqlCacheResults != String.Empty)
                    return sqlCacheResults;

                return TDFactory.Formula.CalculateResult(sql);
            }
        }
    }
}