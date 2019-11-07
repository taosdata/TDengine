using System;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Collections;
using System.Threading.Tasks;
using System.Text;

namespace TDengineExcelPlugins
{
    public class TDExcelFormula
    {
        public String formula;
        public String sql;
        public String result;
        public bool finished;
        public String table;
        public TDExcelFormula(String formula)
        {
            this.formula = formula;
            this.finished = false;
            this.result = String.Empty;
            this.sql = String.Empty;
            this.table = String.Empty;
        }
        public String GetKey()
        {
            int position = this.formula.LastIndexOf(",");
            if (position == -1) return String.Empty;
            return this.formula.Substring(0, position);
        }
    }

    public class TDExcelMergedFromula
    {
        public ArrayList formulas = new ArrayList();
        private String sql;
        private String error = String.Empty;

        public bool Full()
        {
            return this.formulas.Count >= TDFactory.Util.TD_MAX_SQL_COUNT;
        }

        public void Add(TDExcelFormula formula)
        {
            this.formulas.Add(formula);
        }

        public void Run()
        {
            if (this.formulas.Count == 1)
            {
                TDExcelFormula formula = this.formulas[0] as TDExcelFormula;
                formula.result = TDFactory.Formula.CalculateResultImp(formula.sql);
            }
            else
            {
                bool parsed = this.Parse();
                if (parsed)
                {
                    this.Execute();
                }
            }
        }

        private bool Parse()
        {
            if (formulas.Count == 0)
            {
                error = "no formulas input";
                this.SetResult(null);
                return false;
            }

            TDExcelFormula formula = formulas[0] as TDExcelFormula;
            if (formulas.Count == 1)
            {
                this.sql = formula.sql;
                return true;
            }

            //select count(*) from tb where _c0 ...
            int position1 = formula.sql.IndexOf(" from ");
            if (position1 == -1)
            {
                error = "keywords from not find";
                this.SetResult(null);
                return false;
            }
            position1 += 6;
            String sql1 = formula.sql.Substring(0, position1);

            int position2 = formula.sql.IndexOf(" where ", position1);
            if (position2 == -1)
            {
                error = "keywords where not find";
                this.SetResult(null);
                return false;
            }
            String sql2 = formula.sql.Substring(position2 + 7, formula.sql.Length - position2 - 7);

            position1 = formula.sql.IndexOf(".", position1) + 1;
            formula.table = formula.sql.Substring(position1, position2  - position1).ToLower();
            String stableName = TDFactory.Util.GetMetricsNameOfTable(formula.table);
            if (stableName == String.Empty) {
                error = formula.table + TDFactory.Util.TD_TABLE_NOT_FROM_ANY_STABLE_SURFIX;
                this.SetResult(null);
                return false;
            }
            else if (stableName == TDFactory.Util.TD_TABLE_NOT_EXIST)
            {
                error = formula.table + TDFactory.Util.TD_TABLE_NOT_EXIST_SURFIX;
                this.SetResult(null);
                return false;
            }

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.Append(sql1).Append(TDFactory.Persist.connectDB).Append('.').Append(stableName).Append(" where tbname in(");
            sqlBuilder.Append('\'').Append(formula.table).Append('\'');
            for (int i = 0; i < this.formulas.Count; ++i)
            {
                TDExcelFormula f = (TDExcelFormula)this.formulas[i] as TDExcelFormula;
                position1 = f.sql.IndexOf(" from ");
                position1 = f.sql.IndexOf(".", position1) + 1;
                position2 = f.sql.IndexOf(" where ", position1);
                f.table = f.sql.Substring(position1, position2 - position1).ToLower();
                sqlBuilder.Append(",\'").Append(f.table).Append('\'');
            }
            sqlBuilder.Append(") and ").Append(sql2).Append(" group by tbname");

            this.sql = sqlBuilder.ToString();
            return true;
        }

        private void Execute()
        {
            String httpError;
            Hashtable hash = new Hashtable();
            JObject jo = TDFactory.Http.DoRequestSilent(this.sql, TDHttpTimestampType.TD_SHOW_TIMESTSAMP, out httpError);
            if (jo != null)
            {
                try
                {
                    Array heads = jo.GetValue("head").ToArray();
                    Array datas = jo.GetValue("data").ToArray();
                    int headLength = heads.GetLength(0);
                    int dataLength = datas.GetLength(0);

                    if (headLength != 2)
                    {
                        this.error = TDFactory.Util.ErrorPrefix + "invalid response from server";
                        this.SetResult(hash);
                        return;
                    }

                    if (dataLength < 1)
                    {
                        this.error = TDFactory.Util.ErrorPrefix + "null response from server";
                        this.SetResult(hash);
                        return;
                    }

                    for (int row = 0; row < dataLength; ++row)
                    {
                        Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                        String groupByName = dataCols.GetValue(1).ToString();
                        String value = dataCols.GetValue(0).ToString();
                        hash.Add(groupByName, value);
                    }
                }
                catch (Exception e)
                {
                    this.error = TDFactory.Util.ExceptionPrefix + e.Message;
                }
                finally { }
            }
            else
            {
                this.error = httpError;
            }

            this.SetResult(hash);
        }

        private void SetResult(Hashtable hash)
        {
            if (this.error != String.Empty)
            { 
                foreach (TDExcelFormula formula in this.formulas)
                {
                    formula.result = this.error;
                }
            }
            else
            {
                foreach (TDExcelFormula formula in this.formulas)
                {
                    Object hashValue = hash[formula.table];
                    if (hashValue != null)
                    {
                        formula.result = hashValue as String;
                    }
                    else
                    {
                        formula.result = TDFactory.Util.ErrorPrefix + "no result of " + formula.table;
                    }
                }
            }            
        }
    }
    
    public enum TDFormulaCalcType
    {
        TD_FORMULA_NOT_CALC,
        TD_FORMULA_CALC_SQL,
        TD_FORMULA_CALC_VALUE,
    }

    public class TDFormula
    {
        private Hashtable sqlResults = new Hashtable();
        private Hashtable formulaResults = new Hashtable();
        public TDFormulaCalcType CalcType = TDFormulaCalcType.TD_FORMULA_NOT_CALC;
        
        public void ClearFormulaResult(String formula)
        {
            this.formulaResults.Remove(formula);
        }

        public String GetFormulaResult(String formula)
        {
            if (this.formulaResults.ContainsKey(formula))
                return this.formulaResults[formula] as String;
            else
                return String.Empty;
        }

        public void PutFormulaResult(String formula, String result)
        {
            this.formulaResults[formula] = result;
        }

        public void ClearSqlResult(String sql)
        {
            this.sqlResults.Remove(sql);
        }

        public String GetSqlResult(String sql)
        {
            if (this.sqlResults.ContainsKey(sql))
                return this.sqlResults[sql] as String;
            else
                return String.Empty;
        }

        public void PutSqlResult(String sql, String result)
        {
            this.sqlResults[sql] = result;
        }

        public void ClearAll()
        {
            this.formulaResults.Clear();
            this.sqlResults.Clear();
        }

        public String CalculateResult(String sql)
        {
            if (this.CalcType == TDFormulaCalcType.TD_FORMULA_CALC_VALUE)
            {
                return CalculateResultImp(sql);
            }
            else if (this.CalcType == TDFormulaCalcType.TD_FORMULA_NOT_CALC)
            {
                return TDFactory.Util.InfoPrefix + "should run via dialog";
            }
            else
            {
                return sql;
            }
        }
        
        public async Task<String> CalculateResultAsync(String sql)
        {
            return await Task.Factory.StartNew<String>(() => { return this.CalculateResultImp(sql); });
        }

        public String CalculateResultImp(String sql)
        {
            if (sqlResults.Contains(sql)) return sqlResults[sql] as String;

            String error;
            JObject jo = TDFactory.Http.DoRequestSilent(sql, TDHttpTimestampType.TD_SHOW_TIMESTSAMP, out error);
            if (jo != null)
            {
                try
                {
                    Array heads = jo.GetValue("head").ToArray();
                    Array datas = jo.GetValue("data").ToArray();
                    int headLength = heads.GetLength(0);
                    int dataLength = datas.GetLength(0);
                    if (headLength != 1)
                    {
                        return TDFactory.Util.ErrorPrefix + "invalid response from server";
                    }
                   
                    if (dataLength < 1)
                    {
                        return "no result";
                    }

                    Array dataCols = (datas.GetValue(0) as JToken).ToArray();
                    String result = dataCols.GetValue(0).ToString();
                    return result;
                }
                catch (Exception e) {
                    return TDFactory.Util.ExceptionPrefix + e.Message;
                }
                finally { }
            }

            return TDFactory.Util.ErrorPrefix + error;
        }
    }
}
