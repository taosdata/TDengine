using Newtonsoft.Json;
using System.Text;
using System.Text.RegularExpressions;
using TDengineDriver;

namespace Consumer
{
    internal class TDengineWriter
    {
        StringBuilder stringBuilder = new StringBuilder();

        const string createTable = "CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);";

        public string GenerateSql(MeterTag key, MeterValues values)
        {
            stringBuilder.Clear();
            string childTable = Regex.Replace(key.Location + key.GroupID, "[ ,]", String.Empty);

            stringBuilder.Append("insert into ");
            stringBuilder.Append(childTable);
            stringBuilder.Append(" using meters");
            stringBuilder.Append(" tags(");
            stringBuilder.Append('\'');
            stringBuilder.Append(key.Location);
            stringBuilder.Append('\'');
            stringBuilder.Append(',');
            stringBuilder.Append(key.GroupID);
            stringBuilder.Append(" ) values (");
            stringBuilder.Append(values.TimeStamp);
            stringBuilder.Append(',');
            stringBuilder.Append(values.Current);
            stringBuilder.Append(',');
            stringBuilder.Append(values.Voltage);
            stringBuilder.Append(',');
            stringBuilder.Append(values.Phase);
            stringBuilder.Append(" )");
            return stringBuilder.ToString();
        }

        public string GenerateSqlBatch(string tag, Queue<string> values)
        {
            stringBuilder.Clear();
            MeterTag key = JsonConvert.DeserializeObject<MeterTag>(tag);


            string childTable = Regex.Replace(key.Location + key.GroupID, "[ ,]", String.Empty);
            stringBuilder.Append("insert into ");
            stringBuilder.Append(childTable);
            stringBuilder.Append(" using meters");
            stringBuilder.Append(" tags(");
            stringBuilder.Append('\'');
            stringBuilder.Append(key.Location);
            stringBuilder.Append('\'');
            stringBuilder.Append(',');
            stringBuilder.Append(key.GroupID);
            stringBuilder.Append(" ) values ");
            foreach (string v in values)
            {
                MeterValues row = JsonConvert.DeserializeObject<MeterValues>(v);
                stringBuilder.Append("( ");
                stringBuilder.Append(row.TimeStamp);
                stringBuilder.Append(',');
                stringBuilder.Append(row.Current);
                stringBuilder.Append(',');
                stringBuilder.Append(row.Voltage);
                stringBuilder.Append(',');
                stringBuilder.Append(row.Phase);
                stringBuilder.Append(" )");
            }

            return stringBuilder.ToString();
        }
        public void InsertData(IntPtr conn, string sql)
        {

            //Console.WriteLine("{0} {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), sql);

            IntPtr res = TDengine.Query(conn, sql);
            CheckTDQuery(res);
            TDengine.FreeResult(res);
        }

        private void CheckTDQuery(IntPtr taosRes)
        {
            if (taosRes != IntPtr.Zero && TDengine.ErrorNo(taosRes) != 0)
            {
                throw new Exception($"{TDengine.Error(taosRes)},{TDengine.ErrorNo(taosRes)}");
            }
        }
    }
}
