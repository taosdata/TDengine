using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR.TDEngineClient
{
    public class TDPIStream
    {
        private readonly TDHttpClient httpClient;

        public string Database { get; set; }
        public string Table { get; set; }
        public string ColumnValue { get; set; }
        public string ColumnStatus { get; set; }

        public TDPIStreamMode Mode { get; set; }


        internal TDPIStream(TDHttpClient httpClient)
        {
            this.httpClient = httpClient;
        }

        internal static TDPIStream CreateTDPIStreamForPIPoint(TDHttpClient httpClient, string database, string pointName)
        {
            TDPIStream tdPIStream = new TDPIStream(httpClient);
            tdPIStream.Database = database.SanitizeIdentifier();
            tdPIStream.Table = pointName.SanitizeIdentifier();
            tdPIStream.ColumnValue = "val";
            tdPIStream.ColumnStatus = "quality";
            tdPIStream.Mode = TDPIStreamMode.PIPoint;
            return tdPIStream;
        }

        internal static TDPIStream CreateTDPIStreamForAFElement(TDHttpClient httpClient, string database, string element, string attribute)
        {
            TDPIStream tdPIStream = new TDPIStream(httpClient);
            tdPIStream.Database = database.SanitizeIdentifier();
            tdPIStream.Table = element.SanitizeIdentifier();
            tdPIStream.ColumnValue = (attribute.ToDatabaseName() + "_val").SanitizeIdentifier();
            tdPIStream.ColumnStatus = (attribute.ToDatabaseName() + "_status").SanitizeIdentifier();
            tdPIStream.Mode = TDPIStreamMode.AFElement;
            return tdPIStream;
        }

        internal static TDPIStream CreateTDPIStreamForTable(TDHttpClient httpClient, string database, string table, string column)
        {
            TDPIStream tdPIStream = new TDPIStream(httpClient);
            tdPIStream.Database = database.SanitizeIdentifier();
            tdPIStream.Table = table.SanitizeIdentifier();
            tdPIStream.ColumnValue = column.SanitizeIdentifier();
            tdPIStream.ColumnStatus = null;
            tdPIStream.Mode = TDPIStreamMode.Table;
            return tdPIStream;
        }

        public async Task<TDValue> GetSnapshotValueAsync()
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` ORDER BY ts DESC limit 1;";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            return resp.ToTDValue();
        }

        public TDValue GetSnapshotValue()
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` ORDER BY ts DESC limit 1;";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            return resp.ToTDValue();
        }

        public string GetAllStringColumnNames()
        {
            if (!string.IsNullOrEmpty(ColumnStatus))
            {
                return $"ts, `{ColumnValue}`, `{ColumnStatus}`";
            }
            else
            {
                return $"ts, `{ColumnValue}`";
            }
        }

        public async Task<TDValue> GetRecordedValueAsync(DateTime timestamp)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts = '{timestamp.ToUtcTimeString()}';";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            TDValue tdValue = resp.ToTDValue();
            if (tdValue == null)
            {
                return null;
            }
            return tdValue;
        }

        public TDValue GetRecordedValue(DateTime timestamp)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts = '{timestamp.ToUtcTimeString()}';";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            TDValue tdValue = resp.ToTDValue();
            if (tdValue == null)
            {
                return null;
            }
            return tdValue;
        }

        public async Task<TDValues> GetRecordedValuesAsync(DateTime startTime, DateTime endTime)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts >= '{startTime.ToUtcTimeString()}' AND ts <= '{endTime.ToUtcTimeString()}';";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }

        public TDValues GetRecordedValues(DateTime startTime, DateTime endTime)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts >= '{startTime.ToUtcTimeString()}' AND ts <= '{endTime.ToUtcTimeString()}';";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }

        public async Task<TDValues> RecordedValuesAtTimesAsync(IList<DateTime> times)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ";

            foreach (DateTime time in times)
            {
                sqlCommand += $"ts = '{time.ToUtcTimeString()}' OR ";
            }
            sqlCommand = sqlCommand.Substring(0, sqlCommand.Length - 4);
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }

        public TDValues RecordedValuesAtTimes(List<DateTime> times)
        {
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ";

            foreach (DateTime time in times)
            {
                sqlCommand += $"ts = '{time.ToUtcTimeString()}' OR ";
            }
            sqlCommand = sqlCommand.Substring(0, sqlCommand.Length - 4);
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }

        public async Task<TDValues> RecordedValuesByCountAsync(DateTime timestamp, int numberOfEvents, bool forward)
        {
            string greaterOrLess = ">=";
            if (!forward)
            {
                greaterOrLess = "<=";
            }
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts {greaterOrLess} '{timestamp.ToUtcTimeString()}' LIMIT {numberOfEvents};";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }

        public TDValues RecordedValuesByCount(DateTime timestamp, int numberOfEvents, bool forward)
        {
            string greaterOrLess = ">=";
            string orderByDescAsc = "ASC";
            if (!forward)
            {
                greaterOrLess = "<=";
                orderByDescAsc = "DESC";
            }
            string sqlCommand = $"select {GetAllStringColumnNames()} from `{Database}`.`{Table}` where ts {greaterOrLess} '{timestamp.ToUtcTimeString()}' ORDER BY ts {orderByDescAsc} LIMIT {numberOfEvents};";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            TDValues tdValues = resp.ToTDValues();
            return tdValues;
        }


        public async Task<TDValue> InterpolatedValueAsync(DateTime timestamp)
        {
            string sqlCommand = $"select interp(`{ColumnValue}`) from `{Database}`.`{Table}` RANGE('{timestamp.AddDays(-1).ToUtcTimeString()}','{timestamp.AddDays(1).ToUtcTimeString()}') EVERY(1d) FILL(LINEAR)";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            double value = ConvertToDouble(resp.Data[1][0]);
            return new TDValue(value, timestamp, 0, TDValueType.Double);

        }

        internal TDValue InterpolatedValue(DateTime timestamp)
        {
            string sqlCommand = $"select interp(`{ColumnValue}`) from `{Database}`.`{Table}` RANGE('{timestamp.AddDays(-1).ToUtcTimeString()}','{timestamp.AddDays(1).ToUtcTimeString()}') EVERY(1d) FILL(LINEAR)";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            if (resp.Data.Count == 3)
            {
                return new TDValue(ConvertToDouble(resp.Data[1][0]), timestamp, 0, TDValueType.Double);
            }
            TDValues valuesForward = this.RecordedValuesByCount(timestamp, 1, true);
            TDValues valuesBackward = this.RecordedValuesByCount(timestamp, 1, false);
            return InterpolatorService.InterpolateTwoValues(timestamp, valuesBackward, valuesForward);

        }

        public TDValues InterpolatedValues(DateTime startTime, DateTime endTime, TimeSpan interval)
        {

            string intervalString = GetIntervalString(interval);
            string sqlCommand = $"select _irowts, interp(`{ColumnValue}`) from `{Database}`.`{Table}` RANGE('{startTime.ToUtcTimeString()}','{endTime.ToUtcTimeString()}') EVERY({intervalString}) FILL(LINEAR)";
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            TDValues tdValues = new TDValues();
            for (int i = 0; i < resp.Data.Count; i++)
            {
                DateTime ts = Convert.ToDateTime(resp.Data[i][0]);
                double value = ConvertToDouble(resp.Data[i][1]);
                TDValue tdValue = new TDValue(value, ts, 0, TDValueType.Double);
                tdValues.Add(tdValue);
            }
            return tdValues;
        }

        private string GetIntervalString(TimeSpan interval)
        {
            int totalSeconds = Convert.ToInt32(Math.Round(interval.TotalSeconds));
            if (totalSeconds < 60)
            {
                int totalMilliseconds = Convert.ToInt32(Math.Round(interval.TotalMilliseconds));
                return $"{totalMilliseconds}a";
            }
            return $"{totalSeconds}s";
        }

        private double ConvertToDouble(string value)
        {
            double result;
            double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
            return result;
        }

        public async Task<TDValues> InterpolatedValuesAsync(DateTime startTime, DateTime endTime, TimeSpan interval)
        {

            string intervalString = GetIntervalString(interval);
            string sqlCommand = $"select _irowts, interp(`{ColumnValue}`) from `{Database}`.`{Table}` RANGE('{startTime.ToUtcTimeString()}','{endTime.ToUtcTimeString()}') EVERY({intervalString}) FILL(LINEAR)";
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            TDValues tdValues = new TDValues();
            for (int i = 0; i < resp.Data.Count; i++)
            {
                DateTime ts = Convert.ToDateTime(resp.Data[i][0]);
                double value = ConvertToDouble(resp.Data[i][1]);
                TDValue tdValue = new TDValue(value, ts, 0, TDValueType.Double);
                tdValues.Add(tdValue);
            }
            return tdValues;
        }

        public async Task<TDValues> InterpolatedValuesAtTimesAsync(IList<DateTime> times)
        {
            TDValues tdValues = new TDValues();
            foreach (DateTime time in times)
            {
                TDValue tdValue = await InterpolatedValueAsync(time);
                tdValues.Add(tdValue);
            }
            return tdValues;
        }

        public TDValues InterpolatedValuesAtTimes(List<DateTime> times)
        {
            TDValues tdValues = new TDValues();
            foreach (DateTime time in times)
            {
                TDValue tdValue = InterpolatedValue(time);
                tdValues.Add(tdValue);
            }
            return tdValues;
        }

        public async Task<TDValues> PlotValuesAsync(DateTime startTime, DateTime endTime, int numberOfIntervals)
        {
            long totalTicks = (endTime - startTime).Ticks;
            long intervalTick = Convert.ToInt64(((totalTicks * 1.0) / (numberOfIntervals - 1)));
            TimeSpan interval = new TimeSpan(intervalTick);
            return await this.InterpolatedValuesAsync(startTime, endTime, interval);
        }

        internal TDValues PlotValues(DateTime startTime, DateTime endTime, int numberOfIntervals)
        {
            long totalTicks = (endTime - startTime).Ticks;
            long intervalTick = Convert.ToInt64(((totalTicks * 1.0) / (numberOfIntervals - 1)));
            TimeSpan interval = new TimeSpan(intervalTick);
            return this.InterpolatedValues(startTime, endTime, interval);
        }

        public async Task<IDictionary<TDSummaryTypes, TDValue>> SummaryAsync(DateTime startTime, DateTime endTime, TDSummaryTypes tdSummaryTypes)
        {
            string sqlCommand = GeneratSQLCommandForSummary(startTime, endTime, tdSummaryTypes);
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            return ConvertToSummaryResponse(resp, tdSummaryTypes, startTime);
        }

        public IDictionary<TDSummaryTypes, TDValue> Summary(DateTime startTime, DateTime endTime, TDSummaryTypes tdSummaryTypes)
        {
            string sqlCommand = GeneratSQLCommandForSummary(startTime, endTime, tdSummaryTypes);
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            return ConvertToSummaryResponse(resp, tdSummaryTypes, startTime);
        }

        public async Task<IDictionary<TDSummaryTypes, TDValues>> SummariesAsync(DateTime startTime, DateTime endTime, TimeSpan interval, TDSummaryTypes tdSummaryTypes)
        {
            string sqlCommand = this.GenerateSQLCommandForSummaries(startTime, endTime, interval, tdSummaryTypes);
            TDEngineResponse resp = await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
            return ConvertToSummaryResponse(resp, tdSummaryTypes);
        }

        internal IDictionary<TDSummaryTypes, TDValues> Summaries(DateTime startTime, DateTime endTime, TimeSpan interval, TDSummaryTypes tdSummaryTypes)
        {
            string sqlCommand = this.GenerateSQLCommandForSummaries(startTime, endTime, interval, tdSummaryTypes);
            TDEngineResponse resp = this.httpClient.RetrieveData(sqlCommand, Database);
            return ConvertToSummaryResponse(resp, tdSummaryTypes);
        }

        public async Task UpdateValuesAsync(IEnumerable<TDValue> values)
        {
            CheckPIPointMode();
            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO `{Database}`.`{Table}` VALUES ");
            foreach (TDValue value in values)
            {
                if (value.Quality == 0)
                {
                    sb.Append($"('{value.TimestampString}', {value.ValueString}, 0) ");
                }
                else
                {
                    sb.Append($"('{value.TimestampString}', NULL, {value.Quality}) ");
                }
            }
            string sqlCommand = sb.ToString();
            await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
        }

        internal void UpdateValues(TDValues values)
        {
            CheckPIPointMode();
            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO `{Database}`.`{Table}` VALUES ");
            foreach (TDValue value in values)
            {
                if (value.Quality == 0)
                {
                    sb.Append($"('{value.TimestampString}', {value.ValueString}, 0) ");
                }
                else
                {
                    sb.Append($"('{value.TimestampString}', NULL, {value.Quality}) ");
                }
            }
            string sqlCommand = sb.ToString();
            this.httpClient.RetrieveData(sqlCommand, Database);
        }

        public async Task UpdateValueAsync(TDValue value)
        {
            CheckPIPointMode();
            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO `{Database}`.`{Table}` VALUES ");

            if (value.Quality == 0)
            {
                sb.Append($"('{value.TimestampString}', {value.ValueString}, 0) ");
            }
            else
            {
                sb.Append($"('{value.TimestampString}', NULL, {value.Quality}) ");
            }
            string sqlCommand = sb.ToString();
            await this.httpClient.RetrieveDataAsync(sqlCommand, Database);
        }

        internal void UpdateValue(TDValue value)
        {
            CheckPIPointMode();
            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO `{Database}`.`{Table}` VALUES ");

            if (value.Quality == 0)
            {
                sb.Append($"('{value.TimestampString}', {value.ValueString}, 0) ");
            }
            else
            {
                sb.Append($"('{value.TimestampString}', NULL, {value.Quality}) ");
            }
            string sqlCommand = sb.ToString();
            this.httpClient.RetrieveData(sqlCommand, Database);
        }

        private void CheckPIPointMode()
        {
            if (this.Mode != TDPIStreamMode.PIPoint)
            {
                throw new Exception("Cannot insert values using AF mode TDengine table structure.");
            }
        }

        private string GeneratSQLCommandForSummary(DateTime startTime, DateTime endTime, TDSummaryTypes tdSummaryTypes)
        {
            string querySummary = string.Empty;
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Average))
            {
                querySummary += $"AVG(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Count))
            {
                querySummary += $"COUNT(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Maximum))
            {
                querySummary += $"MAX(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Minimum))
            {
                querySummary += $"MIN(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Total))
            {
                querySummary += $"SUM(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.StdDev))
            {
                querySummary += $"STDDEV(`{ColumnValue}`), ";
            }
            if (string.IsNullOrEmpty(querySummary))
            {
                throw new Exception("At least one summary type must be added.");
            }
            querySummary = querySummary.Substring(0, querySummary.Length - 2);


            string sqlCommand = $"select {querySummary} from `{Database}`.`{Table}` where ts >= '{startTime.ToUtcTimeString()}' AND ts <= '{endTime.ToUtcTimeString()}';";
            return sqlCommand;
        }

        private void FillDicForSummary(IDictionary<TDSummaryTypes, TDValue> dic, TDEngineResponse resp, DateTime ts, int dataIndex, TDSummaryTypes summaryType, TDValueType tdValueType)
        {
            if (tdValueType == TDValueType.Double)
            {
                double value = ConvertToDouble(resp.Data[0][dataIndex]);
                dic[summaryType] = new TDValue(value, ts, 0, tdValueType);
            }
            if (tdValueType == TDValueType.Int)
            {
                int value = Convert.ToInt32(resp.Data[0][dataIndex]);
                dic[summaryType] = new TDValue(value, ts, 0, tdValueType);
            }
        }

        private IDictionary<TDSummaryTypes, TDValue> ConvertToSummaryResponse(TDEngineResponse resp, TDSummaryTypes tdSummaryTypes, DateTime startTime)
        {
            IDictionary<TDSummaryTypes, TDValue> dic = new Dictionary<TDSummaryTypes, TDValue>();
            int dataIndex = 0;
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Average))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.Average, TDValueType.Double);
                dataIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Count))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.Count, TDValueType.Int);
                dataIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Maximum))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.Maximum, TDValueType.Double);
                dataIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Minimum))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.Minimum, TDValueType.Double);
                dataIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Total))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.Total, TDValueType.Double);
                dataIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.StdDev))
            {
                FillDicForSummary(dic, resp, startTime, dataIndex, TDSummaryTypes.StdDev, TDValueType.Double);
            }

            return dic;
        }

        private string GenerateSQLCommandForSummaries(DateTime startTime, DateTime endTime, TimeSpan interval, TDSummaryTypes tdSummaryTypes)
        {
            string querySummary = "_WSTART, ";
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Average))
            {
                querySummary += $"AVG(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Count))
            {
                querySummary += $"COUNT(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Maximum))
            {
                querySummary += $"MAX(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Minimum))
            {
                querySummary += $"MIN(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Total))
            {
                querySummary += $"SUM(`{ColumnValue}`), ";
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.StdDev))
            {
                querySummary += $"STDDEV(`{ColumnValue}`), ";
            }
            querySummary = querySummary.Substring(0, querySummary.Length - 2);


            string sqlCommand = $"select {querySummary} from `{Database}`.`{Table}` where ts >= '{startTime.ToUtcTimeString()}' AND ts <= '{endTime.ToUtcTimeString()}' INTERVAL({interval.TotalMinutes}m) FILL(NULL);";
            return sqlCommand;
        }

        private void FillDicForSummaries(IDictionary<TDSummaryTypes, TDValues> dic, TDEngineResponse resp, int summaryIndex, int dataItemsLength, TDSummaryTypes summaryType, TDValueType valueType)
        {
            dic[summaryType] = new TDValues();
            for (int i = 0; i < dataItemsLength; i++)
            {
                DateTime ts = Convert.ToDateTime(resp.Data[i][0]);
                TDValue tdValue;
                if (valueType == TDValueType.Double)
                {
                    tdValue = new TDValue(ConvertToDouble(resp.Data[i][summaryIndex]), ts, 0, TDValueType.Double);
                }
                else
                {
                    tdValue = new TDValue(Convert.ToInt32(resp.Data[i][summaryIndex]), ts, 0, TDValueType.Int);
                }
                dic[summaryType].Add(tdValue);
            }
        }
        private IDictionary<TDSummaryTypes, TDValues> ConvertToSummaryResponse(TDEngineResponse resp, TDSummaryTypes tdSummaryTypes)
        {
            IDictionary<TDSummaryTypes, TDValues> dic = new Dictionary<TDSummaryTypes, TDValues>();

            int summaryIndex = 1;
            int dataItemsLength = resp.Data.Count;

            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Average))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.Average, TDValueType.Double);
                summaryIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Count))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.Count, TDValueType.Int);
                summaryIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Maximum))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.Maximum, TDValueType.Double);
                summaryIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Minimum))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.Minimum, TDValueType.Double);
                summaryIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Total))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.Total, TDValueType.Double);
                summaryIndex++;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.StdDev))
            {
                FillDicForSummaries(dic, resp, summaryIndex, dataItemsLength, TDSummaryTypes.StdDev, TDValueType.Double);
            }
            return dic;
        }


    }
}
