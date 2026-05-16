using TDPIConnector.PI2;
using System.Collections.Generic;
using System;
using System.IO;
using System.Timers;
using log4net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PISimulator
{

    class SimulatorFromCSV
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private class SigleData {
            public int offset;
            public object value;
        };
        private class InsertData
        {
            public string point;
            public DateTime ts;
            public object value;

            public InsertData(string point, DateTime ts, object value)
            {
                this.point = point;
                this.ts = ts;
                this.value = value;
            }
        }
        class SimulationData {
            public string point;
            public ThisValType type;
            public DateTime start;
            public int currentIndex = 0;
            public List<SigleData> data;

            public SimulationData(string point)
            {
                this.point = point;
                currentIndex = 0;
                start = DateTime.Now;
                data = new List<SigleData>();
            }

            private DateTime NextTs() {
                return start.AddSeconds(data[currentIndex].offset);
            }
            public List<InsertData> PopPreData(DateTime now) {
                var res = new List<InsertData>();
                while (true) {
                    var ts = NextTs();
                    if (ts <= now)
                    {
                        res.Add(new InsertData(point, ts, data[currentIndex].value));
                        SetNextIndex();
                    }
                    else {
                        break;
                    }
                    
                }
                return res;
            }
            private void SetNextIndex()
            {
                if (currentIndex + 1 < data.Count)
                {
                    ++currentIndex;
                }
                else {
                    start = DateTime.Now.AddSeconds(2); // 一轮模拟完成，两秒后下一轮循环
                    currentIndex = 0;
                }
            }
        }

        private Dictionary<string, SimulationData> simulationDataList;
        private PIServerManager piServerManager;
        private string csvPath;

        public SimulatorFromCSV(string piServerName) {
            piServerManager = new PIServerManager(piServerName);
            csvPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "csv");
            simulationDataList = new Dictionary<string, SimulationData>();
        }


        public void Start() {
            piServerManager.Connect();
            CreateSimulationData(GetCSVFiles(csvPath));
            var points = CreatePIPoints();
            updatePointCSV(points);
            resetStartTime(DateTime.Now);

            var timer = new System.Timers.Timer(1000);
            timer.Elapsed += OnTimerUpdate;
            timer.Enabled = true;
        }
        public void updatePointCSV(List<string> points) {
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Points.csv");
            using (StreamWriter writer = new StreamWriter(filePath))
            {
                foreach (string point in points)
                {
                    writer.WriteLine(point);
                }
            }

            log.Info("Point list written to point.csv.");
        }

        private void resetStartTime(DateTime now)
        {
            foreach (var data in simulationDataList)
            {
                data.Value.start = now;
            }
        }

        public void OnTimerUpdate(object sender, ElapsedEventArgs e)
        {
            var now = DateTime.Now;
            int maxConcurrency = 30; // 最大并发数
            SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(maxConcurrency);
            List<Task> tasks = new List<Task>();
            foreach (var data in simulationDataList)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await concurrencySemaphore.WaitAsync();
                    try
                    {
                        var res = data.Value.PopPreData(now);
                        foreach (var pointValue in res) {
                            piServerManager.UpdataPoint(pointValue.point, pointValue.ts, pointValue.value);
                        }
                    }
                    finally
                    {
                        concurrencySemaphore.Release();
                    }
                }));
            }
            Task.WaitAll(tasks.ToArray());
        }

        private List<string> GetCSVFiles(string csvPath)
        {
            string[] files = Directory.GetFiles(csvPath);
            List<string> csvFiles = new List<string>();
            foreach (string file in files)
            {
                if (Path.GetExtension(file).ToLower() == ".csv")
                {
                    csvFiles.Add(file);
                }
            }
            return csvFiles;
        }

        public List<string> CreatePIPoints()
        {
            var pointList = new List<string>();
            foreach (var data in simulationDataList) {
                piServerManager.CreatePoint(data.Key, data.Value.type);
                pointList.Add(data.Key);
            }
            return pointList;
        }

        private void CreateSimulationData(List<string> files) {
            foreach (var filePath in files)
            {
                var file = Path.GetFileNameWithoutExtension(filePath);
                if (!simulationDataList.ContainsKey(file)) {
                    simulationDataList.Add(file, ReadDataFromFile(filePath));
                }
            }
        }

        private SimulationData ReadDataFromFile(string csvFile)
        {
            var point = Path.GetFileNameWithoutExtension(csvFile);
            SimulationData res = new SimulationData(point);
            
            string[] lines = File.ReadAllLines(csvFile);
            List<DateTime> tsList = new List<DateTime>();
            List<double> valList = new List<double>();

            DateTime start = DateTime.Now;
            res.type = ThisValType.Unknown;
            foreach (string line in lines.Skip(1))
            {
                string[] columns = line.Split(',');
                DateTime ts = DateTime.Parse(columns[0].Replace("\"", ""));

                setType(ref res.type, columns[1]);
                object val = getVal(res.type, columns[1]);
                if (res.data.Count == 0) {
                    start = ts;
                }
                SigleData data = new SigleData();
                data.value = val;
                data.offset = (int)(ts - start).TotalSeconds;
                res.data.Add(data);
            }
            return res;
        }

        private object getVal(ThisValType type, string v)
        {
            if (type == ThisValType.Double)
            {
                return double.Parse(v);
            }
            else if (type == ThisValType.String)
            {
                return v;
            }
            else {
                log.Error($"Type {type} not support.");
                throw new NotImplementedException();
            }
        }

        private void setType(ref ThisValType type, string v)
        {
            if (type != ThisValType.Unknown)
            {
                return;
            }
            double number;
            if (double.TryParse(v, out number)) {
                type = ThisValType.Double;
            } else {
                type = ThisValType.String;
            }
        }
    }
}
