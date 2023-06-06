using TDPIConnector.PI2;
using System.Collections.Generic;
using System;
using System.IO;
using System.Timers;
using log4net;
using System.Linq;

namespace PISimulator
{
    class SimulatorFromCSV
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private class SigleData {
            public int offset;
            public double value;
        };
        private class InsertData
        {
            public string point;
            public DateTime ts;
            public double value;

            public InsertData(string point, DateTime ts, double value)
            {
                this.point = point;
                this.ts = ts;
                this.value = value;
            }
        }
        class SimulationData {
            public string point;
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
            CreatePIPoints();
            resetStartTime(DateTime.Now);

            var timer = new Timer(1000);
            timer.Elapsed += OnTimerUpdate;
            timer.Enabled = true;
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
            foreach (var data in simulationDataList)
            {
                var res = data.Value.PopPreData(now);
                foreach (var pointValue in res) {
                    piServerManager.UpdataPoint(pointValue.point, pointValue.ts, pointValue.value);
                }
            }
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

        public void CreatePIPoints()
        {
            foreach (var data in simulationDataList) {
                piServerManager.CreatePoint(data.Key);
            }
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
            foreach (string line in lines.Skip(1))
            {
                string[] columns = line.Split(',');
                DateTime ts = DateTime.Parse(columns[0].Replace("\"", ""));

                double val = double.Parse(columns[1]);
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
    }
}
