using System;
using TDPIConnector.Core.ScanPiInfo;

namespace TDBackfill
{
    internal class CommandLineOptions
    {
        public enum WorkMode
        {
            Observer,
            Backfill,
            PrintPIInfo,
            CheckConfig
        };
        public CommandLineOptions()
        {
            Start = DateTime.MinValue;
            End = DateTime.MaxValue;
        }
        public bool Help { get; internal set; }
        public bool ShowVersion { get; internal set; }
        public bool DropTables { get; internal set; }
        public bool BackfillAll { get; internal set; }
        public bool BackfillToFirstRecorded { get; internal set; }
        public bool BackfillFromLastRecorded { get; internal set; }
        public DateTime Start { get; internal set; }
        public DateTime End { get; internal set; }
        public string tomlFile { get; internal set; }

        public WorkMode workMode = WorkMode.Backfill;
        public ScanMode printMode = ScanMode.ScanNone;
        public FilterMode fileterMode = FilterMode.FilterNone;
        public string filter = "";
    }
}

