using System;

namespace TDBackfill
{
    internal class CommandLineOptions
    {
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

    }
}

