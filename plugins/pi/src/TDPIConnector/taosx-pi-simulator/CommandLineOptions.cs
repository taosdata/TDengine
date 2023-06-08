using System;

namespace PISimulator
{
    internal class CommandLineOptions
    {
        public CommandLineOptions()
        {
        }
        public bool Help { get; internal set; }
        public bool ShowVersion { get; internal set; }
        public bool DropTables { get; internal set; }
        public string tomlFile { get; internal set; }

    }
}

