using System;

namespace PISimulator
{
    internal class CommandLineParser
    {
        private string[] args;

        public CommandLineParser(string[] args)
        {
            this.args = args;
        }

        internal CommandLineOptions GetCommandLineOptions()
        {
            CommandLineOptions options = new CommandLineOptions();
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "-h" || args[i] == "--help")
                {
                    options.Help = true;
                }
                else if (args[i] == "-v" || args[i] == "--version")
                {
                    options.ShowVersion = true;
                }
                else if (args[i] == "-d" || args[i] == "--drop")
                {
                    options.DropTables = true;
                }
                //unknow option
                else
                {
                    throw new ArgumentException("Unknown option: " + args[i]);
                }
            }
            return options;
        }
    }

}
