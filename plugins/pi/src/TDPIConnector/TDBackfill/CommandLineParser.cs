using System;

namespace TDBackfill
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
                else if (args[i] == "-drop" || args[i] == "--drop-table")
                {
                    options.DropTables = true;
                }
                else if (args[i] == "-a" || args[i] == "--all")
                {
                    options.BackfillAll = true;
                }
                else if (args[i] == "-to" || args[i] == "--to-first-recorded")
                {
                    options.BackfillToFirstRecorded = true;
                }
                else if (args[i] == "-from" || args[i] == "--from-last-recorded")
                {
                    options.BackfillFromLastRecorded = true;
                }
                else if (args[i] == "-f" || args[i] == "--file-toml")
                {
                    options.tomlFile = args[i + 1];
                    i++;
                }
                else if (args[i] == "-s" || args[i] == "--start")
                {
                    if (i + 1 < args.Length)
                    {
                        DateTime start;
                        if (DateTime.TryParse(args[i + 1], out start))
                        {
                            options.Start = start;
                            i++;
                        }
                        else
                        {
                            throw new ArgumentException("Invalid start date.");
                        }
                    }
                    else
                    {
                        throw new ArgumentException("Invalid start date.");
                    }
                }
                else if (args[i] == "-e" || args[i] == "--end")
                {
                    if (i + 1 < args.Length)
                    {
                        DateTime end;
                        if (DateTime.TryParse(args[i + 1], out end))
                        {
                            options.End = end;
                            i++;
                        }
                        else
                        {
                            throw new ArgumentException("Invalid end date.");
                        }
                    }
                    else
                    {
                        throw new ArgumentException("Invalid end date.");
                    }
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
