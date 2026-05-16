using System;

namespace Benchmark
{
    public class EntryPoint
    {
        string Host = "127.0.0.1";
        string User = "root";
        string Passwd = "taosdata";
        ushort Port = 6030;
        string benchmarkOptions = "connect";
        int RunTimes = 1;
        string TableTypes = "normal";
        int NumOfTable = 1;
        int NumOfRecords = 1;

        int MaxSqlLength = 5000;

        // private string Rest;
        static void Main(string[] args)
        {
            EntryPoint entryPoint = new EntryPoint();
            entryPoint.ReadArgs(args);
            entryPoint.RunBenchMark(entryPoint.benchmarkOptions, entryPoint.TableTypes, entryPoint.RunTimes);
        }

        public void PrintHelp()
        {
            string indent = "\t\t\t";
            string indent2 = "\t\t";

            Console.WriteLine("\t -s {0}{1}", indent,
                "Benchmark stage, \"connect\",\"insert\",\"query\",\"avg\",\"batch\",\"clean\",default \"connect\"");
            Console.WriteLine("\t -t {0}{1}", indent,
                "Benchmark data type, table with\"json\" tag,table with \"normal\" column type,default \"normal\"");
            Console.WriteLine("\t -n {0}{1}", indent, "number of times to run.Default 1 time.");
            Console.WriteLine("\t -r {0}{1}", indent, "number of record per table,only for insert.Default 1 records");
            Console.WriteLine("\t -b {0}{1}", indent, "number of target tables,only for insert.Default 1 tables");
            Console.WriteLine("\t -l {0}{1}", indent, "Max length of SQL string,only for insert.default 5000.");
            Console.WriteLine("\t --help {0}{1}", indent2, "Print help info");
            System.Environment.Exit(0);
        }

        public void ReadArgs(string[] args)
        {
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "--help":
                        this.PrintHelp();
                        break;
                    case "-s":
                        this.benchmarkOptions = args[i + 1];
                        i++;
                        break;
                    case "-n":
                        this.RunTimes = int.Parse(args[i + 1]);
                        i++;
                        break;
                    case "-t":
                        this.TableTypes = args[i + 1];
                        i++;
                        break;
                    case "-r":
                        this.NumOfRecords = int.Parse(args[i + 1]);
                        i++;
                        break;
                    case "-b":
                        this.NumOfTable = int.Parse(args[i + 1]);
                        i++;
                        break;
                    case "-l":
                        this.MaxSqlLength = int.Parse(args[i + 1]);
                        i++;
                        break;
                    default:
                        Console.WriteLine("Unsupported option {0}", args[i]);
                        System.Environment.Exit(0);
                        break;
                }
            }
        }


        public void RunBenchMark(string options, string tableTypes, int times)
        {
            switch (options)
            {
                // case "prepare":
                //     Prepare prepare = new Prepare(this.Host, this.User, this.Passwd, this.Port);
                //     prepare.Run(TableTypes);
                //     break;
                case "connect":
                    Connect connect = new Connect(this.Host, this.User, this.Passwd, this.Port);
                    connect.Run(times);
                    break;
                case "insert":
                    Insert insert = new Insert(this.Host, this.User, this.Passwd, this.Port, this.MaxSqlLength);
                    insert.Run(TableTypes, NumOfTable);
                    break;
                case "batch":
                    Batch batch = new Batch(this.Host, this.User, this.Passwd, this.Port, this.MaxSqlLength);
                    batch.Run(TableTypes, NumOfRecords, NumOfTable, times);
                    break;
                // case "batchcol":
                //     BatchColumn batchCol = new BatchColumn(this.Host, this.User, this.Passwd, this.Port);
                //     batchCol.Run(TableTypes, times);
                //     break;
                case "query":
                    Query query = new Query(this.Host, this.User, this.Passwd, this.Port);
                    query.Run(TableTypes, times);
                    break;
                case "avg":
                    Aggregate aggregate = new Aggregate(this.Host, this.User, this.Passwd, this.Port);
                    aggregate.Run(TableTypes, times);
                    break;
                case "clean":
                    CleanUp cleanUp = new CleanUp(this.Host, this.User, this.Passwd, this.Port);
                    cleanUp.Run();
                    break;
                default:
                    throw new Exception($"unknown stage {options}");
            }
        }
    }
}