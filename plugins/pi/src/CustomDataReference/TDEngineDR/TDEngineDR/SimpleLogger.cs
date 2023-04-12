using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;

namespace TDEngineDR
{
    public class SimpleLogger
    {
        private const string FILE_EXT = ".log";
        private readonly object fileLock = new object();
        private readonly PISystem piSystem;
        private readonly string datetimeFormat;
        private readonly string logFilePath;
        private DateTime lastEnableCheck = DateTime.MinValue;


        public bool Enabled { get; set; }

        /// <summary>
        /// Initiate an instance of SimpleLogger class constructor.
        /// If log file does not exist, it will be created automatically.
        /// </summary>
        public SimpleLogger(PISystem piSystem)
        {
            this.piSystem = piSystem;
            datetimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
            string logFilename = $"tdenginedr-{DateTime.Now.Date.ToString("yyyy-MM-dd")}" + FILE_EXT;
            string assemblyPath = System.Reflection.Assembly.GetAssembly(typeof(SimpleLogger)).Location;
            logFilePath = assemblyPath.Replace("TDEngineDR.dll", string.Empty) + logFilename;

            // Log file header line
            string logHeader = logFilename + " is created.";
            if (!System.IO.File.Exists(logFilePath))
            {
                WriteLine(System.DateTime.Now.ToString(datetimeFormat) + " " + logHeader);
            }
        }

        internal static void CreateDefaultInstance(PISystem piSystem)
        {
            instance = new SimpleLogger(piSystem);
        }

        public static SimpleLogger instance;

        public static SimpleLogger Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new SimpleLogger(null);
                }
                return instance;
            }
        }

        /// <summary>
        /// Log a DEBUG message
        /// </summary>
        /// <param name="text">Message</param>
        public void Debug(string text)
        {
            WriteFormattedLog(LogLevel.DEBUG, text);
        }

        /// <summary>
        /// Log an ERROR message
        /// </summary>
        /// <param name="text">Message</param>
        public void Error(string text)
        {
            WriteFormattedLog(LogLevel.ERROR, text);
        }

        /// <summary>
        /// Log a FATAL ERROR message
        /// </summary>
        /// <param name="text">Message</param>
        public void Fatal(string text)
        {
            WriteFormattedLog(LogLevel.FATAL, text);
        }

        /// <summary>
        /// Log an INFO message
        /// </summary>
        /// <param name="text">Message</param>
        public void Info(string text)
        {
            WriteFormattedLog(LogLevel.INFO, text);
        }

        /// <summary>
        /// Log a TRACE message
        /// </summary>
        /// <param name="text">Message</param>
        public void Trace(string text)
        {
            WriteFormattedLog(LogLevel.TRACE, text);
        }

        /// <summary>
        /// Log a WARNING message
        /// </summary>
        /// <param name="text">Message</param>
        public void Warning(string text)
        {
            WriteFormattedLog(LogLevel.WARNING, text);
        }

        private void WriteLine(string text, bool append = false)
        {
            try
            {
                CheckIfLogIsEnabled();
                if (!Enabled || string.IsNullOrEmpty(text))
                {
                    return;
                }
                lock (fileLock)
                {
                    using (System.IO.StreamWriter writer = new System.IO.StreamWriter(logFilePath, append, System.Text.Encoding.UTF8))
                    {
                        writer.WriteLine(text);
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        private void CheckIfLogIsEnabled()
        {
            double checkInterval = (DateTime.Now - lastEnableCheck).TotalSeconds;
            if (checkInterval < 60)
            {
                return;
            }
            bool enableLog = false;
            AFElement tdEngineElement = null;
            AFAttribute enableLogAttribute = null;
            AFDatabase dbConfig = piSystem.Databases["Configuration"];
            if (dbConfig != null)
            {
                tdEngineElement = dbConfig.Elements["TDengine"];
            }
            if (tdEngineElement != null)
            {
                enableLogAttribute = tdEngineElement.Attributes["EnableLog"];
            }
            if (enableLogAttribute != null)
            {
                enableLog = (bool)enableLogAttribute.GetValue().Value;
            }
            Enabled = enableLog;
            lastEnableCheck = DateTime.Now;
        }

        private void WriteFormattedLog(LogLevel level, string text)
        {
            string pretext;
            switch (level)
            {
                case LogLevel.TRACE:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [TRACE]   ";
                    break;
                case LogLevel.INFO:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [INFO]    ";
                    break;
                case LogLevel.DEBUG:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [DEBUG]   ";
                    break;
                case LogLevel.WARNING:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [WARNING] ";
                    break;
                case LogLevel.ERROR:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [ERROR]   ";
                    break;
                case LogLevel.FATAL:
                    pretext = System.DateTime.Now.ToString(datetimeFormat) + " [FATAL]   ";
                    break;
                default:
                    pretext = "";
                    break;
            }

            WriteLine(pretext + text, true);

        }

        [System.Flags]
        private enum LogLevel
        {
            TRACE,
            INFO,
            DEBUG,
            WARNING,
            ERROR,
            FATAL
        }
    }
}
