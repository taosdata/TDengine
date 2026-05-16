using System;
using System.Diagnostics;
using System.IO;

namespace CopyPdb
{
    class Program
    {
        static void Main(string[] args)
        {
            string fName = "TDEngineDR.pdb";
            string sourceDir = args[0];
            string sourceFilePath = Path.Combine(sourceDir, fName);
            Console.WriteLine("SourceFilePath: " + sourceFilePath);
        
            FileVersionInfo myFileVersionInfo = FileVersionInfo.GetVersionInfo(sourceFilePath.Replace("pdb", "dll"));

            Console.WriteLine("File: " + myFileVersionInfo.FileDescription + '\n' + "Version number: " + myFileVersionInfo.FileVersion);
            string destinationDir = $"C:\\ProgramData\\OSIsoft\\AF\\PlugIns\\{myFileVersionInfo.FileVersion}\\4.0";

            if (!Directory.Exists(destinationDir))
            {
                Directory.CreateDirectory($"C:\\ProgramData\\OSIsoft\\AF\\PlugIns\\{myFileVersionInfo.FileVersion}");
                Directory.CreateDirectory(destinationDir);
            }
            string destinationFilePath = Path.Combine(destinationDir, fName);

            if (!File.Exists(destinationFilePath))
            {
                Console.WriteLine($"Copying file from {sourceFilePath} to {destinationFilePath}.");
                File.Copy(sourceFilePath, destinationFilePath);
            }
            else
            {
                Console.WriteLine($"File already exists");
            }
        }
    }
}
