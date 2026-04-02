using System.Collections.Generic;
using System.IO;

namespace PISimulator.PIPointTxtList
{
    class Program
    {
        static void Main(string[] args)
        {
            var piPointList = new List<string>();
            for (int i = 1; i < 90000; i++)
            {
                piPointList.Add($"Meter_{CurrentId(i)}_Current");
                piPointList.Add($"Meter_{CurrentId(i)}_Voltage");
            }
            File.WriteAllLines("Points.csv", piPointList);
        }

        private static string CurrentId(int i)
        {
            return (1000001 + i).ToString();
        }
    }
}
