using System;
using System.Diagnostics;
using System.IO;

namespace DataShunt
{
    internal class Program
    {
        public static Config Config;
        public static string SettingsPath = "Config.json";

        private static void Main(string connStr = null, string dir = null)
        {
            if (dir != null) {
                Directory.SetCurrentDirectory(dir);
            }
            
            Config            = Config.Load(SettingsPath);
            Config.SqlConnStr = connStr ?? Config.SqlConnStr;

            Directory.CreateDirectory(Config.OutDir);

            var swTotalTime = Stopwatch.StartNew();
            var sw          = Stopwatch.StartNew();
            var sdbUtils    = new SdbUtils(Config);
            sw.Stop();
            Console.WriteLine($"Loaded sdb and built mapping in {sw.Elapsed}");

            var sw2 = Stopwatch.StartNew();
            SqlUtils.Init();
            SqlUtils.CreateSchema();
            SqlUtils.DropAllViewsInMapping(sdbUtils.Mapping);
            SqlUtils.DropAllInMapping(sdbUtils.Mapping);
            SqlUtils.CreateTypes();
            SqlUtils.CreateCreateSqls(sdbUtils.Mapping, true);

            SqlUtils.CreateNameMappingViews(sdbUtils);
            sw2.Stop();
            Console.WriteLine($"Created DB schema in {sw2.Elapsed}");

            SqlUtils.ImportData(sdbUtils);
            swTotalTime.Stop();
            Console.WriteLine($"All done in {swTotalTime.Elapsed}");
        }
    }
}