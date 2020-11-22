using System.IO;
using Newtonsoft.Json;

namespace DataShunt
{
    public class Config
    {
        public uint[]   OnlyTheseTables  = null;
        public string   OutDir           = "output";
        public string[] PrimaryKeyNames  = {"id", "Id", "ID"};
        public string   SdbPath          = "clientdb.sd2";
        public string   SqlConnStr       = "";
        public string   SqlNamespace     = "sdb";
        public bool     UseNamesInTables = false;

        public static Config Load(string filepath)
        {
            if (!File.Exists(filepath)) {
                var newConfig = new Config();
                newConfig.Save(filepath);
            }

            var text   = File.ReadAllText(filepath);
            var config = JsonConvert.DeserializeObject<Config>(text);
            return config;
        }

        public void Save(string filepath)
        {
            var text = JsonConvert.SerializeObject(this, Formatting.Indented);
            File.WriteAllText(filepath, text);
        }
    }
}