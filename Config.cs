using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;

namespace DataShunt
{
    public class Config
    {
        public uint[]                     OnlyTheseTables   = null;
        public string                     OutDir            = "output";
        public string[]                   PrimaryKeyNames   = {"id", "Id", "ID"};
        public string                     SdbPath           = "clientdb.sd2";
        public string                     SqlConnStr        = "";
        public string                     SqlNamespace      = "sdb";
        public bool                       UseNamesInTables  = false;
        public Dictionary<uint, string>   NameOverridesDict = new();
        public Dictionary<string, string> NameOverrides     = new();

        public static Config Load(string filepath)
        {
            if (!File.Exists(filepath)) {
                var newConfig = new Config();
                newConfig.Save(filepath);
            }

            var text   = File.ReadAllText(filepath);
            var config = JsonConvert.DeserializeObject<Config>(text);

            foreach (var kvp in config.NameOverrides) {
                var hash = Convert.ToUInt32(kvp.Key, 16);
                config.NameOverridesDict.Add(hash, kvp.Value);
            }

            config.NameOverrides = new Dictionary<string, string>();

            return config;
        }

        public void Save(string filepath)
        {
            var text = JsonConvert.SerializeObject(this, Formatting.Indented);
            File.WriteAllText(filepath, text);
        }
    }
}