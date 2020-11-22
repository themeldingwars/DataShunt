using System.Collections.Generic;
using System.IO;
using FauFau.Formats;
using Newtonsoft.Json;

namespace DataShunt
{
    public class SdbTableMapping
    {
        public const string DefaultName = "TableMapping.json";

        public static List<TableMapping> Load(string filePath = DefaultName)
        {
            var text      = File.ReadAllText(filePath);
            var tableData = JsonConvert.DeserializeObject<List<TableMapping>>(text);
            return tableData;
        }

        public static void Save(string filePath, List<TableMapping> tableMapping)
        {
            var jsonStr = JsonConvert.SerializeObject(tableMapping, Formatting.Indented);
            File.WriteAllText(filePath ?? DefaultName, jsonStr);
        }

        public class TableMapping
        {
            public List<ColumMapping> Colums = new();
            public string             DBName;
            public uint               Id;
            public int                Idx;
            public string             Name;
        }

        public class ColumMapping
        {
            public uint            Id;
            public int             Idx;
            public bool            IsNullable;
            public bool            IsPrimaryKey;
            public string          Name;
            public StaticDB.DBType SdbType;
            public SqlTypes        SqlType;
        }
    }
}