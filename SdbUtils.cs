using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Bitter;
using FauFau.Formats;
using FauFau.Util;

namespace DataShunt
{
    public class SdbUtils
    {
        public           Config                             Config;
        public           StaticDB                           DB;
        public           List<SdbTableMapping.TableMapping> Mapping;
        private readonly uint[]                             PrimaryKeyNames;
        public           List<uint>                         UncrackedNameHashes = new();

        public SdbUtils(Config config)
        {
            Config = config;

            Console.WriteLine("Loading sdb....");
            using var fs = new FileStream(Config.SdbPath, FileMode.Open);
            using var bs = new BinaryStream(fs);
            DB = new StaticDB();
            DB.Read(bs);
            Console.WriteLine("Sdb loaded");

            PrimaryKeyNames = new uint[config.PrimaryKeyNames.Length];
            for (var i = 0; i < config.PrimaryKeyNames.Length; i++) {
                var pkName     = config.PrimaryKeyNames[i];
                var pkNameHash = Checksum.FFnv32(pkName);
                PrimaryKeyNames[i] = pkNameHash;
            }

            var dirPath = Path.GetDirectoryName(Config.SdbPath);
            FieldNameLookup = BuildNameLookup(Path.Combine(dirPath, "fields.txt"));
            Console.WriteLine("Built field name mapping");

            Mapping = CreateTableMappingSchema();
            SdbTableMapping.Save(Path.Combine(Config.OutDir, "SdbTableMapping.json"), Mapping);
            Console.WriteLine("Exported table mapping");
        }

        public Dictionary<uint, string> FieldNameLookup { get; }

        public List<SdbTableMapping.TableMapping> CreateTableMappingSchema()
        {
            var mapping = new List<SdbTableMapping.TableMapping>();

            LoopTables((table, tableIdx) =>
            {
                var tbl = new SdbTableMapping.TableMapping
                {
                    Id     = table.Id,
                    Idx    = tableIdx,
                    Name   = GetFieldName(table.Id),
                    DBName = $"H_{table.Id:X}"
                };

                for (var colIdx = 0; colIdx < table.Columns.Count; colIdx++) {
                    var column = table.Columns[colIdx];
                    var col = new SdbTableMapping.ColumMapping
                    {
                        Id         = column.Id,
                        Idx        = colIdx,
                        Name       = GetFieldName(column.Id),
                        IsNullable = table.NullableColumn.Any(x => x.Id == column.Id),
                        SdbType    = column.Type,
                        SqlType    = SdbTypeToSql(column.Type)
                    };

                    //col.IsPrimaryKey = !col.IsNullable && PrimaryKeyNames.Contains(column.Id);
                    tbl.Colums.Add(col);
                }

                if (!tbl.Colums.Any(x => x.IsPrimaryKey)) {
                    // Console.WriteLine($"Table {tbl.Name} ({tbl.Id} {tbl.Idx}) didn't have a primary key :<");
                }

                mapping.Add(tbl);
            });

            return mapping;
        }

        public void LoopTables(Action<StaticDB.Table, int> action, bool parallel = false)
        {
            if (parallel)
                Parallel.For(0, DB.Tables.Count, i =>
                {
                    var table = DB.Tables[i];
                    if (Config.OnlyTheseTables == null || Config.OnlyTheseTables.Contains(table.Id)) action(table, i);
                });
            else
                for (var tableIdx = 0; tableIdx < DB.Tables.Count; tableIdx++) {
                    var table = DB.Tables[tableIdx];
                    if (Config.OnlyTheseTables != null && !Config.OnlyTheseTables.Contains(table.Id)) continue;

                    action(table, tableIdx);
                }
        }

        // this is kinda pointless now I guess
        // TODO remove, maybe
        public static SqlTypes SdbTypeToSql(StaticDB.DBType typ)
        {
            var sqlType = typ switch
            {
                StaticDB.DBType.Int           => SqlTypes.INTEGER,
                StaticDB.DBType.UInt          => SqlTypes.BIGINT,
                StaticDB.DBType.UIntArray     => SqlTypes.BIGINT_ARRAY,
                StaticDB.DBType.Short         => SqlTypes.SMALLINT,
                StaticDB.DBType.UShort        => SqlTypes.INTEGER,
                StaticDB.DBType.UShortArray   => SqlTypes.INTEGER_ARRAY,
                StaticDB.DBType.SByte         => SqlTypes.SMALLINT,
                StaticDB.DBType.Byte          => SqlTypes.SMALLINT,
                StaticDB.DBType.Long          => SqlTypes.BIGINT,
                StaticDB.DBType.ULong         => SqlTypes.BIGINT,
                StaticDB.DBType.Float         => SqlTypes.REAL,
                StaticDB.DBType.Half          => SqlTypes.REAL,
                StaticDB.DBType.Double        => SqlTypes.DOUBLE_PRECISION,
                StaticDB.DBType.String        => SqlTypes.VARCHAR,
                StaticDB.DBType.AsciiChar     => SqlTypes.CHARACTER,
                StaticDB.DBType.Vector2       => SqlTypes.VECTOR2,
                StaticDB.DBType.Vector2Array  => SqlTypes.VECTOR2_ARRAY,
                StaticDB.DBType.Vector3       => SqlTypes.VECTOR3,
                StaticDB.DBType.Vector3Array  => SqlTypes.VECTOR3_ARRAY,
                StaticDB.DBType.Vector4       => SqlTypes.VECTOR4,
                StaticDB.DBType.Vector4Array  => SqlTypes.VECTOR4_ARRAY,
                StaticDB.DBType.Matrix4x4     => SqlTypes.MATRIX4_X4,
                StaticDB.DBType.HalfMatrix4x3 => SqlTypes.HALFMATRIX4_X3,
                StaticDB.DBType.Box3          => SqlTypes.BOX3,
                StaticDB.DBType.ByteArray     => SqlTypes.BLOB,

                _ => SqlTypes.BLOB
            };

            return sqlType;
        }

        public Dictionary<uint, string> BuildNameLookup(string fieldsFile)
        {
            var lookup = new Dictionary<uint, string>();

            try {
                var lines = File.ReadAllLines(fieldsFile);
                foreach (var line in lines) {
                    var hash = Checksum.FFnv32(line);
                    lookup.Add(hash, line);
                }
            }
            catch (Exception e) {
                Console.WriteLine(e);
                throw;
            }

            return lookup;
        }

        public string GetFieldName(uint hash)
        {
            if (Program.Config.NameOverridesDict.TryGetValue(hash, out var value)) return value;
            if (FieldNameLookup.TryGetValue(hash, out var value2)) return value2;

            return $"H_{hash:X}";
        }
    }

    public enum SqlTypes
    {
        INTEGER,
        INTEGER_ARRAY,
        SMALLINT,
        SMALLINT_ARRAY,
        BIGINT,
        BIGINT_ARRAY,
        NUMERIC,
        DECIMAL,

        REAL,
        DOUBLE_PRECISION,
        FLOAT,
        DECFLOAT,

        BOOLEAN,
        CHARACTER,
        VARCHAR,

        DATE,

        BLOB,

        VECTOR2,
        VECTOR2_ARRAY,
        VECTOR3,
        VECTOR3_ARRAY,
        VECTOR4,
        VECTOR4_ARRAY,
        MATRIX4_X4,
        HALFMATRIX4_X3,
        BOX3
    }
}