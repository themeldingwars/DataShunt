using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using FauFau.Formats;
using FauFau.Util.CommmonDataTypes;
using Npgsql;
using NpgsqlTypes;

namespace DataShunt
{
    public class SqlUtils
    {
        public static NpgsqlConnection Conn;

        public static void Init()
        {
            Conn = new NpgsqlConnection(Program.Config.SqlConnStr);
            Conn.Open();
        }

        public static void CreateSchema()
        {
            string    sql = $"CREATE SCHEMA IF NOT EXISTS {Program.Config.SqlNamespace}; SET search_path TO {Program.Config.SqlNamespace};";
            using var cmd = new NpgsqlCommand(sql, Conn);
            cmd.ExecuteNonQuery();
        }

        public static void CreateCreateSqls(List<SdbTableMapping.TableMapping> mapping, bool createTables = false)
        {
            var sb = new StringBuilder();

            foreach (var table in mapping) {
                var sql = CreateTableSql(table);
                //SaveSql($"Create_{table.Name}", sql);

                sb.AppendLine(sql);
                sb.AppendLine();

                /* if (createTables) {
                    try {
                        using var cmd = new NpgsqlCommand(sql, Conn);
                        cmd.ExecuteNonQuery();

                        Console.WriteLine($"Created table: {table.Name} ({table.Idx})");
                    }
                    catch (Exception e) {
                        Console.WriteLine(e);
                    }
                } */
            }

            if (createTables)
                try {
                    using var cmd = new NpgsqlCommand(sb.ToString(), Conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e) {
                    Console.WriteLine(e);
                }
        }

        public static string CreateTableSql(SdbTableMapping.TableMapping tableMapping)
        {
            var sb        = new StringBuilder();
            var tableName = Program.Config.UseNamesInTables ? tableMapping.Name : $"H_{tableMapping.Id:X}";
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS {Program.Config.SqlNamespace}.\"{tableName}\"");
            sb.AppendLine("(");

            for (var i = 0; i < tableMapping.Colums.Count; i++) {
                var col  = tableMapping.Colums[i];
                var name = Program.Config.UseNamesInTables ? col.Name : $"H_{col.Id:X}";
                sb.Append($"\"{name}\"".PadLeft(4));
                sb.Append(" ");
                sb.Append(GetSqlTypeName(col.SqlType));

                if (col.IsPrimaryKey)
                    sb.Append(" PRIMARY KEY");
                else if (!col.IsNullable) sb.Append(" NOT NULL");

                if (tableMapping.Colums.Count - 1 != i) sb.Append(",");

                sb.AppendLine();
            }

            sb.AppendLine(");");

            return sb.ToString();
        }

        public static void CreateTypes()
        {
            var sqls = new List<string>();

            sqls.Add("DROP TYPE IF EXISTS Box3;");
            sqls.Add("DROP TYPE IF EXISTS Matrix4x4;");
            sqls.Add("DROP TYPE IF EXISTS HalfMatrix4x3;");
            sqls.Add("DROP TYPE IF EXISTS Vector2;");
            sqls.Add("DROP TYPE IF EXISTS Vector3;");
            sqls.Add("DROP TYPE IF EXISTS Half3;");
            sqls.Add("DROP TYPE IF EXISTS Vector4;");

            // Vector2
            sqls.Add("CREATE TYPE Vector2 as (" +
                     "x real,"                  +
                     "y real);");

            // Vector3
            sqls.Add("CREATE TYPE Vector3 as (" +
                     "x real,"                  +
                     "y real,"                  +
                     "z real);");

            // Half3
            sqls.Add("CREATE TYPE Half3 as (" +
                     "x real,"                +
                     "y real,"                +
                     "z real);");

            // Vector4
            sqls.Add("CREATE TYPE Vector4 as (" +
                     "x real,"                  +
                     "y real,"                  +
                     "z real,"                  +
                     "w real);");

            // Matrix4x4
            sqls.Add("CREATE TYPE Matrix4x4 as (" +
                     "x vector4,"                 +
                     "y vector4,"                 +
                     "z vector4,"                 +
                     "w vector4);");

            // HalfMatrix4x3
            sqls.Add("CREATE TYPE HalfMatrix4x3 as (" +
                     "x half3,"                       +
                     "y half3,"                       +
                     "z half3,"                       +
                     "w half3);");

            // Box3
            sqls.Add("CREATE TYPE Box3 as (" +
                     "min Vector3,"          +
                     "max Vector3);");

            foreach (var sql in sqls)
                try {
                    //var       combined = String.Join(' ', sqls);
                    using var cmd = new NpgsqlCommand(sql, Conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e) {
                    Console.WriteLine(e);
                }

            // Map the types
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Vector2>("vector2");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Vector3>("vector3");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Vector4>("vector4");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Matrix4x4>("matrix4x4");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Half3>("half3");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<HalfMatrix4x3>("halfmatrix4x3");
            NpgsqlConnection.GlobalTypeMapper.MapComposite<Box3>("box3");

            Conn.ReloadTypes();
        }

        public static void CreateNameMappingViews(SdbUtils sdbutils)
        {
            var sqls = new List<string>();
            for (var index = 0; index < sdbutils.Mapping.Count; index++) {
                var table     = sdbutils.Mapping[index];
                var sb        = new StringBuilder();
                var tableName = Program.Config.UseNamesInTables ? table.Name : $"H_{table.Id:X}";
                var viewName = table.Name ?? $"H_{table.Id:X}";
                sb.AppendLine($"CREATE VIEW \"VM_{viewName}\" AS SELECT ");

                for (var i = 0; i < table.Colums.Count; i++) {
                    var colum     = table.Colums[i];
                    var columName = Program.Config.UseNamesInTables ? colum.Name : $"H_{colum.Id:X}";
                    sb.Append($"\"{columName}\"");

                    if (colum.Name != null) {
                        sb.Append(" as ");
                        sb.Append($"\"{colum.Name}\"");
                    }

                    if (i != table.Colums.Count - 1)
                        sb.AppendLine(", ");
                    else
                        sb.AppendLine();
                }

                sb.Append(" FROM ");
                sb.Append($"\"{tableName}\"");
                sb.AppendLine(";");

                var sqlStr = sb.ToString();
                sqls.Add(sqlStr);
            }

            foreach (var sql in sqls) {
                var cmd = new NpgsqlCommand(sql, Conn);
                cmd.ExecuteNonQuery();
            }
        }

        public static void DropAllInMapping(List<SdbTableMapping.TableMapping> tableMappings)
        {
            var sb = new StringBuilder();
            foreach (var tableMapping in tableMappings) {
                var name = Program.Config.UseNamesInTables ? tableMapping.Name : $"H_{tableMapping.Id:X}";
                var sql  = $"DROP TABLE IF EXISTS {Program.Config.SqlNamespace}.\"{name}\"; ";
                sb.Append(sql);
                //Console.WriteLine($"Droped table {name} ({tableMapping.Idx})");
            }

            using var cmd = new NpgsqlCommand(sb.ToString(), Conn);
            cmd.ExecuteNonQuery();
        }

        public static void DropAllViewsInMapping(List<SdbTableMapping.TableMapping> tableMappings)
        {
            var sb = new StringBuilder();
            foreach (var tableMapping in tableMappings) {
                var name = tableMapping.Name ?? $"H_{tableMapping.Id:X}";
                var sql  = $"DROP VIEW IF EXISTS {Program.Config.SqlNamespace}.\"VM_{name}\"; ";
                sb.Append(sql);
            }

            using var cmd = new NpgsqlCommand(sb.ToString(), Conn);
            cmd.ExecuteNonQuery();
        }

        public static void ImportData(SdbUtils sdbutils)
        {
            var sw             = Stopwatch.StartNew();
            var singleThreaded = false;
            sdbutils.LoopTables((table, tableIdx) =>
            {
                var mapping = sdbutils.Mapping.First(x => x.Id == table.Id);

                if (!singleThreaded) {
                    using var conn = new NpgsqlConnection(Program.Config.SqlConnStr);
                    conn.Open();

                    ImportDataToTable(mapping, table, conn);
                }
                else {
                    ImportDataToTable(mapping, table);
                }
            }, !singleThreaded);

            sw.Stop();
            Console.WriteLine($"Importing data for {sdbutils.Mapping.Count} took {sw.Elapsed}");
        }

        public static void ImportDataToTable(SdbTableMapping.TableMapping tableMapping, StaticDB.Table table, NpgsqlConnection conn = null)
        {
            if (conn == null) conn = Conn;
            var copySql            = CreateCoprySql(tableMapping);

            using (var writer = conn.BeginBinaryImport(copySql)) {
                for (var i = 0; i < table.Rows.Count; i++) {
                    var row = table.Rows[i];
                    writer.StartRow();
                    for (var index = 0; index < row.Fields.Count; index++) {
                        var field     = row.Fields[index];
                        var fieldinfo = tableMapping.Colums[index];

                        if (field == null) {
                            writer.WriteNull();
                        }
                        else if (IsBasicType(fieldinfo.SdbType)) {
                            if (fieldinfo.SdbType == StaticDB.DBType.UShort) {
                                writer.Write(Convert.ToInt32(field));
                            }
                            else if (fieldinfo.SdbType == StaticDB.DBType.UInt) {
                                writer.Write(Convert.ToInt64(field));
                            }
                            else if (fieldinfo.SdbType == StaticDB.DBType.ULong) {
                                var val = (long) (ulong) field;
                                writer.Write(val);
                            }
                            else if (fieldinfo.SdbType == StaticDB.DBType.Half) {
                                writer.Write(Convert.ToSingle(field));
                            }
                            else if (field is string fieldStr) { // clean nulls from the end of strings
                                var val = fieldStr.Replace("\0", "");
                                writer.Write(val);
                            }
                            else if (field is char fieldChar) {
                                if (fieldChar == '\0') { // feels abit eh, TODO: check incase of char trouble
                                    Console.WriteLine($"Got an invalid char {fieldChar} in table {tableMapping.Name} ({tableMapping.Idx}) on colum {fieldinfo.Name} row {i}");
                                    writer.Write(' ');
                                }
                                else {
                                    writer.Write(fieldChar);
                                }
                            }
                            else {
                                writer.Write(field);
                            }
                        }
                        else if (IsCustomType(fieldinfo.SdbType)) {
                            writer.Write(field);
                        }
                        else if (IsArrayType(fieldinfo.SdbType)) {
                            if (field is List<ushort> shortsList) {
                                var val = shortsList.Select(x => Convert.ToInt32(x)).ToList();
                                writer.Write(val);
                            }
                            else if (field is List<uint> intsList) {
                                var val = intsList.Select(x => Convert.ToInt64(x)).ToList();
                                writer.Write(val);
                            }
                            else {
                                writer.Write(field);
                            }
                        }
                        else {
                            Console.WriteLine("Unhandled type");
                            writer.WriteNull();
                        }
                    }
                }

                writer.Complete();
            }

            Console.WriteLine($"Imported data to table {tableMapping.Name} ({tableMapping.Idx})");
        }

        private static string CreateCoprySql(SdbTableMapping.TableMapping tableMapping)
        {
            var sb        = new StringBuilder();
            var tableName = Program.Config.UseNamesInTables ? tableMapping.Name : $"H_{tableMapping.Id:X}";
            sb.Append($"COPY {Program.Config.SqlNamespace}.\"{tableName}\" (");

            for (var i = 0; i < tableMapping.Colums.Count; i++) {
                var col  = tableMapping.Colums[i];
                var name = Program.Config.UseNamesInTables ? col.Name : $"H_{col.Id:X}";
                sb.Append($"\"{name}\"");

                if (tableMapping.Colums.Count - 1 != i) sb.Append(", ");
            }

            sb.Append(") FROM STDIN (FORMAT BINARY)");
            return sb.ToString();
        }

        public static string GetSqlTypeName(SqlTypes typ)
        {
            var typeName = typ switch
            {
                SqlTypes.INTEGER          => "integer",
                SqlTypes.INTEGER_ARRAY    => "integer[]",
                SqlTypes.SMALLINT         => "smallint",
                SqlTypes.SMALLINT_ARRAY   => "smallint[]",
                SqlTypes.BIGINT           => "bigint",
                SqlTypes.BIGINT_ARRAY     => "bigint[]",
                SqlTypes.VARCHAR          => "text",
                SqlTypes.REAL             => "real",
                SqlTypes.DOUBLE_PRECISION => "double precision",
                SqlTypes.CHARACTER        => "char",
                SqlTypes.BLOB             => "bytea",
                SqlTypes.VECTOR2          => "vector2",
                SqlTypes.VECTOR2_ARRAY    => "vector2[]",
                SqlTypes.VECTOR3          => "vector3",
                SqlTypes.VECTOR3_ARRAY    => "vector3[]",
                SqlTypes.VECTOR4          => "vector4",
                SqlTypes.VECTOR4_ARRAY    => "vector4[]",
                SqlTypes.MATRIX4_X4        => "matrix4x4",
                SqlTypes.HALFMATRIX4_X3    => "halfMatrix4x3",
                SqlTypes.BOX3             => "box3",
                _                         => "bytea"
            };

            return typeName;
        }

        public static bool IsBasicType(StaticDB.DBType dbType)
        {
            var result = dbType == StaticDB.DBType.Byte      ||
                         dbType == StaticDB.DBType.UShort    ||
                         dbType == StaticDB.DBType.UInt      ||
                         dbType == StaticDB.DBType.ULong     ||
                         dbType == StaticDB.DBType.SByte     ||
                         dbType == StaticDB.DBType.Short     ||
                         dbType == StaticDB.DBType.Int       ||
                         dbType == StaticDB.DBType.Long      ||
                         dbType == StaticDB.DBType.Float     ||
                         dbType == StaticDB.DBType.Double    ||
                         dbType == StaticDB.DBType.String    ||
                         dbType == StaticDB.DBType.AsciiChar ||
                         dbType == StaticDB.DBType.Half;

            return result;
        }

        public static bool IsArrayType(StaticDB.DBType dbType)
        {
            var result = dbType == StaticDB.DBType.Vector2Array ||
                         dbType == StaticDB.DBType.Vector3Array ||
                         dbType == StaticDB.DBType.Vector4Array ||
                         dbType == StaticDB.DBType.ByteArray    ||
                         dbType == StaticDB.DBType.UShortArray  ||
                         dbType == StaticDB.DBType.Blob         ||
                         dbType == StaticDB.DBType.UIntArray;

            return result;
        }

        public static bool IsCustomType(StaticDB.DBType dbType)
        {
            var result = dbType == StaticDB.DBType.Vector2       ||
                         dbType == StaticDB.DBType.Vector3       ||
                         dbType == StaticDB.DBType.Vector4       ||
                         dbType == StaticDB.DBType.Matrix4x4     ||
                         dbType == StaticDB.DBType.HalfMatrix4x3 ||
                         dbType == StaticDB.DBType.Box3;

            return result;
        }

        public static NpgsqlDbType GetNgpSqlTypeName(SqlTypes typ)
        {
            var ngpsqlType = typ switch
            {
                SqlTypes.INTEGER          => NpgsqlDbType.Integer,
                SqlTypes.INTEGER_ARRAY    => NpgsqlDbType.Integer | NpgsqlDbType.Array,
                SqlTypes.SMALLINT         => NpgsqlDbType.Smallint,
                SqlTypes.BIGINT           => NpgsqlDbType.Bigint,
                SqlTypes.VARCHAR          => NpgsqlDbType.Text,
                SqlTypes.REAL             => NpgsqlDbType.Real,
                SqlTypes.DOUBLE_PRECISION => NpgsqlDbType.Double,
                SqlTypes.CHARACTER        => NpgsqlDbType.Char,
                SqlTypes.BLOB             => NpgsqlDbType.Bytea,
                _                         => NpgsqlDbType.Bytea
            };

            return ngpsqlType;
        }

        public static void SaveSql(string name, string sql)
        {
            var path = Path.Combine(Program.Config.OutDir, "Sql");
            Directory.CreateDirectory(path);

            var fileName = name.Replace("::", "_");
            var filePath = Path.Combine(path, $"{fileName}.sql");
            File.WriteAllText(filePath, sql);
        }
    }
}