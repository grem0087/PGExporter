using System;
using System.Text;
using DbQueries;
using Npgsql;

namespace Exporter.ScriptBuilders.CreationScripts
{
    public class TableCreatonBuilder : CreationBuilderBase
    {
        private const string DataType = "data_type";
        private const string IsNull = "is_nullable";
        private const string SequenceName = "sequence_name";
        private readonly string Owner;

        private string TablePrimaryKeyConstraint(string fieldName) => $"CONSTRAINT {Table}_pkey PRIMARY KEY ({fieldName})";

        private string NullDefine(object value) => value.ToString().ToUpper() == "NO" ? "NOT NULL" : "NULL";

        public TableCreatonBuilder(NpgsqlConnection connection, string owner, string schema, string name)
            :base(connection, schema, name)
        {
            Owner = owner;
        }

        public override void GetCreationScript(StringBuilder source)
        {   
            var primaryKeyFieldName = GetPrimaryKeyField();
            var columnBuilder = new StringBuilder();
            using (var dRead = GetReader(PostgresQueries.GetAllTableColumns(Schema, Table)))
            {
                
                while (dRead.Read())
                {
                    var columName = dRead[ColumnName].ToString();
                    var columnType = dRead[DataType].ToString().ToUpper();

                    if (string.Equals(columName, primaryKeyFieldName, StringComparison.InvariantCulture))
                    {
                        columnType = columnType == "INTEGER" ? "SERIAL" : "BIGSERIAL";
                    }
                    columnBuilder.AppendLine($"{columName} {columnType} {NullDefine(dRead[IsNull])},");
                }
                if (!string.IsNullOrEmpty(primaryKeyFieldName))
                {
                    columnBuilder.AppendLine(TablePrimaryKeyConstraint(primaryKeyFieldName));
                }

                CreateTableFromTemplate(source, columnBuilder);                
            }
        }
        
        private string GetPrimaryKeyField()
        {
            using (var dRead = GetReader(PostgresQueries.GetPrimaryKey($"{Table}_pkey")))
            {
                dRead.Read();                
                return dRead[ColumnName].ToString();
            }
        }

        private void CreateTableFromTemplate(StringBuilder toBuilder, StringBuilder columnBuilder)
        {
            toBuilder.AppendLine($"CREATE TABLE \"{Schema}\".{Table}");
            toBuilder.AppendLine("(");
            toBuilder.Append(columnBuilder);
            toBuilder.AppendLine(")");
            toBuilder.AppendLine(@"WITH(" + Environment.NewLine + "OIDS = FALSE" + Environment.NewLine + "); ");
            toBuilder.AppendLine($"ALTER TABLE \"{Schema}\".{Table}");
            toBuilder.AppendLine($"OWNER TO \"{Owner}\";");
        }
    }
}
