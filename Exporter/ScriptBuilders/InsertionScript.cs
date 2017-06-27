using System;
using System.Collections.Generic;
using System.Text;
using DbQueries;
using Exporter.ScriptBuilders.CreationScripts;
using Npgsql;

namespace Exporter.ScriptBuilders
{
    class InsertionScript : CreationBuilderBase
    {
        public InsertionScript(NpgsqlConnection connection, string schema, string table) : base(connection, schema, table)
        {
        }

        public override void GetCreationScript(StringBuilder source)
        {
            var columns = GetTableColumns();
            var lines = GetInsertionList(columns);
            
            foreach (var line in lines)
            {
                source.AppendLine(line);
            }

            source.AppendLine();
            var alterSequenceStr = PostgresQueries.AlterSequence(Schema, $"{Table}_id_seq", lines.Length + 1);
            source.AppendLine(alterSequenceStr);
        }

        private string[] GetInsertionList(string[] columns)
        {
            var items = new List<string>();
            using (var dRead = GetReader(PostgresQueries.GetValuesByColumns(Schema, Table, columns)))
            {
                while (dRead.Read())
                {
                    var columnsValues = new List<string>();
                    for (int i = 0; i < dRead.FieldCount; i++)
                    {
                        var typeName = dRead[i].GetType().Name;
                        if (dRead[i].ToString() ==
                            "@model.JobStatusId == 0 || @model.JobStatusId == 1 || @model.JobStatusId == 5")
                        {
                            
                        }
                        switch (typeName)
                        {
                            case "DBNull":
                                columnsValues.Add("NULL");
                                break;
                            case "String":
                                var value = dRead[i].ToString();
                                var kovichka = value.IndexOf("'");
                                if (kovichka != -1)
                                {
                                    value = value.Insert(kovichka, "'");
                                }
                                columnsValues.Add($"'{value}'");
                                break;
                            default:
                                columnsValues.Add(dRead[i].ToString());
                                break;
                        }
                    }
                    
                    items.Add(PostgresQueries.InsertIntoLine(Schema,Table, columns, string.Join(", ", columnsValues)));
                }
            }

            return items.ToArray();
        }

        private string[] GetTableColumns()
        {
            var columns = new List<string>();
            using (var dRead = GetReader(PostgresQueries.GetAllTableColumns(Schema, Table)))
            {

                while (dRead.Read())
                {
                    columns.Add(dRead[ColumnName].ToString());
                }
            }
            return columns.ToArray();
        }
    }
}
