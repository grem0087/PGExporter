using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using DbQueries;

namespace Exporter.ScriptBuilders.CreationScripts
{
    public class IndexCreationBuilder : CreationBuilderBase
    {
        private const string IdxNameColumn = "indexname";
        private const string IdxQueryColumn = "indexdef";

        private string CreateIndexFromTemplate(string name, string byField) => $"CREATE INDEX {name}" + Environment.NewLine +
                                                    $"ON \"{Schema}\".\"{Table}\"" + Environment.NewLine +
                                                    "USING btree" + Environment.NewLine +
                                                    $"({byField}); ";
        public IndexCreationBuilder(NpgsqlConnection connection, string schema, string table)
            : base(connection, schema, table)
        {            
        }


        public override void GetCreationScript(StringBuilder source)
        {
            using (var dRead = GetReader(PostgresQueries.GetIndexForTable(Schema, Table)))
            {
                while (dRead.Read())
                {
                    var value = dRead[IdxNameColumn].ToString();
                    if (value.IndexOf("idx_", StringComparison.Ordinal) != -1)
                    {
                        var name = value;
                        var regex = new Regex(@"\((.*)\)");

                        var match = regex.Match($"{dRead[IdxQueryColumn]}");
                        var byField = match.Groups[1].ToString();
                        source.AppendLine(CreateIndexFromTemplate(name, byField));
                        
                    }
                }
            }            
        }
    }
}
