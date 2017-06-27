using System;
using System.Text;
using Npgsql;

namespace Exporter.ScriptBuilders.CreationScripts
{
    class TriggerCreationBuilder : CreationBuilderBase
    {
        public TriggerCreationBuilder(NpgsqlConnection connection, string schema, string table) : base(connection, schema, table)
        {
        }

        public override void GetCreationScript(StringBuilder source)
        {
            source.AppendLine(GetTriggerFromTemplate());
        }

        private string GetTriggerFromTemplate()
        {
            return $"CREATE TRIGGER tr_history_{Table}" + Environment.NewLine +
                   "AFTER INSERT OR UPDATE OR DELETE" + Environment.NewLine +
                   $"ON \"{Schema}\".{Table}" + Environment.NewLine +
                   "FOR EACH ROW" + Environment.NewLine +
                   $"EXECUTE PROCEDURE \"{Schema}\".process_{Table}_audit(); ";
        }
    }
}
