using System;
using DbQueries;
using Exporter.Classes;
using Exporter.ScriptBuilders.CreationScripts;
using Npgsql;

namespace Exporter.ScriptBuilders
{
    public static class QueriesBuilder
    {
        public static CcMigrationScript InsertAllValues(this CcMigrationScript script, NpgsqlConnection connection)
        {
            var inserter = new InsertionScript(connection, script._table.Schema, script._table.Name);
            inserter.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateTrigger(this CcMigrationScript script, NpgsqlConnection connection)
        {
            var triggerCreator = new TriggerCreationBuilder(connection, script._table.Schema, script._table.Name);
            triggerCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateFunction(this CcMigrationScript script, NpgsqlConnection connection, string owner)
        {
            var funcCreator = new FunctionCreationBuilder(connection, owner, script._table.Schema, script._table.Name);
            funcCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateIndexForMasterTable(this CcMigrationScript script, NpgsqlConnection connection)
        {
            var idxCreator = new IndexCreationBuilder(connection, script._table.Schema, script._table.Name);
            idxCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateMasterTable(this CcMigrationScript script, NpgsqlConnection connection, string owner)
        {
            var tableCreator = new TableCreatonBuilder(connection, owner, script._table.Schema, script._table.Name);
            tableCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateIndexForHistoryTable(this CcMigrationScript script, NpgsqlConnection connection)
        {
            var idxCreator = new IndexCreationBuilder(connection, script._table.Schema, $"h_{script._table.Name}");
            idxCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript CreateHistoryTable(this CcMigrationScript script, NpgsqlConnection connection, string owner)
        {
            var tableCreator = new TableCreatonBuilder(connection, owner, script._table.Schema, $"h_{script._table.Name}");
            tableCreator.GetCreationScript(script._doc);
            script._doc.AppendLine();
            return script;
        }
        
        public static CcMigrationScript DropFunction(this CcMigrationScript script)
        {
            script._doc.AppendLine(PostgresQueries.DropFunction(script._table.Schema, script._table.Name, $"process_{script._table.Name}_audit"));
            script._doc.AppendLine();
            return script;
        }
        
        public static CcMigrationScript DropTrigger(this CcMigrationScript script)
        {
            script._doc.AppendLine(PostgresQueries.DropTrigger(script._table.Schema, script._table.Name, $"tr_history_{script._table.Name}"));
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript IfExistMasterTable(this CcMigrationScript script)
        {
            script._doc.AppendLine(IfExist(script._table.Schema, script._table.Name));
            script._doc.AppendLine();
            return script;
        }
        
        public static CcMigrationScript IfExistHistoryTable(this CcMigrationScript script)
        {
            script._doc.AppendLine(IfExist(script._table.Schema, "h_" + script._table.Name));
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript EndIf(this CcMigrationScript script)
        {
            script._doc.AppendLine(EndIf());
            script._doc.AppendLine();
            return script;
        }
        public static CcMigrationScript DropHistoryTable(this CcMigrationScript script)
        {
            script._doc.AppendLine(PostgresQueries.DropTableByName(script._table.Schema, "h_" + script._table.Name));
            script._doc.AppendLine();
            return script;
        }

        public static CcMigrationScript DropMasterTable(this CcMigrationScript script)
        {
            script._doc.AppendLine(PostgresQueries.DropTableByName(script._table.Schema, script._table.Name));
            script._doc.AppendLine();
            return script;
        }

        private static string IfExist(string schema, string tbName)
        {
            return "IF EXISTS (SELECT 1 " + Environment.NewLine +
                   "FROM information_schema.columns" + Environment.NewLine +
                   $"WHERE table_schema = '{schema}' and table_name = '{tbName}') THEN ";
        }

        private static string EndIf()
        {
            return "END IF;";
        }        
    }
}
