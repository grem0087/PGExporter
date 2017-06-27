using System;
using System.Text;
using DbQueries;
using Npgsql;

namespace Exporter.ScriptBuilders.CreationScripts
{
    class FunctionCreationBuilder : CreationBuilderBase
    {
        private readonly string _owner;
        private const string OidColumn = "oid";
        private const string FuncBodyColumn = "pg_get_functiondef";

        public FunctionCreationBuilder(NpgsqlConnection connection, string owner, string schema, string table)
            : base(connection, schema, table)
        {
            _owner = owner;
        }

        public override void GetCreationScript(StringBuilder source)
        {
            string oid;
            using (var dRead = GetReader(PostgresQueries.GetOidOfFunc($"process_{Table}_audit")))
            {
                dRead.Read();
                oid = dRead[OidColumn].ToString();
            }

            var function = GetFunctionBody(oid);
            source.AppendLine($"{function};");
            source.AppendLine(PostgresQueries.AlterFuncToOwner(Schema, $"process_{Table}_audit", _owner));
        }

        private string GetFunctionBody(string oid)
        {
            using (var dRead = GetReader(PostgresQueries.GetFuncBody(oid)))
            {
                dRead.Read();
                return dRead[FuncBodyColumn].ToString();
            }
        }
    }
}