using Npgsql;
using System.Text;

namespace Exporter.ScriptBuilders.CreationScripts
{
    interface ICreationBuilder
    {
        void GetCreationScript(StringBuilder source);
    }

    public abstract class CreationBuilderBase : ICreationBuilder
    {
        protected const string ColumnName = "column_name";
        protected NpgsqlDataReader GetReader(string q) => new NpgsqlCommand(q, Connection).ExecuteReader();
        protected readonly NpgsqlConnection Connection;
        protected readonly string Schema;
        protected readonly string Table;

        protected CreationBuilderBase(NpgsqlConnection connection, string schema, string table)
        {
            Connection = connection;
            Schema = schema;
            Table = table;
        }

        public abstract void GetCreationScript(StringBuilder source);
    }
}
