using System;

namespace DbQueries
{
    public static class PostgresQueries
    {
        public static string DropTableByName(string schema, string tbName) => $"DROP TABLE \"{schema}\".{tbName};";

        public static string DropTrigger(string schema, string tbName, string name) => $"DROP TRIGGER {name} ON \"{schema}\".{tbName};";

        public static string DropFunction(string schema, string tbName, string name) => $"DROP FUNCTION \"{schema}\".{name}();";

        public static string AlterFuncToOwner(string schema, string name, string owner) => $"ALTER FUNCTION \"{schema}\".{name}() " + Environment.NewLine + $"OWNER TO \"{owner}\"; ";

        public static string GetIndexForTable(string schema, string table) => $"select * from pg_indexes where schemaname = '{schema}' and tablename = '{table}'";

        public static string GetPrimaryKey(string name) => $"select column_name from information_schema.key_column_usage where constraint_name = '{name}';";

        public static string FindSequencesForTable(string schema, string table) => $"SELECT sequence_name FROM information_schema.sequences  WHERE sequence_name LIKE '{schema}%' and sequence_schema = '{table}'; ";

        public static string GetAllTableColumns(string schema, string table) => $"select column_name, data_type, is_nullable from information_schema.columns where table_schema = '{schema}' and table_name = '{table}' ;";

        public static string GetValuesByColumns(string schema, string table, string[] fields ) => $"select {string.Join(", ", fields)} from \"{schema}\".{table}";

        public static string InsertIntoLine(string schema, string table, string[] to, string values) => $"INSERT INTO \"{schema}\".{table} ({string.Join(", ", to)}) VALUES ({values});";

        public static string GetOidOfFunc(string funcName) => $"select oid  from pg_proc where proname='{funcName}';";

        public static string GetFuncBody(string oid) => $"select pg_get_functiondef({oid});";

        public static string GetTriggerBody(string oid) => $"select pg_get_functiondef({oid});";

        public static string GetTriggerBody(string schema, string table, string name) => $"SELECT event_manipulation, action_statement FROM information_schema.triggers where trigger_name='{name}';";

        public static string AlterSequence(string schema, string name, int count) => $"ALTER SEQUENCE \"{schema}\".{name} RESTART WITH {count};";
    }
}