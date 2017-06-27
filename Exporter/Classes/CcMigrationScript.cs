using System;
using System.Text;

namespace Exporter.Classes
{
    public class CcMigrationScript
    {
        public StringBuilder _doc;
        public DbTable _table;
        public CcMigrationScript(DbTable table)
        {
            _table = table;
            _doc = new StringBuilder();
        }

        public void AppandStringBuilder(StringBuilder sb)
        {
            _doc.Append(sb);
        }
    }
}
