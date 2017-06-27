using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exporter.Classes
{
    public class Config
    {
        public DbTable[] Tables { get; set; }
        public string Owner { get; set; }
        public string ConnectionString { get; set; }
        public string OutputFile { get; set; }
    }    
}
