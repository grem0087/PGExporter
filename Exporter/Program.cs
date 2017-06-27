using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Exporter.Classes;
using Exporter.ScriptBuilders;
using Newtonsoft.Json;
using Npgsql;

namespace Exporter
{
    class Program
    {
        private static Config GetConfigureObject(string fname) => JsonConvert.DeserializeObject<Config>( File.ReadAllText(fname) );

        static void Main(string[] args)
        {
            var config = GetConfigureObject(@".\config.json");
            var fileBegining = @"DO" + Environment.NewLine + "$$" + Environment.NewLine + "BEGIN" + Environment.NewLine;
            var EOF = @"END" + Environment.NewLine + "$$";

            using (var file = new StreamWriter(config.OutputFile))
            using (var connect = new NpgsqlConnection(config.ConnectionString))
            {
                file.Write(fileBegining);
                connect.Open();
                
                foreach (var table in config.Tables)
                {
                    Console.WriteLine($"{table.Schema}.{table.Name}...");
                    var doc = new CcMigrationScript(table);
                    if (table.HasHistory)
                    {
                        doc.IfExistHistoryTable()
                            .DropTrigger()
                            .DropFunction()
                            .DropHistoryTable()
                            .EndIf();                        
                    }

                    doc.IfExistMasterTable()
                        .DropMasterTable()
                        .EndIf()
                        .CreateMasterTable(connect, config.Owner)
                        .CreateIndexForMasterTable(connect);

                    if (table.HasHistory)
                    {
                        doc.CreateHistoryTable(connect, config.Owner)
                            .CreateIndexForHistoryTable(connect)
                            .CreateFunction(connect, config.Owner)
                            .CreateTrigger(connect);
                    }
                    doc.InsertAllValues(connect);
                    file.WriteLine(doc._doc);
                }
                file.Write(EOF);
            }

            Console.WriteLine("Success");
            Console.ReadLine();
        }
    }
}
