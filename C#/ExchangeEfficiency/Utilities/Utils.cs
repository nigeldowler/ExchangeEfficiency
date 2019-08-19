using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace Utilities
{
    public static class Utils
    {
        // Simple logging method which writes to screen and to a file
        public static void Log(string msg, bool toFileOnly = false)
        {
            // Create directory if it doesn't exist
            var logDir = @"..\..\..\..\Logs";
            Directory.CreateDirectory(logDir);

            // Set the filename
            var logfile = Path.Combine(logDir,  "log_" + DateTime.Now.ToString("yyyy_MM_dd") + ".txt");

            // Write the message to the log file
            using (var sw = new StreamWriter(logfile, true))
            {
                var line = "[" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "] " + msg;
                sw.WriteLine(line);

                // Write the message to screen
                if (!toFileOnly)
                {
                    Console.WriteLine(line);
                }
            }
        }

        // Convert a list of strings to a CSV file
        public static void CreateCSV(IEnumerable<string> list, string filename)
        {
            using (var sw = new StreamWriter(filename, false))
            {
                foreach (var line in list)
                {
                    sw.WriteLine(line);
                }
            }
        }

        // Convert a DataTable to a CSV file suitable for loading to MySQL
        public static void CreateCSV(DataTable table, string filename, bool removeConsecutiveDuplicates = false)
        {
            using (var sw = new StreamWriter(filename, false))
            {
                var nCols = table.Columns.Count;
                var prevLine = String.Empty;

                foreach (DataRow row in table.Rows)
                {
                    var str = String.Empty;
                    for (var i = 0; i < nCols; i++)
                    {
                        // Convert NULLs to \N
                        str += (!Convert.IsDBNull(row[i]) ? row[i].ToString() : @"\N");
                        //str += (!Convert.IsDBNull(row[i]) ? row[i].ToString().Replace(",", @"\,") : @"\N"); // if you have to escape commas

                        if (i < nCols - 1)
                        {
                            str += ",";
                        }
                    }
                    if (str != prevLine)
                    {
                        sw.Write(str + sw.NewLine);
                        prevLine = str;
                    }
                }
            }
        }
    }
}
