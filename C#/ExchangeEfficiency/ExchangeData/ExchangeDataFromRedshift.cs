using System;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;

namespace ExchangeData
{
    class ExchangeDataFromRedshift
    {
        //private static readonly RedshiftDB Redshift = new RedshiftDB(ConfigurationManager.ConnectionStrings["redshift"].ConnectionString);
        //private static readonly MySQLDB MySQL = new MySQLDB(ConfigurationManager.ConnectionStrings["mysql"].ConnectionString);

        //public void LoadTimeformDataFromRedshiftToMySQL(DateTime dateStart, DateTime dateEnd)
        //{
        //    Log("LoadTimeformDataFromRedshiftToMySQL starting...");

        //    var totalRows = 0;

        //    for (var day = dateStart; day <= dateEnd; day = day.AddDays(1))
        //    {
        //        Log("Running query for " + day.ToString("yyyy-MM-dd") + "...");

        //        var results = Redshift.GetTimeformData(day);

        //        if (results != null)
        //        {
        //            PrintQueryResults(results);
        //            MySQL.BulkInsert(results, "horses");
        //            var nRows = results.Rows.Count;
        //            Log(" - Bulk loaded " + nRows.ToString("N0") + " rows to MySQL table for day " + day.ToString("yyyy-MM-dd"));
        //            totalRows += nRows;
        //        }
        //        else
        //        {
        //            Log(" - No results for this day");
        //        }
        //    }

        //    Log("LoadTimeformDataFromRedshiftToMySQL finished. " + totalRows.ToString("N0") + " processed.");
        //    Log("*** SUMMARY ***");
        //    Log("Start date: " + dateStart.ToString("yyyy-MM-dd"));
        //    Log("End date:   " + dateEnd.ToString("yyyy-MM-dd"));
        //    Log("Total rows: " + totalRows.ToString("N0"));
        //}

        //public void LoadExchangeSnapshotDataForManyMarkets(string tableName)
        //{
        //    Log("### Starting LoadExchangeSnapshotDataForManyMarkets ###");

        //    // Get all market_ids from existing markets
        //    var market_ids = MySQL.GetMarketIDs();
        //    if (market_ids == null || !market_ids.Any())
        //    {
        //        Log("No market_ids returned");
        //        Console.WriteLine("Press a key to finish");
        //        Console.ReadKey();
        //        return;
        //    }
        //    Log(market_ids.Count.ToString("N0") + " market_ids returned");

        //    // Get list of market_ids already processed and remove from first list
        //    var market_ids_done = MySQL.GetMarketIDsProcessed();
        //    var x = 0;
        //    foreach (var id in market_ids.ToList().Where(market_ids_done.Contains))
        //    {
        //        market_ids.Remove(id);
        //        x++;
        //    }
        //    Log(x.ToString("N0") + " market_ids already processed, so removed. " + market_ids.Count.ToString("N0") + " remaining");

        //    // Test on a few ids
        //    var market_ids_test = market_ids.OrderBy(m => m).Skip(42).Take(300);

        //    var n = 1;
        //    foreach (var market_id in market_ids_test)
        //    {
        //        Console.WriteLine(n);
        //        LoadExchangeSnapshotDataForMarket(market_id);
        //        n++;
        //    }

        //    Log("Done");
        //}

        //public void LoadExchangeSnapshotDataForMarket(long market_id)
        //{
        //    Log("Running Exchange snapshot query for market_id " + market_id + "...");

        //    // Define the snapshot timepoints as an array of number of seconds before the off
        //    #region seconds before off
        //    var seconds_before_off = new[]
        //    {
        //        172800,
        //        169200,
        //        165600,
        //        162000,
        //        158400,
        //        154800,
        //        151200,
        //        147600,
        //        144000,
        //        140400,
        //        136800,
        //        133200,
        //        129600,
        //        126000,
        //        122400,
        //        118800,
        //        115200,
        //        111600,
        //        108000,
        //        104400,
        //        100800,
        //        97200,
        //        93600,
        //        90000,
        //        86400,
        //        82800,
        //        79200,
        //        75600,
        //        72000,
        //        68400,
        //        64800,
        //        61200,
        //        57600,
        //        54000,
        //        50400,
        //        46800,
        //        43200,
        //        39600,
        //        36000,
        //        32400,
        //        28800,
        //        25200,
        //        21600,
        //        18000,
        //        14400,
        //        10800,
        //        9000,
        //        7200,
        //        5400,
        //        3600,
        //        2700,
        //        1800,
        //        1500,
        //        1200,
        //        900,
        //        840,
        //        780,
        //        720,
        //        660,
        //        600,
        //        570,
        //        540,
        //        510,
        //        480,
        //        450,
        //        420,
        //        390,
        //        360,
        //        330,
        //        300,
        //        270,
        //        240,
        //        210,
        //        180,
        //        170,
        //        160,
        //        150,
        //        140,
        //        130,
        //        120,
        //        110,
        //        100,
        //        90,
        //        80,
        //        70,
        //        60,
        //        50,
        //        40,
        //        30,
        //        20,
        //        10,
        //        0
        //    };
        //    #endregion

        //    var results = Redshift.GetExchangeSnapshotData(market_id, seconds_before_off);

        //    if (results != null)
        //    {
        //        //PrintQueryResults(results);
        //        var nRows = results.Rows.Count;
        //        //Log(nRows + " rows returned");

        //        // Write to CSV (for testing)
        //        //var filepath = "exch.csv";
        //        //WriteDataTableToCSV(results, filepath);

        //        // Write to MySQL table
        //        MySQL.BulkInsert(results, "exch_snapshots");
        //        Log("Bulk loaded " + nRows.ToString("N0") + " rows to MySQL table for market_id " + market_id);
        //    }
        //}

        //public void LoadHistoricExchangeData(string filename)
        //{
        //    Log("Running LoadHistoricExchangeData for file " + filename + "...");

        //    var folder = @"C:\ExchangeEfficiency\Data\Test\Clean_Data";

        //    // Write to MySQL table
        //    MySQL.BulkInsert(folder, filename, "price_history");
        //    Log("Bulk loaded " + filename + " to MySQL table");
        //}

        //public static void Log(string msg)
        //{
        //    using (var sw = new StreamWriter("log.txt", true))
        //    {
        //        var line = "[" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "] " + msg;
        //        sw.WriteLine(line);
        //        Console.WriteLine(line);
        //    }
        //}

        //public static void PrintQueryResults(DataTable results)
        //{
        //    if (results != null)
        //    {
        //        foreach (var row in results.AsEnumerable())
        //        {
        //            foreach (var field in row.ItemArray)
        //            {
        //                Console.Write(field + ", ");
        //            }
        //            Console.Write(Environment.NewLine);
        //        }
        //    }
        //    Console.WriteLine("Hit a key to continue...");
        //    Console.ReadKey();
        //}

        //public static void WriteDataTableToCSV(DataTable table, string filepath)
        //{
        //    var n = 0;

        //    using (var sw = new StreamWriter(filepath))
        //    {
        //        // Write the column names
        //        var header = String.Join(",", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray());
        //        if (!String.IsNullOrWhiteSpace(header))
        //            sw.WriteLine(header);

        //        // Write each row
        //        foreach (DataRow row in table.Rows)
        //        {
        //            var fields = row.ItemArray.Select(field => string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""));
        //            sw.WriteLine(string.Join(",", fields));
        //            n++;
        //        }
        //    }

        //    Console.WriteLine(n + " rows written to " + filepath);
        //}
    }
}
