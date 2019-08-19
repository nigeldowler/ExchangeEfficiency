using MySql.Data.MySqlClient; // Bulk loading doesn't work with NuGet package v8.0.x
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Utilities;

namespace ExchangeData
{
    class MySQLDB
    {
        private readonly string ConnectionString;

        public MySQLDB(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ApplicationException("No MySQL database connection string supplied");

            this.ConnectionString = connectionString;
        }

        public bool CheckConnection()
        {
            bool isConnected = false;

            using (MySqlConnection conn = new MySqlConnection(ConnectionString))
            {
                try
                {
                    conn.Open();
                    isConnected = true;
                }
                catch (ArgumentException argEx)
                {
                    Utils.Log("ArgumentException:");
                    Utils.Log(argEx.Message);
                }
                catch (MySqlException ex)
                {
                    isConnected = false;
                    Utils.Log("MySqlException number " + ex.Number);
                    Utils.Log(ex.Message);
                }
            }
            return isConnected;
        }

        private DataTable ExecQuery(string query, IEnumerable<KeyValuePair<string, object>> parameters)
        {
            var output = new DataTable();

            using (var sqlCon = new MySqlConnection(this.ConnectionString))
            {
                using (var sqlDA = new MySqlDataAdapter(query, sqlCon))
                {
                    sqlDA.SelectCommand.CommandType = CommandType.Text;
                    sqlDA.SelectCommand.CommandTimeout = 300;
                    if (parameters != null)
                    {
                        foreach (var kvp in parameters)
                        {
                            var param = kvp.Key;
                            if (!param.StartsWith("@"))
                                param = param.Insert(0, "@");
                            sqlDA.SelectCommand.Parameters.AddWithValue(param, kvp.Value);
                        }
                    }
                    try
                    {
                        sqlDA.Fill(output);
                    }
                    catch (Exception ex)
                    {
                        Utils.Log(ex.ToString());
                    }
                }
            }

            return output;
        }

        private int ExecStoredProcedure(string storedProc, IEnumerable<KeyValuePair<string, object>> parameters)
        {
            var rowsAffected = 0;

            using (var sqlCon = new MySqlConnection(this.ConnectionString))
            {
                using (var sqlCmd = sqlCon.CreateCommand())
                {
                    sqlCmd.CommandType = CommandType.StoredProcedure;
                    sqlCmd.CommandText = storedProc;
                    sqlCmd.CommandTimeout = 3600;
                    if (parameters != null)
                    {
                        foreach (var kvp in parameters)
                        {
                            var param = kvp.Key;
                            if (!param.StartsWith("@"))
                                param = param.Insert(0, "@");
                            sqlCmd.Parameters.AddWithValue(param, kvp.Value);
                        }
                    }
                    try
                    {
                        sqlCon.Open();
                        rowsAffected = sqlCmd.ExecuteNonQuery();
                        sqlCon.Close();
                    }
                    catch (Exception ex)
                    {
                        Utils.Log(ex.ToString());
                    }
                }
            }

            return rowsAffected;
        }

        // Bulk insert a CSV file to MySQL
        public int BulkInsert(string csvFile, string tableName)
        {
            // Create the connection to MySQL
            using (var conn = new MySqlConnection(this.ConnectionString))
            {
                var bulk = new MySqlBulkLoader(conn)
                {
                    TableName = tableName,
                    FieldTerminator = ",",
                    EscapeCharacter = '\\',
                    LineTerminator = "\r\n",
                    FileName = csvFile,
                    NumberOfLinesToSkip = 0
                };

                // Load the data
                try
                {
                    return bulk.Load();
                }
                catch (Exception ex)
                {
                    Utils.Log("Error loading to MySQL:" + Environment.NewLine + ex);
                    return 0;
                }
            }
        }

        // Bulk insert a DataTable to MySQL
        public void BulkInsert(DataTable datatable, string tablename)
        {
            // Convert the data to a CSV file
            const string filename = "temp.csv";
            CreateCSVFile(datatable, filename);

            // Create the connection to MySQL
            var conn = new MySqlConnection(this.ConnectionString);
            var bulk = new MySqlBulkLoader(conn)
            {
                TableName = tablename,
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                EscapeCharacter = '\\',
                LineTerminator = "\r\n",
                FileName = filename,
                NumberOfLinesToSkip = 0
            };

            // Load the data
            try
            {
                bulk.Load();
            }
            catch (Exception ex)
            {
                Utils.Log("Error loading to MySQL:" + Environment.NewLine + ex);
            }

            // Delete the file
            try
            {
                File.Delete(filename);
            }
            catch (Exception ex)
            {
                Utils.Log("Error deleting file " + filename + Environment.NewLine + ex);
            }
        }

        // Convert a DataTable to a CSV file suitable for MySQL format
        public static void CreateCSVFile(DataTable table, string file)
        {
            var sw = new StreamWriter(file, false);
            var nCols = table.Columns.Count;

            foreach (DataRow drow in table.Rows)
            {
                for (var i = 0; i < nCols; i++)
                {
                    // Escape commas and convert NULLs to \N
                    sw.Write(!Convert.IsDBNull(drow[i]) ? drow[i].ToString().Replace(",", @"\,") : @"\N");

                    if (i < nCols - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }

            sw.Close();
            sw.Dispose();
        }

        public int InsertToSelectionsStage2(DateTime startDate, DateTime endDate)
        {
            return ExecStoredProcedure("horseracing_uki_selections_insert_to_stage_2", new[] {
                new KeyValuePair<string, object>("startDate", startDate),
                new KeyValuePair<string, object>("endDate", endDate) }
            );
        }

        public int UpdateSelectionsWithRedshiftRacesData(DateTime startDate, DateTime endDate)
        {
            return ExecStoredProcedure("horseracing_uki_selections_update_stage_2_races", new[] {
                new KeyValuePair<string, object>("startDate", startDate),
                new KeyValuePair<string, object>("endDate", endDate) }
            );
        }

        public int UpdateSelectionsWithRedshiftSelectionsData(DateTime startDate, DateTime endDate)
        {
            return ExecStoredProcedure("horseracing_uki_selections_update_stage_2_selections", new[] {
                new KeyValuePair<string, object>("startDate", startDate),
                new KeyValuePair<string, object>("endDate", endDate) }
            );
        }

        public int InsertToSelectionsFromStage2(DateTime startDate, DateTime endDate)
        {
            return ExecStoredProcedure("horseracing_uki_selections_insert", new[] {
                new KeyValuePair<string, object>("startDate", startDate),
                new KeyValuePair<string, object>("endDate", endDate) }
            );
        }

        //public List<long> GetMarketIDs()
        //{
        //    var query = "select distinct market_id from exchange.horses where market_id > 0 order by market_id";

        //    var results = ExecQuery(query, null);

        //    if (results == null || results.Rows.Count == 0)
        //        return null;

        //    return results.AsEnumerable().Select(r => r.Field<long>("market_id")).ToList();
        //}

        //public List<long> GetMarketIDsProcessed()
        //{
        //    var query = "select distinct market_id from exchange.exch_snapshots where market_id > 0 order by market_id";

        //    var results = ExecQuery(query, null);

        //    if (results == null || results.Rows.Count == 0)
        //        return null;

        //    return results.AsEnumerable().Select(r => r.Field<long>("market_id")).ToList();
        //}

        //// Used when you want to pass DataTables as parameters to a Stored Procedure
        //public DataTable ExecQueryWithStructuredParameter(string storedProc, KeyValuePair<string, object>[] parameters)
        //{
        //    var output = new DataTable();
        //    using (var sqlCon = new SqlConnection(this.connStr))
        //    {
        //        using (var sqlDA = new SqlDataAdapter(storedProc, sqlCon))
        //        {
        //            sqlDA.SelectCommand.CommandType = CommandType.StoredProcedure;
        //            sqlDA.SelectCommand.CommandTimeout = 300;
        //            if (parameters != null)
        //                foreach (var kvp in parameters)
        //                {
        //                    string param = kvp.Key;
        //                    if (!param.StartsWith("@"))
        //                        param = param.Insert(0, "@");
        //                    var paramList = new SqlParameter(param, SqlDbType.Structured)
        //                    {
        //                        TypeName = "dbo.IntegerTableType",
        //                        Value = kvp.Value
        //                    };
        //                    sqlDA.SelectCommand.Parameters.AddWithValue(param, kvp.Value);
        //                }
        //            sqlDA.Fill(output);
        //        }
        //    }
        //    return output;
        //}
    }
}
