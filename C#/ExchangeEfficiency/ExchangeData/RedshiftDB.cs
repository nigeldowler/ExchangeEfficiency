using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Reflection;
using Utilities;

namespace ExchangeData
{
    public class RedshiftDB
    {
        private readonly string ConnectionString;

        public RedshiftDB(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ApplicationException("No Redshift DB connection string found");

            this.ConnectionString = connectionString;
        }

        private DataTable ExecQuery(string querySql, IEnumerable<KeyValuePair<string, object>> parameters)
        {
            var output = new DataTable();

            using (var sqlCon = new OdbcConnection(this.ConnectionString))
            {
                using (var sqlDA = new OdbcDataAdapter(querySql, sqlCon))
                {
                    sqlDA.SelectCommand.CommandType = CommandType.Text;
                    sqlDA.SelectCommand.CommandTimeout = 600;
                    if (parameters != null)
                        foreach (var kvp in parameters)
                        {
                            var param = kvp.Key;
                            if (!param.StartsWith("@"))
                                param = param.Insert(0, "@");
                            sqlDA.SelectCommand.Parameters.AddWithValue(param, kvp.Value);
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

        // Used for a query which creates a temp table on Redshift, inserts data to it and then does other stuff
        private DataTable ExecQueryWithTempTable(string queryTemp, string querySelect, IEnumerable<KeyValuePair<string, object>> parameters)
        {
            var output = new DataTable();

            using (var sqlCon = new OdbcConnection(this.ConnectionString))
            {
                using (var sqlCmd = sqlCon.CreateCommand())
                {
                    sqlCmd.CommandType = CommandType.Text;
                    sqlCmd.CommandText = queryTemp;
                    sqlCon.Open();
                    try
                    {
                        sqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Utils.Log(ex.ToString());
                    }

                    using (var sqlDA = new OdbcDataAdapter(querySelect, sqlCon))
                    {
                        sqlDA.SelectCommand.CommandType = CommandType.Text;
                        sqlDA.SelectCommand.CommandTimeout = 600;
                        if (parameters != null)
                            foreach (var kvp in parameters)
                            {
                                var param = kvp.Key;
                                if (!param.StartsWith("@"))
                                    param = param.Insert(0, "@");
                                sqlDA.SelectCommand.Parameters.AddWithValue(param, kvp.Value);
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

                    sqlCon.Close();
                }

            }

            return output;
        }

        // Use this for testing only
        public DataTable RunQuery(string query, IEnumerable<KeyValuePair<string, object>> parameters)
        {
            return ExecQuery(query, parameters);
        }

        public DataTable GetHorseRacingData(DateTime startDate, DateTime endDate)
        {
            const string resource = "ExchangeData.Queries.redshift-horse-racing-data.sql";
            var resourceReader = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource);

            if (resourceReader == null)
            {
                Console.WriteLine("No resource found: " + resource);
                return null;
            }

            string query;
            using (var reader = new StreamReader(resourceReader))
            {
                query = reader.ReadToEnd();
            }

            if (String.IsNullOrWhiteSpace(query))
            {
                Console.WriteLine("Couldn't read any query");
                return null;
            }

            if (startDate < new DateTime(2000, 1, 1) || startDate > DateTime.Today)
            {
                Console.WriteLine("Invalid date");
                return null;
            }

            var parameters = new[]
            {
                new KeyValuePair<string, object>("startDate", startDate),
                new KeyValuePair<string, object>("endDate", endDate)
            };

            var results = ExecQuery(query, parameters);

            if (results == null || results.Rows.Count == 0)
            {
                Console.WriteLine("Nothing returned from query");
                return null;
            }

            return results;
        }

        // Old method
        public DataTable GetTimeformData(DateTime dt)
        {
            const string resource = "ExchangeEfficiency.Queries.redshift-racing-timeform.sql";
            var resourceReader = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource);

            if (resourceReader == null)
            {
                Console.WriteLine("No resource found: " + resource);
                return null;
            }

            string query;
            using (var reader = new StreamReader(resourceReader))
            {
                query = reader.ReadToEnd();
            }

            if (String.IsNullOrWhiteSpace(query))
            {
                Console.WriteLine("Couldn't read any query");
                return null;
            }

            if (dt < new DateTime(2000, 1, 1) || dt > DateTime.Today)
            {
                Console.WriteLine("Invalid date");
                return null;
            }

            var parameters = new[]
            {
                new KeyValuePair<string, object>("date", dt)
            };

            var results = ExecQuery(query, parameters);

            if (results == null || results.Rows.Count == 0)
            {
                Console.WriteLine("Nothing returned from query");
                return null;
            }

            return results;
        }

        // Old method
        public DataTable GetExchangeSnapshotData(long market_id, int[] seconds_before_off)
        {
            const string resource = "ExchangeEfficiency.Queries.redshift-racing-exchange-snapshots.sql";
            var resourceReader = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource);

            if (resourceReader == null)
            {
                Console.WriteLine("No resource found: " + resource);
                return null;
            }

            string query;
            using (var reader = new StreamReader(resourceReader))
            {
                query = reader.ReadToEnd();
            }

            if (String.IsNullOrWhiteSpace(query))
            {
                Console.WriteLine("Couldn't read any query");
                return null;
            }

            // Split the query into two blocks
            const string blockSeparator = "--END OF INSERT--";
            var n = query.LastIndexOf(blockSeparator, System.StringComparison.Ordinal);
            var queryInsert = string.Format(query.Substring(0, n), String.Concat("(", String.Join("),(", seconds_before_off), ")"));
            var querySelect = query.Substring(n + blockSeparator.Length);

            var parameters = new[]
            {
                new KeyValuePair<string, object>("market_id", market_id)
            };

            var results = ExecQueryWithTempTable(queryInsert, querySelect, parameters);

            if (results == null || results.Rows.Count == 0)
            {
                Utils.Log("Nothing returned from query");
                return null;
            }

            return results;
        }
    }
}
