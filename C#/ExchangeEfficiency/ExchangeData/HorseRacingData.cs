using System;
using System.Configuration;
using System.Data;
using System.IO;
using Utilities;

namespace ExchangeData
{
    public class HorseRacingData
    {
        // Local directories
        private static readonly string DataPath = @"C:\ExchangeEfficiency\Data\HorseRacing";
        private static readonly string Completed = "Completed";

        // Databases
        private static RedshiftDB Redshift = new RedshiftDB(ConfigurationManager.ConnectionStrings["redshift"].ConnectionString);
        private static MySQLDB MySQL = new MySQLDB(ConfigurationManager.ConnectionStrings["mysql"].ConnectionString);
        private static readonly string racesTableName = "horseracing_uki_selections_redshift_races";
        private static readonly string selectionsTableName = "horseracing_uki_selections_redshift_selections";

        // Set the date range
        private readonly DateTime StartDate;
        private readonly DateTime EndDate;

        public HorseRacingData(DateTime startDate, DateTime endDate)
        {
            Utils.Log("Starting HorseRacingData");

            // Set the date range
            StartDate = startDate;
            EndDate = endDate;

            // Create directories if they don't exist
            Directory.CreateDirectory(DataPath);
            Directory.CreateDirectory(Path.Combine(DataPath, Completed));

            // Run Redshift query to get horse racing data for given date period
            Utils.Log("Getting Horse Racing Data from Redshift for date range " + StartDate.ToShortDateString() + " to " + EndDate.ToShortDateString());
            var results = Redshift.GetHorseRacingData(StartDate, EndDate);

            if (results == null)
            {
                Utils.Log("Returned nothing");
                return;
            }
            Utils.Log("Returned " + results.Rows.Count.ToString("N0") + " rows");

            // Races
            ProcessRaceLevelData(results.Copy());

            // Selections
            ProcessSelectionLevelData(results.Copy());

            // Merge all data in the MySQL database
            MergeMySQLSelectionsData();

            // Insert the merged data into the selections table
            InsertToSelections();
        }

        private void ProcessRaceLevelData(DataTable dt)
        {
            Utils.Log("------------------------------------------------------------");
            Utils.Log("Processing race-level data");

            // Retain the selection-level details only
            dt.Columns.Remove("selection_id");
            dt.Columns.Remove("selection_name");
            dt.Columns.Remove("horse_age");
            dt.Columns.Remove("horse_sex");
            dt.Columns.Remove("weight_carried");
            dt.Columns.Remove("handicap_mark");
            dt.Columns.Remove("draw");
            dt.Columns.Remove("jockey");
            dt.Columns.Remove("trainer");
            dt.Columns.Remove("owner");
            dt.Columns.Remove("bsp");
            dt.Columns.Remove("sp");
            dt.Columns.Remove("fav_rank");
            dt.Columns.Remove("position");
            dt.Columns.Remove("inplay_min");
            dt.Columns.Remove("inplay_max");

            // Create a CSV file from the DataTable
            var csvFile = CreateCSVFromRedshiftData(dt, "redshift-horse-racing-data-races-");

            // Load the CSV file to MySQL
            LoadCSVToMySQL(csvFile, racesTableName);
        }

        private void ProcessSelectionLevelData(DataTable dt)
        {
            Utils.Log("------------------------------------------------------------");
            Utils.Log("Processing selection-level data");

            // Retain the race-level details only
            dt.Columns.Remove("race_title");
            dt.Columns.Remove("runners");
            dt.Columns.Remove("surface");
            dt.Columns.Remove("race_type");
            dt.Columns.Remove("going");
            dt.Columns.Remove("distance");
            dt.Columns.Remove("race_class");
            dt.Columns.Remove("race_code");
            dt.Columns.Remove("handicap");
            dt.Columns.Remove("age");
            dt.Columns.Remove("sex_limit");
            dt.Columns.Remove("prize_fund");
            dt.Columns.Remove("tv");

            // Create a CSV file from the DataTable
            var csvFile = CreateCSVFromRedshiftData(dt, "redshift-horse-racing-data-selections-");

            // Load the CSV file to MySQL
            LoadCSVToMySQL(csvFile, selectionsTableName);
        }

        private string CreateCSVFromRedshiftData(DataTable dt, string filename_prefix)
        {
            var filename = filename_prefix + StartDate.ToString("yyyyMMdd") + "-to-" + EndDate.ToString("yyyyMMdd") + ".csv";
            var csvFile = Path.Combine(DataPath, filename);
            Utils.CreateCSV(dt, csvFile, true);

            return csvFile;
        }

        private void LoadCSVToMySQL(string csvFile, string tableName)
        {
            var filename = Path.GetFileName(csvFile);
            Utils.Log("Loading " + filename + " to MySQL");
            if (MySQL.CheckConnection())
            {
                var n = MySQL.BulkInsert(csvFile, tableName);
                Utils.Log("Loaded " + n.ToString("N0") + " rows");

                // Move the CSV file to the Completed directory
                try
                {
                    File.Move(csvFile, Path.Combine(DataPath, Completed, filename));
                }
                catch (Exception ex)
                {
                    Utils.Log("Error trying to move file " + csvFile);
                    Utils.Log(ex.ToString());
                }
            }
        }

        private void MergeMySQLSelectionsData()
        {
            Utils.Log("------------------------------------------------------------");
            Utils.Log("Merging Exchange data and Redshift data");

            Utils.Log("Inserting Exchange stage data into Selections table");
            var rowsAffected = MySQL.InsertToSelectionsStage2(StartDate, EndDate);
            Utils.Log(rowsAffected.ToString("N0") + " rows inserted");

            Utils.Log("Updating Selections table with Redshift races data");
            rowsAffected = MySQL.UpdateSelectionsWithRedshiftRacesData(StartDate, EndDate);
            Utils.Log(rowsAffected.ToString("N0") + " rows updated");

            Utils.Log("Updating Selections table with Redshift selections data");
            rowsAffected = MySQL.UpdateSelectionsWithRedshiftSelectionsData(StartDate, EndDate);
            Utils.Log(rowsAffected.ToString("N0") + " rows updated");
        }

        private void InsertToSelections()
        {
            Utils.Log("------------------------------------------------------------");
            Utils.Log("Insert data from selections stage 2 to selections table");

            var rowsAffected = MySQL.InsertToSelectionsFromStage2(StartDate, EndDate);
            Utils.Log(rowsAffected.ToString("N0") + " rows inserted" + Environment.NewLine);
        }
    }
}
