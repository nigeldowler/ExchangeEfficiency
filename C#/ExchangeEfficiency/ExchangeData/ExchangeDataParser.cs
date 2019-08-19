using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Utilities;

namespace ExchangeData
{
    public class ExchangeDataParser
    {
        // Define the directories to use - should only need to change DataPath
        private static readonly string DataPath = @"C:\ExchangeEfficiency\Data\2018";
        private static readonly string RawDataDir = "RawData";
        private static readonly string Bz2DataDir = "Bz2";
        private static readonly string JsonDataDir = "Json";
        private static readonly string TarCompletedDir = "Completed";
        private static readonly string CleanCSVsDir = "CSVs";
        private static readonly string SnapshotsCSVsDir = "Snapshots";
        private static readonly string SnapshotsCSVsUploadedDir = "Uploaded";
        private static readonly string MarketsCSVsDir = "Markets";
        private static readonly string MarketsCSVsUploadedDir = "Uploaded";

        // Database
        private static MySQLDB MySQL = new MySQLDB(ConfigurationManager.ConnectionStrings["mysql"].ConnectionString);
        private static readonly string snapshotsTableName = "horseracing_uki_exchange_snapshots";
        private static readonly string marketsTableName = "horseracing_uki_selections_stage";

        private static readonly object locker = new object();

        public ExchangeDataParser()
        {
            Utils.Log("Starting ExchangeDataParser");
            Utils.Log("Using base directory: " + DataPath);

            // Create directories if they don't exist
            Directory.CreateDirectory(Path.Combine(DataPath, RawDataDir, Bz2DataDir));
            Directory.CreateDirectory(Path.Combine(DataPath, RawDataDir, JsonDataDir));
            Directory.CreateDirectory(Path.Combine(DataPath, RawDataDir, TarCompletedDir));
            Directory.CreateDirectory(Path.Combine(DataPath, CleanCSVsDir));
            Directory.CreateDirectory(Path.Combine(DataPath, CleanCSVsDir, SnapshotsCSVsDir));
            Directory.CreateDirectory(Path.Combine(DataPath, CleanCSVsDir, SnapshotsCSVsDir, SnapshotsCSVsUploadedDir));
            Directory.CreateDirectory(Path.Combine(DataPath, CleanCSVsDir, MarketsCSVsDir));
            Directory.CreateDirectory(Path.Combine(DataPath, CleanCSVsDir, MarketsCSVsDir, MarketsCSVsUploadedDir));

            // Fetch all tar files from the raw data subdirectory
            var tarPath = Path.Combine(DataPath, RawDataDir);
            var tarFiles = GetTarFiles(tarPath);
            Utils.Log("Found " + tarFiles.Count + " tar file" + (tarFiles.Count != 1 ? "s" : ""));

            // Loop through each tar file and process its contents
            int i = 1;
            var n = tarFiles.Count();
            foreach (var tarFile in tarFiles)
            {
                Utils.Log("*** Processing tar file " + i + " of " + n + ": " + tarFile + " ***");
                ProcessTarFile(tarFile);
                i++;
            }
        }

        // The main method for processing a single tar file.
        // Extracts the bzip2 files from within, then unpacks those to get the json files.
        // Parses each json file to get the price history for each selection in the market.
        // Writes it neatly to CSV files and uploads them to a MySQL database.
        private static void ProcessTarFile(string tarFile)
        {
            // Define the paths to use for the bz2 files and the json files
            var bz2Path = Path.Combine(DataPath, RawDataDir, Bz2DataDir);
            var jsonPath = Path.Combine(DataPath, RawDataDir, JsonDataDir);

            // Extract the bz2 files from the tar file
            var bz2NumFiles = ExtractTar(tarFile, bz2Path);
            if (bz2NumFiles == 0)
                return;
            Utils.Log("Extracted " + bz2NumFiles.ToString("N0") + " bz2 file" + (bz2NumFiles != 1 ? "s" : ""));

            // Get the bz2 files and unpack them
            var bz2Files = GetBz2Files(bz2Path);
            var bz2Exceptions = new ConcurrentQueue<Exception>();
            Parallel.ForEach(bz2Files, bz2File =>
            {
                try
                {
                    UnpackBz2File(bz2File, jsonPath);
                }
                catch (Exception ex)
                {
                    bz2Exceptions.Enqueue(ex);
                }
                finally
                {
                    File.Delete(bz2File);
                }
            });

            // Log any exceptions
            if (bz2Exceptions.Any())
            {
                Utils.Log("Exceptions from queue:");
                foreach (var ex in bz2Exceptions)
                {
                    Utils.Log(ex.ToString() + Environment.NewLine);
                }
            }

            // Get the json files
            var jsonFiles = GetJsonFiles(jsonPath);
            var jsonNumFiles = jsonFiles.Count;
            Utils.Log("Unpacked " + jsonNumFiles.ToString("N0") + " json file" + (jsonNumFiles != 1 ? "s" : ""));

            // We will combine market-level details from many markets into one list for writing to CSV
            var exchangeMarketDataForCSV = new ConcurrentBag<string>();

            // Parse each json file
            var jsonCount = 0;
            var jsonExceptions = new ConcurrentQueue<Exception>();
            Parallel.ForEach(jsonFiles, jsonFile =>
            {
                try
                {
                    // Do the parsing
                    var mkt = ParseJson(jsonFile);

                    // Process the results
                    if (mkt != null)
                    {
                        foreach (var line in mkt.GetCleanDataForCSV())
                        {
                            exchangeMarketDataForCSV.Add(line);
                        }
                        Interlocked.Increment(ref jsonCount);
                        lock (locker)
                        {
                            Utils.Log("Parsed " + jsonFile + " (" + jsonCount + "/" + jsonNumFiles + ")");
                        }
                    }
                }
                catch (Exception ex)
                {
                    jsonExceptions.Enqueue(ex);
                }
                finally
                {
                    File.Delete(jsonFile);
                }
            });

            Utils.Log("Parsed " + jsonCount.ToString("N0") + " json file" + (jsonCount != 1 ? "s" : ""));

            // Sort the markets
            exchangeMarketDataForCSV.OrderBy(e => e);

            // Log any exceptions
            if (jsonExceptions.Any())
            {
                Utils.Log("Exceptions from queue:");
                foreach (var ex in jsonExceptions)
                {
                    Utils.Log(ex.ToString() + Environment.NewLine);
                }
            }

            // Load the exchange snapshots CSVs to MySQL
            var snapshotsCSVsPath = Path.Combine(DataPath, CleanCSVsDir, SnapshotsCSVsDir);
            var csvFiles = GetCSVFiles(snapshotsCSVsPath);
            if (csvFiles.Any())
            {
                Utils.Log("Loading " + csvFiles.Count.ToString("N0") + " csv file" + (csvFiles.Count != 1 ? "s" : "") + " from " + snapshotsCSVsPath + " ...");
                foreach (var snapshotCsvFile in csvFiles)
                {
                    // Load the CSV to a MySQL database
                    if (MySQL.CheckConnection())
                    {
                        var n = MySQL.BulkInsert(snapshotCsvFile, snapshotsTableName);
                        Utils.Log("Loaded " + snapshotCsvFile + " (" + n.ToString("N0") + " rows)");
                        try
                        {
                            File.Move(snapshotCsvFile, Path.Combine(snapshotsCSVsPath, SnapshotsCSVsUploadedDir, Path.GetFileName(snapshotCsvFile)));
                        }
                        catch (Exception ex)
                        {
                            Utils.Log("Error trying to move file " + snapshotCsvFile);
                            Utils.Log(ex.ToString());
                        }
                    }
                }
            }

            // Write all the market-level details to a CSV file
            var marketsFilename = "markets_" + Path.GetFileName(tarFile) + ".csv";
            var marketsCsvPath = Path.Combine(DataPath, CleanCSVsDir, MarketsCSVsDir);
            var marketsCsvFile = Path.Combine(marketsCsvPath, marketsFilename);
            Utils.CreateCSV(exchangeMarketDataForCSV, marketsCsvFile);

            // Load the markets CSV to MySQL
            Utils.Log("Loading " + marketsFilename + " ...");
            if (MySQL.CheckConnection())
            {
                var n = MySQL.BulkInsert(marketsCsvFile, marketsTableName);
                Utils.Log("Loaded " + marketsCsvFile + " (" + n.ToString("N0") + " rows)");
                try
                {
                    File.Move(marketsCsvFile, Path.Combine(marketsCsvPath, MarketsCSVsUploadedDir, marketsFilename));
                }
                catch (Exception ex)
                {
                    Utils.Log("Error trying to move file " + marketsCsvFile);
                    Utils.Log(ex.ToString());
                }
            }

            // Move the tar file to the Completed directory
            try
            {
                File.Move(tarFile, Path.Combine(Path.GetDirectoryName(tarFile), TarCompletedDir, Path.GetFileName(tarFile)));
            }
            catch (Exception ex)
            {
                Utils.Log("Error trying to move file " + tarFile);
                Utils.Log(ex.ToString());
            }
        }

        private static List<string> GetFiles(string path, string type)
        {
            return Directory.GetFiles(path, type).ToList();
        }

        private static List<string> GetTarFiles(string path)
        {
            return GetFiles(path, "*.tar");
        }

        private static List<string> GetBz2Files(string path)
        {
            return GetFiles(path, "*.bz2");
        }

        private static List<string> GetJsonFiles(string path)
        {
            return GetFiles(path, "1.*");
        }

        private static List<string> GetCSVFiles(string path)
        {
            return GetFiles(path, "*.csv");
        }

        private static int ExtractTar(string tarFile, string targetDir)
        {
            // Count up the files inside the tar
            var numFiles = 0;

            // Ensure the target directory exists
            Directory.CreateDirectory(targetDir);

            using (FileStream fsIn = new FileStream(tarFile, FileMode.Open, FileAccess.Read))
            {
                using (TarInputStream tarIn = new TarInputStream(fsIn))
                {
                    TarEntry tarEntry;
                    try
                    {
                        while ((tarEntry = tarIn.GetNextEntry()) != null)
                        {
                            if (tarEntry.IsDirectory)
                                continue;

                            string name = Path.GetFileName(tarEntry.Name);
                            string outName = Path.Combine(targetDir, name);

                            using (FileStream outStr = new FileStream(outName, FileMode.Create))
                            {
                                tarIn.CopyEntryContents(outStr);
                            }

                            numFiles++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Utils.Log("ERROR - Tar file possibly corrupted: " + tarFile);
                        Utils.Log(ex.ToString());

                        // Empty the target directory
                        var dirInfo = new DirectoryInfo(targetDir);
                        foreach (FileInfo file in dirInfo.EnumerateFiles())
                        {
                            file.Delete();
                        }
                        numFiles = 0;
                    }
                }
            }

            return numFiles;
        }

        private static void UnpackBz2File(string bz2File, string targetPath)
        {
            using (Stream inStream = File.OpenRead(bz2File))
            {
                var innerFile = Path.GetFileNameWithoutExtension(bz2File);
                using (Stream outStream = File.OpenWrite(Path.Combine(targetPath, innerFile)))
                {
                    BZip2.Decompress(inStream, outStream, true);
                }
            }
        }

        // Parse a single json file to get market details and price history details for each selection
        private static ExchangeMarket ParseJson(string jsonFile)
        {
            var jsonMkt = new List<JsonObject>();
            var n = 0;

            // This needs to be done in serial
            foreach (var line in File.ReadAllLines(jsonFile))
            {
                // Deserialize the json line into a class
                jsonMkt.Add(JsonConvert.DeserializeObject<JsonObject>(line));
                n++;

                // Only want data up to the race off time
                if (line.Contains("\"inPlay\":true"))
                    break;
            }

            // Check the market is ok
            var mktDetailLastDef = jsonMkt.Where(j => j.mc.Any(m => m.marketDefinition != null)).OrderBy(j => j.pt).Last().mc.First().marketDefinition;
            if (mktDetailLastDef.status == "CLOSED" && !mktDetailLastDef.inPlay && mktDetailLastDef.bspMarket && !mktDetailLastDef.bspReconciled)
            {
                lock (locker)
                {
                    Utils.Log(" - Something wrong in event id " + mktDetailLastDef.eventId + " (" + mktDetailLastDef.marketTime + "), possibly abandoned race, skipping");
                }
                return null;
            }

            // Get market information
            var exchangeMarket = new ExchangeMarket(jsonMkt);

            // Get price history
            var exchangePriceHistory = new ExchangePriceHistory(jsonMkt, exchangeMarket);

            // Write the price history to a CSV file
            var csvDataPriceHistory = exchangePriceHistory.GetCleanDataForCSV();
            var csvFile = Path.Combine(DataPath, CleanCSVsDir, SnapshotsCSVsDir, exchangePriceHistory.GetCSVFilename());
            Utils.CreateCSV(csvDataPriceHistory, csvFile);

            // Return the market-level details which will be combined with other markets and written to a CSV
            return exchangeMarket;
        }
    }
}
