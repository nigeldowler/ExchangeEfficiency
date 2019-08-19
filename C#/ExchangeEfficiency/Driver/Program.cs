using ExchangeData;
using System;
using Utilities;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            // Print a menu for the user
            Console.WriteLine();
            Console.WriteLine("Make your choice:");
            Console.WriteLine();
            Console.WriteLine("[1] Exchange Data Parser - extract, unpack and parse json files from tar files, then load to MySQL");
            Console.WriteLine("[2] Horse Racing Data - get horse racing data from Redshift and load to MySQL");
            Console.WriteLine();

            // Read in the choice
            var choice = Console.ReadKey(true).KeyChar.ToString();

            switch (choice)
            {
                case "1":
                    // Do data parsing
                    var exchangeDataParser = new ExchangeDataParser();
                    break;

                case "2":
                    // Get horse racing data
                    var startDate = new DateTime(2016, 01, 01);
                    var endDate = new DateTime(2016, 01, 01);
                    var numberOfDays = 7;
                    for (var d = startDate; d < endDate; d = d.AddDays(numberOfDays))
                    {
                        var horseRacingData = new HorseRacingData(d, d.AddDays(numberOfDays));
                    }
                    break;

                default:
                    // Do nothing
                    break;
            }

            // Finished
            Utils.Log("Done" + Environment.NewLine);
            Console.ReadKey();
        }
    }
}
