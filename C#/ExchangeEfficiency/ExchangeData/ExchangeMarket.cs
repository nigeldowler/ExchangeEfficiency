using System;
using System.Collections.Generic;
using System.Linq;

namespace ExchangeData
{
    public class ExchangeMarket
    {
        private List<JsonObject> JsonObjects { get; set; }
        private List<ExchangeMarketSelection> ExchangeSelections { get; set; }
        private Dictionary<long, Tuple<DateTime, double>> NonRunnerRemovalInfo { get; set; }

        public ExchangeMarket(List<JsonObject> jsonObjects)
        {
            this.JsonObjects = jsonObjects;
            this.ExchangeSelections = new List<ExchangeMarketSelection>();
            this.NonRunnerRemovalInfo = new Dictionary<long, Tuple<DateTime, double>>();
            ParseMarket();
        }

        private void ParseMarket()
        {
            // Get all market definition changes
            var mktDetails = JsonObjects.Where(j => j.mc.Any(m => m.marketDefinition != null)).OrderBy(j => j.pt).ToList();

            // Need to get accurate timestamps and reduction factors for when a non-runner was removed from a market
            // First grab all non-runner info, which will include duplicates
            var nonRunnersList = new List<KeyValuePair<long, Tuple<long, double>>>();
            foreach (var j in mktDetails)
            {
                foreach (var m in j.mc)
                {
                    foreach (var r in m.marketDefinition.runners)
                    {
                        if (r.removalDate > DateTime.MinValue)
                        {
                            nonRunnersList.Add(new KeyValuePair<long, Tuple<long, double>>(r.id, new Tuple<long, double>(j.pt, r.adjustmentFactor)));
                        }

                    }
                }
            }

            // Then convert to a dictionary with one entry per non-runner
            NonRunnerRemovalInfo = nonRunnersList
                .GroupBy(kvp => kvp.Key)
                .Select(grp => new { g = grp.OrderBy(x => x.Value.Item1).FirstOrDefault() })
                .ToDictionary(
                    k => k.g.Key,
                    v => new Tuple<DateTime, double>(
                        DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Math.Round(v.g.Value.Item1 / 1000.0, 0))).UtcDateTime,
                        v.g.Value.Item2 / 100.0
                    ));


            // All other details we can get from the last market definition at the off
            var mktDetail = mktDetails.Last();

            // Timestamp
            var offTime = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Math.Round(mktDetail.pt / 1000.0, 0))).UtcDateTime;

            // Extract the market definition details
            var mc = mktDetail.mc.Where(m => m.marketDefinition != null).First();

            // Market id
            var id = Convert.ToInt64(mc.id.Split('.')[1]);

            // Extract other details
            var mktDef = mc.marketDefinition;
            var eventId = Convert.ToInt64(mktDef.eventId);
            var countryCode = mktDef.countryCode;
            var venue = mktDef.venue;
            var marketTime = mktDef.marketTime;
            var marketName = mktDef.name;
            var numberOfActiveRunners = mktDef.numberOfActiveRunners;
            var status = mktDef.status;
            var runners = mktDef.runners;
            var numRunners = runners.Count;

            // Create a class for each selection
            foreach (var runner in runners)
            {
                var exchMktSel = new ExchangeMarketSelection()
                {
                    EventId = eventId,
                    MarketId = id,
                    MeetingDate = marketTime.Date,
                    DayOfWeek = marketTime.DayOfWeek == DayOfWeek.Sunday ? 7 : (int)marketTime.DayOfWeek,
                    CountryCode = countryCode,
                    Track = venue,
                    RaceTime = marketTime,
                    OffTime = offTime,
                    MarketName = marketName,
                    Entries = numRunners,
                    Runners = numberOfActiveRunners,
                    SelectionId = runner.id,
                    SelectionName = runner.name,
                    Bsp = runner.bsp
                };

                // Check if a non-runner
                if (runner.status == "REMOVED")
                {
                    exchMktSel.NonRunner = true;
                    exchMktSel.RemovalTime = NonRunnerRemovalInfo[runner.id].Item1;
                    exchMktSel.ReductionFactor = NonRunnerRemovalInfo[runner.id].Item2;
                }

                ExchangeSelections.Add(exchMktSel);
            }
        }

        public IEnumerable<string> GetCleanDataForCSV()
        {
            return ExchangeSelections.Select(e => e.ToString());
        }

        public IEnumerable<long> GetSelectionIds()
        {
            return ExchangeSelections.Select(e => e.SelectionId).OrderBy(s => s);
        }

        public Dictionary<long, string> GetSelectionIdsAndNames()
        {
            return ExchangeSelections.OrderBy(e => e.SelectionId).ToDictionary(k => k.SelectionId, v => v.SelectionName);
        }

        public Dictionary<long, Tuple<DateTime, double>> GetNonRunnerRemovalInfo()
        {
            return NonRunnerRemovalInfo;
        }
    }
}
