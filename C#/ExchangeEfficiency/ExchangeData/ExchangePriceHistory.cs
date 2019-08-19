using System;
using System.Collections.Generic;
using System.Linq;

namespace ExchangeData
{
    class ExchangePriceHistory
    {
        private List<JsonObject> JsonObjects;
        private ExchangeMarket ExchangeMarket;
        private List<ExchangeSnapshot> ExchangeSnapshots { get; set; }
        private List<ExchangeSnapshot> ExchangeSnapshotsFiltered { get; set; }
        private readonly double ReductionFactorThreshold = 0.025;

        public ExchangePriceHistory(List<JsonObject> jsonObjects, ExchangeMarket exchangeMarket)
        {
            this.JsonObjects = jsonObjects.Where(j => j.mc.Any(m => m.rc != null)).ToList();
            this.ExchangeMarket = exchangeMarket;
            this.ExchangeSnapshots = new List<ExchangeSnapshot>();
            this.ExchangeSnapshotsFiltered = new List<ExchangeSnapshot>();
            ParsePriceHistory();
        }

        private void ParsePriceHistory()
        {
            // Get all selection ids in the market
            var selectionIds = ExchangeMarket.GetSelectionIds();

            // Get a dictionary of selection ids => selection names
            var selectionNamesDict = ExchangeMarket.GetSelectionIdsAndNames();

            // Create dictionary of selection ids => array row index
            var selIndex = selectionIds.Select((s, i) => new { s, i }).ToDictionary(x => x.s, x => x.i);

            // Create a matrix of zeroes to store the latest state for each selection
            // Column 0 => Back3
            // Column 1 => Back3Vol
            // Column 2 => Back2
            // Column 3 => Back2Vol
            // Column 4 => Back
            // Column 5 => BackVol
            // Column 6 => Lay
            // Column 7 => LayVol
            // Column 8 => Lay2
            // Column 9 => Lay2Vol
            // Column 10 => Lay3
            // Column 11 => Lay3Vol
            // Column 12 => LastTradedPrice
            // Column 13 => CumulTradedVolSelection
            // Column 14 => CumulTradedVolMarket
            // Column 15 => BecomesNonRunner
            // Column 16 => IsNonRunner
            // Column 17 => ReductionFactorToApply - needed?
            var latestState = new double[selIndex.Count(), 18];
            for (int i = 0; i < latestState.GetLength(0); i++)
            {
                for (int j = 0; j < latestState.GetLength(1); j++)
                {
                    latestState[i, j] = 0;
                }
            }

            // Get the timestamps and reduction factors for when any non-runners were removed from the market
            var nonRunnerRemovalInfo = ExchangeMarket.GetNonRunnerRemovalInfo();

            // Initialise BecomesNonRunner
            foreach (var nr in nonRunnerRemovalInfo)
            {
                latestState[selIndex[nr.Key], 15] = 1;
            }

            // ReductionFactorToApply
            var cumulReductionFactors = new List<Tuple<DateTime, DateTime, double>>();
            if (nonRunnerRemovalInfo.Any())
            {
                // Get the market opening time
                var startTime = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Math.Round(Convert.ToDouble(JsonObjects.OrderBy(j => j.pt).First().pt) / 1000, 0))).UtcDateTime;

                // Get the distinct timestamps of non-runner removals, in case more than one NR at the same time
                var distinctNRTimes = nonRunnerRemovalInfo.Values.Select(v => v.Item1).Distinct().OrderBy(t => t);

                // At each non-runner removal time, calculate the cumulative reduction factor to that point
                foreach (var nrTime in distinctNRTimes)
                {
                    var cumulReducFac = 1 - nonRunnerRemovalInfo
                        .Where(n => n.Value.Item1 >= nrTime && n.Value.Item2 > ReductionFactorThreshold)
                        .Select(n => n.Value.Item2)
                        .Aggregate(1.0, (x, y) => x * (1 - y));
                    var tup = new Tuple<DateTime, DateTime, double>(startTime, nrTime, cumulReducFac);
                    cumulReductionFactors.Add(tup);
                    startTime = tup.Item2;
                }
            }

            // Find the actual off time
            var marketOffTime = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Math.Round(JsonObjects.OrderByDescending(j => j.pt).First().pt / 1000.0))).UtcDateTime;

            // Define a list of ints representing the timepoints (in seconds) to filter
            var filteredTimepoints = new List<int>();

            #region Definte snapshot timepoints
            for (int i = 86400; i > 21600; i -= 3600)
            {
                filteredTimepoints.Add(i);
            }
            for (int i = 21600; i > 10800; i -= 900)
            {
                filteredTimepoints.Add(i);
            }
            for (int i = 10800; i > 3600; i -= 300)
            {
                filteredTimepoints.Add(i);
            }
            for (int i = 3600; i > 900; i -= 10)
            {
                filteredTimepoints.Add(i);
            }
            for (int i = 900; i >= 0; i--)
            {
                filteredTimepoints.Add(i);
            }
            #endregion

            filteredTimepoints = filteredTimepoints.OrderByDescending(s => s).ToList();

            // Keep track of the latest snapshots
            var prevSnapshots = new Dictionary<long, ExchangeSnapshot>();

            // Loop through each line of json (except the last where it turns in-play)
            foreach (var obj in JsonObjects.OrderBy(j => j.pt).Take(JsonObjects.Count() - 1))
            {
                // Timestamp
                var snapshotTime = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Math.Round(Convert.ToDouble(obj.pt) / 1000, 0))).UtcDateTime;
                var secondsBeforeOff = Convert.ToInt32(Math.Round(marketOffTime.Subtract(snapshotTime).TotalSeconds));

                // Have we crossed over a required timepoint?
                while (filteredTimepoints.Any() && secondsBeforeOff < filteredTimepoints[0] && prevSnapshots != null)
                {
                    var nextFilteredTimepoint = filteredTimepoints[0];

                    // Ensure we didn't just get them in the last loop
                    if (prevSnapshots.All(s => s.Value.SecondsBeforeOff > nextFilteredTimepoint))
                    {
                        // Create new intermediate snapshots
                        var intermediateSnapshots = prevSnapshots;
                        foreach (var inter in intermediateSnapshots.ToList())
                        {
                            var updatedSnapshot = UpdateExchangeSnapshot(inter.Value, nextFilteredTimepoint);
                            ExchangeSnapshotsFiltered.Add(updatedSnapshot);
                            ExchangeSnapshots.Add(updatedSnapshot);
                            prevSnapshots[inter.Key] = updatedSnapshot;
                        }
                    }
                    filteredTimepoints.RemoveAt(0);
                }

                // Market id
                var mktId = Convert.ToInt64(obj.mc.Select(m => m.id).First().Split('.')[1]);

                // Traded volume on market
                var mktTv = 0.0;
                var mktsWithTv = obj.mc.Where(m => m.tv > 0);
                if (mktsWithTv.Any())
                    mktTv = mktsWithTv.OrderByDescending(m => m.tv).FirstOrDefault().tv;

                // Extract a list of the pricing details
                var rc = obj.mc.SelectMany(m => m.rc).ToList();

                // Loop through each selection and create an ExchangeSnapshot for each
                foreach (var selId in selectionIds)
                {
                    // Don't proceed for non-runners
                    if (latestState[selIndex[selId], 16] == 1)
                    {
                        continue;
                    }

                    // Check if selection has become a non-runner at this timepoint
                    if (nonRunnerRemovalInfo.ContainsKey(selId) && nonRunnerRemovalInfo[selId].Item1 <= snapshotTime)
                    {
                        latestState[selIndex[selId], 16] = 1;
                        prevSnapshots.Remove(selId);
                        continue;
                    }

                    // Start creating the Exchange Snapshot for this selection at this timepoint
                    var exchangeSnapshot = new ExchangeSnapshot()
                    {
                        MarketId = mktId,
                        MarketOffDatetime = marketOffTime,
                        SnapshotDatetime = snapshotTime,
                        SecondsBeforeOff = secondsBeforeOff,
                        SelectionId = selId,
                        SelectionName = selectionNamesDict[selId],
                        BecomesNonRunner = latestState[selIndex[selId], 15] == 1
                    };

                    // Find the corresponding rcSel if it exists
                    var rcSel = rc.Where(r => r.id == selId).LastOrDefault();

                    if (rcSel != null)
                    {
                        // Assign the back prices and volumes if any
                        if (rcSel.batb != null)
                        {
                            foreach (var back in rcSel.batb)
                            {
                                if (back[0] == 0)
                                {
                                    exchangeSnapshot.Back = back[1];
                                    exchangeSnapshot.BackVol = back[2];
                                    latestState[selIndex[rcSel.id], 4] = back[1];
                                    latestState[selIndex[rcSel.id], 5] = back[2];
                                }
                                if (back[0] == 1)
                                {
                                    exchangeSnapshot.Back2 = back[1];
                                    exchangeSnapshot.Back2Vol = back[2];
                                    latestState[selIndex[rcSel.id], 2] = back[1];
                                    latestState[selIndex[rcSel.id], 3] = back[2];
                                }
                                if (back[0] == 2)
                                {
                                    exchangeSnapshot.Back3 = back[1];
                                    exchangeSnapshot.Back3Vol = back[2];
                                    latestState[selIndex[rcSel.id], 0] = back[1];
                                    latestState[selIndex[rcSel.id], 1] = back[2];
                                }
                            }
                        }

                        // Assign the lay prices and volumes if any
                        if (rcSel.batl != null)
                        {
                            foreach (var lay in rcSel.batl)
                            {
                                if (lay[0] == 0)
                                {
                                    exchangeSnapshot.Lay = lay[1];
                                    exchangeSnapshot.LayVol = lay[2];
                                    latestState[selIndex[rcSel.id], 6] = lay[1];
                                    latestState[selIndex[rcSel.id], 7] = lay[2];
                                }
                                if (lay[0] == 1)
                                {
                                    exchangeSnapshot.Lay2 = lay[1];
                                    exchangeSnapshot.Lay2Vol = lay[2];
                                    latestState[selIndex[rcSel.id], 8] = lay[1];
                                    latestState[selIndex[rcSel.id], 9] = lay[2];
                                }
                                if (lay[0] == 2)
                                {
                                    exchangeSnapshot.Lay3 = lay[1];
                                    exchangeSnapshot.Lay3Vol = lay[2];
                                    latestState[selIndex[rcSel.id], 10] = lay[1];
                                    latestState[selIndex[rcSel.id], 11] = lay[2];
                                }
                            }
                        }

                        // Was there a trade on this selection at this timepoint?
                        // Will default to false anyway, so no need to store latest state
                        if (rcSel.trd != null)
                        {
                            exchangeSnapshot.Trade = true;
                        }

                        // Assign last traded price
                        if (rcSel.ltp > 0)
                        {
                            exchangeSnapshot.LastTradedPrice = rcSel.ltp;
                            latestState[selIndex[rcSel.id], 12] = rcSel.ltp;
                        }

                        // Cumulative traded volume on this selection
                        exchangeSnapshot.CumulTradedVolSelection = rcSel.tv;
                        latestState[selIndex[rcSel.id], 13] = rcSel.tv;
                    }

                    // Fill in blank prices from earlier price histories
                    if (exchangeSnapshot.Back == 0)
                    {
                        exchangeSnapshot.Back = latestState[selIndex[selId], 4];
                        exchangeSnapshot.BackVol = latestState[selIndex[selId], 5];
                    }
                    if (exchangeSnapshot.Back2 == 0)
                    {
                        exchangeSnapshot.Back2 = latestState[selIndex[selId], 2];
                        exchangeSnapshot.Back2Vol = latestState[selIndex[selId], 3];
                    }
                    if (exchangeSnapshot.Back3 == 0)
                    {
                        exchangeSnapshot.Back3 = latestState[selIndex[selId], 0];
                        exchangeSnapshot.Back3Vol = latestState[selIndex[selId], 1];
                    }
                    if (exchangeSnapshot.Lay == 0)
                    {
                        exchangeSnapshot.Lay = latestState[selIndex[selId], 6];
                        exchangeSnapshot.LayVol = latestState[selIndex[selId], 7];
                    }
                    if (exchangeSnapshot.Lay2 == 0)
                    {
                        exchangeSnapshot.Lay2 = latestState[selIndex[selId], 8];
                        exchangeSnapshot.Lay2Vol = latestState[selIndex[selId], 9];
                    }
                    if (exchangeSnapshot.Lay3 == 0)
                    {
                        exchangeSnapshot.Lay3 = latestState[selIndex[selId], 10];
                        exchangeSnapshot.Lay3Vol = latestState[selIndex[selId], 11];
                    }

                    // Fill in blanks for last traded price
                    if (exchangeSnapshot.LastTradedPrice == 0)
                    {
                        exchangeSnapshot.LastTradedPrice = latestState[selIndex[selId], 12];
                    }

                    // Fill in blank cumulative traded volume for each selection
                    if (exchangeSnapshot.CumulTradedVolSelection == 0)
                    {
                        exchangeSnapshot.CumulTradedVolSelection = latestState[selIndex[selId], 13];
                    }

                    // Cumulative traded volume on this market
                    if (mktTv > 0)
                    {
                        exchangeSnapshot.CumulTradedVolMarket = mktTv;
                        latestState[selIndex[selId], 14] = mktTv;
                    }
                    else
                    {
                        exchangeSnapshot.CumulTradedVolMarket = latestState[selIndex[selId], 14];
                    }

                    // Reduction factor to apply
                    if (latestState[selIndex[selId], 15] == 0)
                    {
                        foreach (var rf in cumulReductionFactors)
                        {
                            if (exchangeSnapshot.SnapshotDatetime >= rf.Item1 && exchangeSnapshot.SnapshotDatetime < rf.Item2)
                            {
                                exchangeSnapshot.ReductionFactorToApply = rf.Item3;
                                break;
                            }
                        }
                    }

                    // Midpoint
                    if (exchangeSnapshot.Back > 1 && exchangeSnapshot.Lay > 1)
                    {
                        //exchangeSnapshot.Midpoint = (exchangeSnapshot.Back + exchangeSnapshot.Lay) / 2;
                        // Midpoint of the probabilities
                        exchangeSnapshot.Midpoint = 2 * exchangeSnapshot.Back * exchangeSnapshot.Lay / (exchangeSnapshot.Back + exchangeSnapshot.Lay);

                        // Weighted average
                        if (exchangeSnapshot.BackVol + exchangeSnapshot.LayVol > 0)
                        {
                            var backVolRisked = exchangeSnapshot.BackVol * (exchangeSnapshot.Back - 1);
                            exchangeSnapshot.WeightedAverage =
                                ((exchangeSnapshot.Back * backVolRisked) + (exchangeSnapshot.Lay * exchangeSnapshot.LayVol))
                                / (backVolRisked + exchangeSnapshot.LayVol);
                        }
                    }

                    // Add this snapshot to our list
                    ExchangeSnapshots.Add(exchangeSnapshot);

                    // Add it to the filtered list if the snapshot is at an exact required timepoint
                    if (filteredTimepoints.Any() && exchangeSnapshot.SecondsBeforeOff == filteredTimepoints[0])
                    {
                        ExchangeSnapshotsFiltered.Add(exchangeSnapshot);
                    }

                    // Update the value for the latest snapshot
                    prevSnapshots[exchangeSnapshot.SelectionId] = exchangeSnapshot;
                }
            }

            // Are there any filtered timepoints left over?
            if (filteredTimepoints.Any() && filteredTimepoints.Min() < prevSnapshots.Values.Min(s => s.SecondsBeforeOff))
            {
                filteredTimepoints = filteredTimepoints.Where(t => t < prevSnapshots.Values.Min(s => s.SecondsBeforeOff)).ToList();

                // Fill them in
                foreach (var leftoverTimepoint in filteredTimepoints.ToList())
                {
                    foreach (var leftover in prevSnapshots)
                    {
                        var updatedSnapshot = UpdateExchangeSnapshot(leftover.Value, leftoverTimepoint);
                        ExchangeSnapshotsFiltered.Add(updatedSnapshot);
                        ExchangeSnapshots.Add(updatedSnapshot);
                    }
                }
            }

            // Add any details back to the market class here?
        }

        // A helper method to copy/update a ExchangeSnapshot with no reference to the old one
        private ExchangeSnapshot UpdateExchangeSnapshot(ExchangeSnapshot e, int s)
        {
            return new ExchangeSnapshot
            {
                MarketId = e.MarketId,
                MarketOffDatetime = e.MarketOffDatetime,
                SnapshotDatetime = e.MarketOffDatetime.AddSeconds(-s),
                SecondsBeforeOff = s,
                SelectionId = e.SelectionId,
                SelectionName = e.SelectionName,
                Back3 = e.Back3,
                Back3Vol = e.Back3Vol,
                Back2 = e.Back2,
                Back2Vol = e.Back2Vol,
                Back = e.Back,
                BackVol = e.BackVol,
                Lay = e.Lay,
                LayVol = e.LayVol,
                Lay2 = e.Lay2,
                Lay2Vol = e.Lay2Vol,
                Lay3 = e.Lay3,
                Lay3Vol = e.Lay3Vol,
                Trade = e.Trade,
                LastTradedPrice = e.LastTradedPrice,
                CumulTradedVolSelection = e.CumulTradedVolSelection,
                CumulTradedVolMarket = e.CumulTradedVolMarket,
                BecomesNonRunner = e.BecomesNonRunner,
                ReductionFactorToApply = e.ReductionFactorToApply,
                Midpoint = e.Midpoint,
                WeightedAverage = e.WeightedAverage
            };
        }

        public IEnumerable<string> GetCleanDataForCSV()
        {
            return ExchangeSnapshotsFiltered.Select(e => e.ToString());
        }

        public string GetCSVFilename()
        {
            if (!ExchangeSnapshots.Any())
                return "nodata.csv";

            var s = ExchangeSnapshots.First();
            return s.MarketOffDatetime.ToString("yyyyMMdd") + "_" + s.MarketId + ".csv";
        }
    }
}
