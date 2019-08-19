using System;

namespace ExchangeData
{
    public class ExchangeSnapshot
    {
        public long MarketId { get; set; }
        public DateTime MarketOffDatetime { get; set; }
        public DateTime SnapshotDatetime { get; set; }
        public int SecondsBeforeOff { get; set; }
        public long SelectionId { get; set; }
        public string SelectionName { get; set; }
        public double Back3 { get; set; }
        public double Back3Vol { get; set; }
        public double Back2 { get; set; }
        public double Back2Vol { get; set; }
        public double Back { get; set; }
        public double BackVol { get; set; }
        public double Lay { get; set; }
        public double LayVol { get; set; }
        public double Lay2 { get; set; }
        public double Lay2Vol { get; set; }
        public double Lay3 { get; set; }
        public double Lay3Vol { get; set; }
        public bool Trade { get; set; }
        public double LastTradedPrice { get; set; }
        public double CumulTradedVolSelection { get; set; }
        public double CumulTradedVolMarket { get; set; }
        public bool BecomesNonRunner { get; set; }
        public double ReductionFactorToApply { get; set; }
        public double Midpoint { get; set; }
        public double WeightedAverage { get; set; }

        public ExchangeSnapshot()
        {
            MarketOffDatetime = DateTime.MinValue;
            SnapshotDatetime = DateTime.MinValue;
        }

        public override string ToString()
        {
            return MarketId.ToString()
                + "," + MarketOffDatetime.ToString("yyyy-MM-dd HH:mm:ss")
                + "," + SnapshotDatetime.ToString("yyyy-MM-dd HH:mm:ss")
                + "," + SecondsBeforeOff.ToString()
                + "," + SelectionId.ToString()
                + "," + SelectionName.ToString()
                + "," + (Back3 == 0 ? @"\N" : Back3.ToString())
                + "," + (Back3Vol == 0 ? @"\N" : Back3Vol.ToString())
                + "," + (Back2 == 0 ? @"\N" : Back2.ToString())
                + "," + (Back2Vol == 0 ? @"\N" : Back2Vol.ToString())
                + "," + (Back == 0 ? @"\N" : Back.ToString())
                + "," + (BackVol == 0 ? @"\N" : BackVol.ToString())
                + "," + (Lay == 0 ? @"\N" : Lay.ToString())
                + "," + (LayVol == 0 ? @"\N" : LayVol.ToString())
                + "," + (Lay2 == 0 ? @"\N" : Lay2.ToString())
                + "," + (Lay2Vol == 0 ? @"\N" : Lay2Vol.ToString())
                + "," + (Lay3 == 0 ? @"\N" : Lay3.ToString())
                + "," + (Lay3Vol == 0 ? @"\N" : Lay3Vol.ToString())
                + "," + (Trade ? "1" : "0")
                + "," + (LastTradedPrice == 0 ? @"\N" : LastTradedPrice.ToString())
                + "," + CumulTradedVolSelection.ToString()
                + "," + CumulTradedVolMarket.ToString()
                + "," + (BecomesNonRunner ? "1" : "0")
                + "," + (ReductionFactorToApply > 0 ? ReductionFactorToApply.ToString() : @"\N")
                + "," + (Midpoint == 0 ? @"\N" : Midpoint.ToString())
                + "," + (WeightedAverage == 0 ? @"\N" : WeightedAverage.ToString())
                ;
        }
    }
}
