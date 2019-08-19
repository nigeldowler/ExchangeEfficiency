using System;

namespace ExchangeData
{
    public class ExchangeMarketSelection
    {
        public long EventId { get; set; }
        public long MarketId { get; set; }
        public DateTime MeetingDate { get; set; }
        public int DayOfWeek { get; set; }
        public string CountryCode { get; set; }
        public string Track { get; set; }
        public DateTime RaceTime { get; set; }
        public DateTime OffTime { get; set; }
        public string MarketName { get; set; }
        public int Entries { get; set; }
        public int Runners { get; set; }
        public long SelectionId { get; set; }
        public string SelectionName { get; set; }
        public double Bsp { get; set; }
        public bool NonRunner { get; set; }
        public DateTime RemovalTime { get; set; }
        public double ReductionFactor { get; set; }

        public ExchangeMarketSelection()
        {
            MeetingDate = DateTime.MinValue;
            RaceTime = DateTime.MinValue;
            OffTime = DateTime.MinValue;
            RemovalTime = DateTime.MinValue;
        }

        public override string ToString()
        {
            return EventId.ToString()
                + "," + MarketId.ToString()
                + "," + (MeetingDate > DateTime.MinValue ? MeetingDate.ToString("yyyy-MM-dd") : @"\N")
                + "," + DayOfWeek.ToString()
                + "," + CountryCode.ToString()
                + "," + Track.ToString()
                + "," + (RaceTime > DateTime.MinValue ? RaceTime.ToString("yyyy-MM-dd HH:mm:ss") : @"\N")
                + "," + (OffTime > DateTime.MinValue ? OffTime.ToString("yyyy-MM-dd HH:mm:ss") : @"\N")
                + "," + MarketName.ToString()
                + "," + Entries.ToString()
                + "," + Runners.ToString()
                + "," + SelectionId.ToString()
                + "," + SelectionName.ToString()
                + "," + (Bsp > 1 ? Bsp.ToString() : @"\N")
                + "," + (NonRunner ? "1" : "0")
                + "," + (RemovalTime > DateTime.MinValue ? RemovalTime.ToString("yyyy-MM-dd HH:mm:ss") : @"\N")
                + "," + (NonRunner ? ReductionFactor.ToString() : @"\N")
                ;
        }
    }
}
