using System;
using System.Collections.Generic;

namespace ExchangeData
{
    public class JsonObject
    {
        public string op { get; set; }
        public string clk { get; set; }
        public long pt { get; set; }
        public List<Mc> mc { get; set; }

        public class Mc
        {
            public string id { get; set; }
            public MarketDefinition marketDefinition { get; set; }
            public List<Rc> rc { get; set; }
            public double tv { get; set; }
        }

        public class Rc
        {
            public List<List<double>> trd { get; set; }
            public List<List<double>> batb { get; set; }
            public List<List<double>> batl { get; set; }
            public double ltp { get; set; }
            public double tv { get; set; }
            public int id { get; set; }
        }

        public class MarketDefinition
        {
            public bool bspMarket { get; set; }
            public bool turnInPlayEnabled { get; set; }
            public bool persistenceEnabled { get; set; }
            public double marketBaseRate { get; set; }
            public string eventId { get; set; }
            public string eventTypeId { get; set; }
            public int numberOfWinners { get; set; }
            public string bettingType { get; set; }
            public string marketType { get; set; }
            public DateTime marketTime { get; set; }
            public DateTime suspendTime { get; set; }
            public bool bspReconciled { get; set; }
            public bool complete { get; set; }
            public bool inPlay { get; set; }
            public bool crossMatching { get; set; }
            public bool runnersVoidable { get; set; }
            public int numberOfActiveRunners { get; set; }
            public int betDelay { get; set; }
            public string status { get; set; }
            public List<Runner> runners { get; set; }
            public List<string> regulators { get; set; }
            public string venue { get; set; }
            public string countryCode { get; set; }
            public bool discountAllowed { get; set; }
            public string timezone { get; set; }
            public DateTime openDate { get; set; }
            public long version { get; set; }
            public string name { get; set; }
            public string eventName { get; set; }
        }

        public class Runner
        {
            public double adjustmentFactor { get; set; }
            public string status { get; set; }
            public int sortPriority { get; set; }
            public DateTime removalDate { get; set; }
            public double bsp { get; set; }
            public int id { get; set; }
            public string name { get; set; }
        }
    }
}
