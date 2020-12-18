using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Ragc.Etl
{
    public class OrchestrationRun
    {   
        [JsonProperty(PropertyName = "id")]
        public Guid Id {get; set; } 
        public DateTime StartTime {get; set;}
        public DateTime EndTime {get; set;}
        public double Duration {get; set;} //seconds
        public bool ForcedLease {get; set;}
        public bool Succeeded {get; set;}
        public string OrchestrationWorker {get; set;}
        public string AdditionlInfo {get; set;}
    }
}