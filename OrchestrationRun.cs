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
        public bool ForcedLease {get; set;}
        public bool Succeeded {get; set;}
        public string Worker {get; set;}
        public string AdditionlInfo {get; set;}
    }
}