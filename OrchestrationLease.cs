using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Ragc.Etl
{
    public class OrchestrationLease
    {   
        [JsonProperty(PropertyName = "id")]
        public string Id {get; set; } 
        public DateTime StartTime {get; set;}
        public DateTime EndTime {get; set;}
        public bool Succeeded {get; set;}
        public bool Locked {get; set;}
        public string Worker {get; set;}
        
        [JsonProperty(PropertyName = "_etag")]
        public string ETag {get; set;}
    }
}