using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Ragc.Etl
{
    public class SampleItem
    {
        [JsonProperty(PropertyName = "id")]
        public Guid Id {get; set;}
        public string Date {get; set;}
        public string Desc {get; set;}
        public string Done {get; set;}
        public string Name {get; set;}
        public string Pr {get; set;}
        public string TransformData {get; set;}
        public Guid TransformBatch {get; set;}
        public string UpdateLocation {get; set;}
    }
}