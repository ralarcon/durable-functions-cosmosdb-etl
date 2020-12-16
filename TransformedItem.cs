using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Ragc.Etl
{
    public class TransformedItem
    {
        [JsonProperty(PropertyName = "id")]
        public Guid Id {get; set;}
        public string SourceDate {get; set;}
        public DateTime TransformTimeStamp {get; set;}
        public string Description {get; set;}
        public string Done {get; set;}
        public string Name {get; set;}
        public string Pr {get; set;}
        public string AdditionalData {get; set;}
        public Guid TransformBatch {get; set;}
        public string UpdateLocation {get; set;}
    }
}