using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebApplication8.Models
{
    public class Outputs {
        [JsonProperty("output1")]
        public List<Output> output;
    }

    public class Result
    {
        [JsonProperty("Results")]
        public Outputs results { get; set; }
    }

        public class Output
    {
        [JsonProperty("productId")]
        public string productId { get; set; }
        [JsonProperty("text")]
        public string text { get; set; }
        [JsonProperty("Scored Labels")]
        public Int32 SL { get; set; }
        [JsonProperty("Scored Probabilities")]
        public double SP { get; set; }

       // "{\"Results\":{\"output1\":[{\"productId\":\"A\",\"text\":\"good\",\"Scored Labels\":\"5\",\"Scored Probabilities\":\"0.836313366889954\"}]}}"
    }
}