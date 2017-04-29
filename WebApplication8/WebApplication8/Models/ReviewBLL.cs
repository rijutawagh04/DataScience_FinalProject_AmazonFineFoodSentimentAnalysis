using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

using System.IO;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace WebApplication8.Models
{
    public class ReviewBLL
    {

        public Output getReviews(String productid, String text)
        {
            System.Diagnostics.Debug.Write("test");
            return InvokeReviews(productid, text);
        }

        protected Output InvokeReviews(String productid, String text)
        {
            using (var client = new HttpClient())
            {
                var scoreRequest = new
                {
                    Inputs = new Dictionary<string, List<Dictionary<string, string>>>() {
                        {
                            "input1",
                            new List<Dictionary<string, string>>(){new Dictionary<string, string>(){
                                            {
                                                "productId", productid
                                            },
                                            {
                                                "text", text
                                            },
                                }
                            }
                        },
                    },
                    GlobalParameters = new Dictionary<string, string>()
                    {
                    }
                };
                const string apiKey = "LGIxmEj5fAd/M9IWm8va0ZG5+kZCKIjv7gsjYWdWC8zBtxRVNIgW1sgrfeMFAKbTJo1dIiC2JPRfwPb72oVs7w=="; // Replace this with the API key for the web service
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
                client.BaseAddress = new Uri("https://ussouthcentral.services.azureml.net/workspaces/3951998e339c4eec9d0a7b39eeb2ded6/services/5a858901916f41309d755096772f85bc/execute?api-version=2.0&format=swagger");

                // WARNING: The 'await' statement below can result in a deadlock
                // if you are calling this code from the UI thread of an ASP.Net application.
                // One way to address this would be to call ConfigureAwait(false)
                // so that the execution does not attempt to resume on the original context.
                // For instance, replace code such as:
                //      result = await DoSomeTask()
                // with the following:
                //      result = await DoSomeTask().ConfigureAwait(false)

                HttpResponseMessage response = client.PostAsJsonAsync("", scoreRequest).Result;
                //string output = null;
                if (response.IsSuccessStatusCode)
                {
                    System.Diagnostics.Debug.Write("Sending Request");
                    var result = response.Content.ReadAsStringAsync().Result;
                    Result model = JsonConvert.DeserializeObject<Result>(result);
                    Output output = model.results.output[0];
                    return output;
                }
                else
                {
                    System.Diagnostics.Debug.Write("IN else");
                    System.Diagnostics.Debug.Write(string.Format("The request failed with status code: {0}", response.StatusCode));
                    // Print the headers - they include the requert ID and the timestamp,
                    // which are useful for debugging the failure
                    System.Diagnostics.Debug.Write(response.Headers.ToString());

                    string responseContent = response.Content.ReadAsStringAsync().Result;
                    System.Diagnostics.Debug.Write(responseContent);
                }
                return null;
            }
        }
    }
}