using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.Formatting;

namespace nsgFunc
{
    public partial class Util
    {
        public static async Task<int> obXDR(string newClientContent, ILogger log)
        {
            //
            // newClientContent looks like this:
            //
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //

            string xdrHost = Util.GetEnvironmentVariable("xdrHost");
            string xdrToken = Util.GetEnvironmentVariable("xdrToken");

            if (xdrHost.Length == 0 || xdrToken.Length == 0)
            {
                log.LogError("Invalid xdrHost and xdrToken are required.");
                return 0;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(delegate { return true; });

            int bytesSent = 0;

            foreach (var transmission in convertToXDRList(newClientContent, log))
            {
                var client = new SingleHttpClientInstance();
                try
                {
                    HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, xdrHost);
                    req.Headers.Accept.Clear();
                    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    req.Headers.Add("Authorization", $"Bearer {xdrToken}");
                    req.Content = new StringContent(transmission, Encoding.UTF8, "application/json");
                    HttpResponseMessage response = await SingleHttpClientInstance.SendToXDR(req);
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new System.Net.Http.HttpRequestException($"Non HTTP 200 status code received from XDR: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                    }
                }
                catch (System.Net.Http.HttpRequestException e)
                {
                    throw new System.Net.Http.HttpRequestException("Failed sending data to XDR", e);
                }
                catch (Exception f)
                {
                    throw new System.Exception("Failed sending data to XDR.", f);
                }
                bytesSent += transmission.Length;
            }

            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> convertToXDRList(string newClientContent, ILogger log)
        {
            foreach (var messageList in denormalizedRecords(newClientContent, null, log))
            {

                StringBuilder outgoingJson = StringBuilderPool.Allocate();
                outgoingJson.Capacity = MAXTRANSMISSIONSIZE;

                try
                {
                    foreach (var message in messageList)
                    {
                        var messageAsString = JsonConvert.SerializeObject(message, new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Ignore
                        });
                        outgoingJson.AppendLine(messageAsString);
                    }
                    yield return outgoingJson.ToString();
                }
                finally
                {
                    StringBuilderPool.Free(outgoingJson);
                }

            }
        }
    }
}
