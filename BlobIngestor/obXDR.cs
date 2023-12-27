using Microsoft.CodeAnalysis.Formatting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Cortex
{
    public partial class Util
    {
        public static async Task<int> ObXDR(string newClientContent, ILogger log)
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

            string xdrHost = GetEnvironmentVariable("XDR_HOST");
            string xdrToken = GetEnvironmentVariable("XDR_TOKEN");

            if (xdrHost.Length == 0 || xdrToken.Length == 0)
            {
                log.LogError("Invalid xdrHost and xdrToken are required.");
                return 0;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(delegate { return true; });

            int bytesSent = 0;

            foreach (var transmission in ConvertToXDRList(newClientContent, log))
            {
                var client = new SingleHttpClientInstance();
                try
                {
                    var req = new HttpRequestMessage(HttpMethod.Post, xdrHost);
                    req.Headers.Accept.Clear();
                    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    req.Headers.Add("Authorization", $"Bearer {xdrToken}");
                    req.Content = new StringContent(transmission, Encoding.UTF8, "application/json");

                    var response = await SingleHttpClientInstance.SendToXDR(req);
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new HttpRequestException($"Non HTTP 200 status code received from XDR: {response.StatusCode}, reason: {response.ReasonPhrase}");
                    }
                }
                catch (HttpRequestException e)
                {
                    throw new HttpRequestException("Failed sending data to XDR", e);
                }
                catch (Exception f)
                {
                    throw new Exception("Failed sending data to XDR.", f);
                }
                bytesSent += transmission.Length;
            }

            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> ConvertToXDRList(string newClientContent, ILogger log)
        {
            foreach (var messageList in DenormalizedRecords(newClientContent))
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
