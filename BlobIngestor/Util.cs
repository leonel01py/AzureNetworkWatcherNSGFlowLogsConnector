using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.Formatting;

namespace Cortex
{
    public partial class Util
    {
        const int MAXTRANSMISSIONSIZE = 512 * 1024;

        public static string GetEnvironmentVariable(string name)
        {
            var result = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
            if (result == null)
                return "";

            return result;
        }

        public static async Task<int> SendMessagesDownstreamAsync(string nsgMessagesString, ILogger log)
        {
            StringBuilder sb = StringBuilderPool.Allocate();
            string newClientContent = "";
            try
            {
                sb.Append("{\"records\":[").Append(nsgMessagesString).Append("]}");
                newClientContent = sb.ToString();
            } 
            finally
            {
                StringBuilderPool.Free(sb);
            }

            // string logIncomingJSON = GetEnvironmentVariable("LOG_INCOMING_JSON");
            // if (bool.TryParse(logIncomingJSON, out bool flag))
            // {
            //     if (flag)
            //     {
            //         logIncomingRecord(newClientContent, log).Wait();
            //     }
            // }

            return await ObXDR(newClientContent, log);
        }

        public class SingleHttpClientInstance
        {
            private static readonly HttpClient HttpClient;

            static SingleHttpClientInstance()
            {
                HttpClient = new HttpClient
                {
                    Timeout = new TimeSpan(0, 1, 0)
                };
            }


            public static async Task<HttpResponseMessage> SendToXDR(HttpRequestMessage req)
            {
                HttpResponseMessage response = await HttpClient.SendAsync(req);
                return response;
            }

        }

        static IEnumerable<List<DenormalizedRecord>> DenormalizedRecords(string newClientContent)
        {
            var outgoingList = ListPool<DenormalizedRecord>.Allocate();
            outgoingList.Capacity = 450;
            var sizeOfListItems = 0;

            try
            {
                NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

                foreach (var record in logs.Records)
                {
                    float version = record.Properties.Version;

                    foreach (var outerFlow in record.Properties.Flows)
                    {
                        foreach (var innerFlow in outerFlow.Flows)
                        {
                            foreach (var flowTuple in innerFlow.FlowTuples)
                            {
                                var tuple = new NSGFlowLogTuple(flowTuple, version);

                                var denormalizedRecord = new DenormalizedRecord(
                                    record.Properties.Version,
                                    record.Time,
                                    record.Category,
                                    record.OperationName,
                                    record.ResourceId,
                                    outerFlow.Rule,
                                    innerFlow.Mac,
                                    tuple);

                                var sizeOfDenormalizedRecord = denormalizedRecord.GetSizeOfJSONObject(); 
                                
                                yield return outgoingList;
                                outgoingList.Clear();
                                sizeOfListItems = 0;

                                outgoingList.Add(denormalizedRecord);
                                sizeOfListItems += sizeOfDenormalizedRecord;
                            }
                        }
                    }
                }
                if (sizeOfListItems > 0)
                {
                    yield return outgoingList;
                }
            }
            finally
            {
                ListPool<DenormalizedRecord>.Free(outgoingList);
            }
        }

        // public static async Task logIncomingRecord(string record, ILogger log)
        // {
        //     byte[] transmission = [];

        //     try
        //     {
        //         transmission = AppendToTransmission(transmission, record);

        //         Guid guid = Guid.NewGuid();
        //         var attributes = new Attribute[]
        //         {
        //             new BlobAttribute(String.Format("incomingrecord/{0}", guid)),
        //             new StorageAccountAttribute("cefLogAccount")
        //         };

        //         CloudBlockBlob blob = await binder.BindAsync<CloudBlockBlob>(attributes);
        //         await blob.UploadFromByteArrayAsync(transmission, 0, transmission.Length);

        //         transmission = new Byte[] { };
        //     }
        //     catch (Exception ex)
        //     {
        //         log.LogError($"Exception logging record: {ex.Message}");
        //     }
        // }

        public static byte[] AppendToTransmission(byte[] existingMessages, string appendMessage)
        {
            byte[] appendMessageBytes = Encoding.ASCII.GetBytes(appendMessage);
            byte[] crlf = "\r\n"u8.ToArray();

            byte[] newMessages = new Byte[existingMessages.Length + appendMessage.Length + 2];

            existingMessages.CopyTo(newMessages, 0);
            appendMessageBytes.CopyTo(newMessages, existingMessages.Length);
            crlf.CopyTo(newMessages, existingMessages.Length + appendMessageBytes.Length);

            return newMessages;
        }

        // typical use cases
        // , key: value ==> , "key": "value" --> if there's a comma, there's a colon
        // key: value ==> "key": "value" --> if there's no comma, there may be a colon
        static string eqs(string inString)
        {
            // eqs = Escape Quote String

            return "\"" + inString + "\"";
        }

        static string eqs(bool prependComma, string inString, bool appendColon)
        {
            var outString = String.Concat(prependComma ? "," : "", eqs(inString), appendColon ? ":" : "");

            return outString;
        }

        static string eqs(string inString, bool appendColon)
        {
            return eqs(false, inString, appendColon);
        }

        static string eqs(bool prependComma, string inString)
        {
            return eqs(prependComma, inString, true);
        }

        static string kvp(string key, string value)
        {
            return eqs(true, key) + eqs(value);
        }

        static string kvp(bool firstOne, string key, string value)
        {
            return eqs(key, true) + eqs(value);
        }

    }
}
