using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker;
using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Cortex
{
    public class Functions
    {
        [Function("BlobIngester")]
        public async Task BlobIngester(
            [BlobTrigger("%BLOB_CONTAINER_NAME%/resourceId=/SUBSCRIPTIONS/{subId}/RESOURCEGROUPS/{resourceGroup}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{nsgName}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/macAddress={mac}/PT1H.json", Connection = "SOURCE_ACCOUNT_CONNECTION_STRING")] BlockBlobClient item,
            string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, string blobMinute, string mac,
            FunctionContext context)
        {

            var logger = context.GetLogger("BlobIngester");
            var connectionString = Util.GetEnvironmentVariable("AzureWebJobsStorage");
            var serviceClient = new TableServiceClient(connectionString);
            // Get a reference to the TableClient from the service client instance.
            var tableClient = serviceClient.GetTableClient("checkpoints");

            logger.LogDebug("triggered");

            string sourceAccountConnectionString = Util.GetEnvironmentVariable("SOURCE_ACCOUNT_CONNECTION_STRING");
            if (sourceAccountConnectionString.Length == 0)
            {
                logger.LogError("Value for sourceAccountConnectionString is required.");
                throw new InvalidOperationException("Missing `SOURCE_ACCOUNT_CONNECTION_STRING` value");
            }

            string logsContainerName = Util.GetEnvironmentVariable("LOGS_CONTAINER_NAME");
            if (logsContainerName.Length == 0)
            {
                logger.LogError("Value for logsContainerName is required.");
                throw new InvalidOperationException("Missing `LOGS_CONTAINER_NAME` value");
            }

            var blobDetails = new BlobDetails(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour, blobMinute, mac);

            // get checkpoint
            var checkpoint = Checkpoint.GetCheckpoint(blobDetails, tableClient);
            var result = item.GetBlockListAsync(BlockListTypes.Committed).Result;
            var blocks = result.Value.CommittedBlocks;

            var startingByte = blocks.Where((item, index) => index < checkpoint.CheckpointIndex).Sum(item => item.SizeLong);
            var endingByte = blocks.Where((item, index) => index < result.Value.CommittedBlocks.Count()-1).Sum(item => item.SizeLong);
            var dataLength = endingByte - startingByte;

            logger.LogDebug($"Blob: {blobDetails}, starting byte: {startingByte}, ending byte: {endingByte}, number of bytes: {dataLength}");

            if (dataLength == 0)
            {
                logger.LogWarning($"Blob: {blobDetails}, triggered on completed hour.");
                return;
            }

            string nsgMessagesString = "";
            var bytePool = ArrayPool<byte>.Shared;
            byte[] nsgMessages = bytePool.Rent((int)dataLength);
            try
            {
                var options = new BlobDownloadOptions
                {
                    Range = new HttpRange(startingByte, dataLength)
                };

                BlobDownloadStreamingResult res = await item.DownloadStreamingAsync(options);
                using (var stream = new MemoryStream(nsgMessages)) {
                    res.Content.CopyTo(stream);    
                }

                res.Content.Close();

                if (nsgMessages[0] == ',')
                {
                    nsgMessagesString = System.Text.Encoding.UTF8.GetString(nsgMessages, 1, (int)(dataLength - 1));
                } else {
                    nsgMessagesString = System.Text.Encoding.UTF8.GetString(nsgMessages, 0, (int)dataLength);
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Error binding blob input: {ex.Message}");
                throw;
            }
            finally
            {
                bytePool.Return(nsgMessages);
            }

            try
            {
                var bytesSent = await Util.SendMessagesDownstreamAsync(nsgMessagesString, logger);
                logger.LogInformation($"Sending {nsgMessagesString.Length} bytes (denormalized to {bytesSent} bytes) downstream via output binding XDR.");
            }
            catch (Exception ex)
            {
                logger.LogError($"SendMessagesDownstreamAsync: Error {ex.Message}");
                throw;
            }

            checkpoint.PutCheckpoint(tableClient, blocks.Count()-1);
        }
    }
}
