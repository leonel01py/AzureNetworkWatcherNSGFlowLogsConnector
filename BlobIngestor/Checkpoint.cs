using Azure;
using Azure.Data.Tables;
using System;


namespace Cortex
{
    public class Checkpoint : ITableEntity
    {
        public int CheckpointIndex { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }
        
        public ETag ETag { get; set; }

        public Checkpoint() {}

        public Checkpoint(string partitionKey, string rowKey, int index) {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            CheckpointIndex = index;
        }

        public static Checkpoint GetCheckpoint(BlobDetails blobDetails, TableClient tableClient)
        {
            var entity = tableClient.GetEntityIfExists<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey());

            Checkpoint checkpoint;
            if (entity.HasValue && entity.Value != null) {
                checkpoint = entity.Value;
            } else {
                checkpoint = new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), 1);
            }

            if (checkpoint != null && checkpoint.CheckpointIndex == 0)
            {
                checkpoint.CheckpointIndex = 1;
            }

            return checkpoint;
        }

        public void PutCheckpoint(TableClient tableClient, int index)
        {
            CheckpointIndex = index;
            
            tableClient.UpsertEntity(this);
        }
    }
}
