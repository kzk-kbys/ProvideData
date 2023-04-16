using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace DequeueEvent
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("evtqueue", Connection = "EventHubConnectionAppSetting")] EventData[] events, ILogger log, PartitionContext partitionContext)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string partitionId = partitionContext.PartitionId;
                    string partitionKey = eventData.PartitionKey;
                    string messageBody = Encoding.UTF8.GetString(eventData.EventBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    log.LogInformation($"Event Hub partition: {partitionId}");
                    log.LogInformation($"Partition context: {partitionKey}");

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
