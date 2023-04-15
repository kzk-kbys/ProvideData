// https://devblogs.microsoft.com/azure-sdk/eventhubs-clients/
// https://learn.microsoft.com/ja-jp/azure/event-hubs/event-hubs-dotnet-standard-getstarted-send?tabs=passwordless%2Croles-azure-portal
// https://learn.microsoft.com/ja-jp/dotnet/api/overview/azure/messaging.eventhubs-readme?view=azure-dotnet

// 特定のパーティションにキューする話
// https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/eventhub/Azure.Messaging.EventHubs/samples/Sample04_PublishingEvents.md#publishing-events-to-a-specific-partition

using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using System.Text;

// using Microsoft.Azure.EventHubs; // Legacy
using Azure.Messaging.EventHubs; // Latest
using Azure.Messaging.EventHubs.Producer;

namespace Company.Function
{
    public class HttpTriggerCs
    {
        private readonly ILogger _logger;
        private static string EHubConnStr = "XXXX";
        private static string EhubName = "yyyy";

        // private static EventHubClient EHubClient;
        private static EventHubProducerClient EHubProdClient = new EventHubProducerClient(EHubConnStr, EhubName);

        public HttpTriggerCs(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<HttpTriggerCs>();
        }

        [Function("HttpTriggerCs")]
        public async Task Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            string json_msg = "{'message': 'hello, world', 'version': 'latest'}";

            try
            {
                Console.WriteLine($"Sending message to EventHub: {json_msg}");

                // var conn_str_builder = new EventHubsConnectionStringBuilder(EHubConnStr)
                // {
                //     EntityPath = EhubName
                // };
                // EHubClient = EventHubClient.CreateFromConnectionString(conn_str_builder.ToString());
                // PartitionSender sender = EHubClient.CreatePartitionSender("0");
                // await sender.SendAsync(new EventData(Encoding.UTF8.GetBytes(json_msg)));

                // string firstPartition = (await EHubProdClient.GetPartitionIdsAsync()).First();
                // string partitionId = firstPartition;
                string partitionId = "1";                
                Console.WriteLine($"partation: {partitionId}"); // 送信先partition
                var batchOptions = new CreateBatchOptions{PartitionId = partitionId};
                using EventDataBatch eventBatch = await EHubProdClient.CreateBatchAsync(batchOptions);
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json_msg)));
                await EHubProdClient.SendAsync(eventBatch);

            }
            catch (Exception ex) 
            {
                Console.WriteLine($"Error: {ex.Message}\n{ex.StackTrace}");
            }            
            finally
            {
                // await EHubClient.CloseAsync();
                await EHubProdClient.DisposeAsync();
            }

        }
    }
}
