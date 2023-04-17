// https://devblogs.microsoft.com/azure-sdk/eventhubs-clients/
// https://learn.microsoft.com/ja-jp/azure/event-hubs/event-hubs-dotnet-standard-getstarted-send?tabs=passwordless%2Croles-azure-portal
// https://learn.microsoft.com/ja-jp/dotnet/api/overview/azure/messaging.eventhubs-readme?view=azure-dotnet

// 特定のパーティションにキューする話
// https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/eventhub/Azure.Messaging.EventHubs/samples/Sample04_PublishingEvents.md#publishing-events-to-a-specific-partition
// 可用性の観点でパーティション指定は推奨されない？
// https://learn.microsoft.com/ja-jp/azure/event-hubs/event-hubs-availability-and-consistency?tabs=dotnet
// 「イベント ハブ用に選択された特定のパーティション分割モデルは使用せずに…」
// https://learn.microsoft.com/ja-jp/azure/event-hubs/event-hubs-features


using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration; // 追加
using System.Text;

using System.Threading;
using System.Threading.Tasks;

// using Microsoft.Azure.EventHubs; // Legacy
using Azure.Messaging.EventHubs; // Latest
using Azure.Messaging.EventHubs.Producer;

namespace Company.Function
{
    public class HttpTriggerCs
    {
        private readonly ILogger _logger;
        private static string EHubConnStr;
        private static string EHubName;
        // private static EventHubClient EHubClient;
        private static EventHubProducerClient EHubProdClient;

        public HttpTriggerCs(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<HttpTriggerCs>();

            var builder = new ConfigurationBuilder()
                .AddJsonFile("local.settings.json", true)
                .AddEnvironmentVariables();
            var configuration = builder.Build();
            EHubConnStr = configuration["EventHubConnectionAppSetting"];
            EHubName = configuration["EventHubName"];
            EHubProdClient = new EventHubProducerClient(EHubConnStr, EHubName);
        }

        [Function("HttpTriggerCs")]
        public async Task Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            string msg_body = "{'message': 'hello, world'}";

            try
            {
                // Console.WriteLine($"Sending message to EventHub: {json_msg}");

                // var conn_str_builder = new EventHubsConnectionStringBuilder(EHubConnStr)
                // {
                //     EntityPath = EhubName
                // };
                // EHubClient = EventHubClient.CreateFromConnectionString(conn_str_builder.ToString());
                // PartitionSender sender = EHubClient.CreatePartitionSender("0");
                // await sender.SendAsync(new EventData(Encoding.UTF8.GetBytes(json_msg)));
                
                // 割り当てるpartitionをIDで指定する場合
                // string firstPartition = (await EHubProdClient.GetPartitionIdsAsync()).First();
                // string partitionId = firstPartition;
                // string partitionId = "1";                
                // Console.WriteLine($"partation: {partitionId}"); 
                // var batchOptions = new CreateBatchOptions{PartitionId = partitionId};

                // キーで指定する場合
                // string partitionKey = "pkey_1";                
                // Console.WriteLine($"partation: {partitionKey}"); 
                // var batchOptions = new CreateBatchOptions{PartitionKey = partitionKey};

                // using EventDataBatch eventBatch = await EHubProdClient.CreateBatchAsync(batchOptions);
                // eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json_msg)));
                // await EHubProdClient.SendAsync(eventBatch);

                // for(int i = 1; i < 3; i++){
                //     string partitionKey = $"pkey_{i}";                
                //     Console.WriteLine($"partation: {partitionKey}"); 
                //     var batchOptions = new CreateBatchOptions{PartitionKey = partitionKey};
                //     using EventDataBatch eventBatch = await EHubProdClient.CreateBatchAsync(batchOptions);
                //     eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json_msg)));
                //     await EHubProdClient.SendAsync(eventBatch);
                // }

                // Parallel.For(1, 6, i => SleepTask(i));
                // await Task.Run(() => SendEvent(1, msg_body));
                // await Task.Run(() => SendEvent(2, msg_body));
                // Parallel.For(1, 3, i => SendEvent(i, msg_body));

                // Task[] tasks = new Task[]{
                //     SendEvent(1, 3, msg_body),
                //     SendEvent(2, 2, msg_body)
                // };
                Task[] tasks = new Task[]{
                    SendEvent(1, 1, msg_body),
                    SendEvent(2, 1, msg_body),
                    SendEvent(3, 1, msg_body),
                    SendEvent(4, 1, msg_body),
                    SendEvent(5, 1, msg_body),
                };
                Task.WaitAll(tasks);

                Console.WriteLine("finished");
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

        private void SleepTask(int sec){
            Console.WriteLine($"{sec} sleep start");
            Thread.Sleep(sec*1000);
            Console.WriteLine($"sleep end {sec}");
        }

        private async Task SendEvent(int pid, int evt_num, string msg_body){
            Console.WriteLine($"proc {pid} start");

            string partitionKey = $"pkey_{pid}";

            for(int evt_id=0; evt_id<evt_num; evt_id++){
                string json_msg = "{" + $"'msgBody': {msg_body}, 'pKey': '{partitionKey}', 'evtId':'{evt_id}'" + "}";

                var batchOptions = new CreateBatchOptions{PartitionKey = partitionKey};
                using EventDataBatch eventBatch = await EHubProdClient.CreateBatchAsync(batchOptions);

                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json_msg)));
                await EHubProdClient.SendAsync(eventBatch);

                Thread.Sleep(100);
            }
            Console.WriteLine($"...proc {pid} finish");
        }

    }
}
