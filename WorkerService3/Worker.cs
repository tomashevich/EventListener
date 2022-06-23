using System.Text;
using System.Text.Json;
using EventStore.Client;
using Polly;
using Polly.Retry;

namespace WorkerService3
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private EventStoreClient _esClient;

        public Worker()
        {
        }
        private void InitEsdbClient()
        {
            Console.WriteLine($"Esdb client init start");
            var esdbConnectionString = $"{Environment.GetEnvironmentVariable("ESDB_CONNECTION_STRING")}";
            _esClient = new EventStoreClient(EventStoreClientSettings.Create(esdbConnectionString));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            InitEsdbClient();

            //wait in case esdb is not ready yet
            var reconnectPolicy = GetPolicyWithRetryCount(3, action: ReconnectClient());
            await reconnectPolicy.ExecuteAsync(() => AppendToStream());

            await Init();
        }


        public async Task Init()
        {
            Console.WriteLine($"subscribing to events");
            Console.WriteLine();

            var eventsList = new List<string>
            {
                "user:cell_start",
                "user:cell_stop",

                "storage:bin_startfetchcycle",
                "storage:bin_ready",
                "storage:bin_getnext",
                "storage:bin_stopfetchcycle",

                "stock:picktask_issued",
                "stock:taskgroup_get",
                "stock:taskgroup_issued",
                "stock:picktask_get",
                "stock:picktask_started",
                "stock:picktask_completed",

                "mfc:box_get",
                "mfc:box_movingstarted",
                "mfc:box_ready",
                "mfc:box_complete",

                "pick:pick_start",
                "pick:pick_completed",

                "robot:mission_start",
                "robot:status_issued",

                "order:order_complete",

            };

            foreach (var evnt in eventsList)
            {
                await SubscribeToStream(evnt, e => HandleEvent(evnt, e, CancellationToken.None)
                ).ConfigureAwait(false);

            }
            Console.WriteLine();
            Console.WriteLine($"subscribed...");
            Console.WriteLine($"waiting for events...");
            Console.WriteLine();
        }

        private async Task AppendToStream()
        {
            var evt = new StartEvent
            {
                ApplicationName = "EventListener",
                StartedAt = DateTime.Now
            };

            var eventData = new EventData(
                Uuid.NewUuid(),
                "StartEvent",
                JsonSerializer.SerializeToUtf8Bytes(evt)
            );


            await _esClient.AppendToStreamAsync(
                        "Started",
                        StreamState.Any,
                        new[] { eventData },
                        cancellationToken: CancellationToken.None);
        }

        private async Task SubscribeToStream(string streamName, Action<ResolvedEvent> action)
        {
            await _esClient.SubscribeToStreamAsync(
               streamName,
               FromStream.End,
               async (subscription, evnt, cancellationToken) =>
               {
                   action(evnt);
               }).ConfigureAwait(false);
        }


        public void HandleEvent(string streamName, ResolvedEvent e, CancellationToken cancellationToken)
        {
            var bytesAsString = Encoding.UTF8.GetString(e.Event.Data.ToArray());
            Console.WriteLine($"{e.Event.Created.ToString("MM/dd/yy H:mm:ss:ffff")} {streamName}  EventType = {e.Event.EventType}, event Data: {bytesAsString}");
            Console.WriteLine();

        }

        private AsyncRetryPolicy GetPolicyWithRetryCount(int numberOfRetries, Action<Exception, TimeSpan> action)
        {
            return Policy.Handle<Exception>()
                .WaitAndRetryAsync(numberOfRetries, retryAttempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, retryAttempt) / 2),
                    onRetry: action);
        }

        private Action<Exception, TimeSpan> ReconnectClient()
        {
            return (response, retryCount) =>
            {
                Console.WriteLine($"Error, trying to reconnect...");

                Task.Delay(2000).Wait();
            };
        }
    }
}