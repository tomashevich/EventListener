using System.Text;
using EventStore.Client;

namespace WorkerService3
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly EventStoreClient _esClient;

        public Worker()
        {

            Console.WriteLine($"Esdb client init started");
            var esdbConnectionString = $"{Environment.GetEnvironmentVariable("ESDB_CONNECTION_STRING")}";
            _esClient = new EventStoreClient(EventStoreClientSettings.Create(esdbConnectionString));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
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

                "stock:picktask_issued",
                "stock:taskgroup_get",
                "stock:taskgroup_issued",

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


        private async Task SubscribeToStream(string streamName, Action<ResolvedEvent> action)
        {
            var sub = await _esClient.SubscribeToStreamAsync(
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
            Console.WriteLine($"{streamName}  EventType = {e.Event.EventType}, event Data: {bytesAsString}");

        }
    }
}