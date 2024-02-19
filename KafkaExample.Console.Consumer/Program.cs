// Docs: https://github.com/confluentinc/confluent-kafka-dotnet

Console.WriteLine("Application started. Press Ctrl+C to shut down.");
Console.WriteLine("Kafka Consuming...");

IConfigurationRoot configuration = GetConfiguration();

string? groupId = configuration["Kafka:GroupId"];
string? bootstrapServers = configuration["Kafka:BootstrapServers"];
string? topic = configuration["Kafka:Topic"];

ConsumerConfig conf = new()
{
    GroupId = groupId,
    BootstrapServers = bootstrapServers,
    // Note: The AutoOffsetReset property determines the start offset in the event
    // there are not yet any committed offsets for the consumer group for the
    // topic/partitions of interest. By default, offsets are committed
    // automatically, so in this example, consumption will only start from the
    // earliest message in the topic 'my-topic' the first time you run the program.
    AutoOffsetReset = AutoOffsetReset.Earliest
};


using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
{
    consumer.Subscribe(topic);

    CancellationTokenSource cts = new();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true; // prevent the process from terminating.
        cts.Cancel();
    };

    try
    {
        while (true)
        {
            try
            {
                ConsumeResult<Ignore, string> cr = consumer.Consume(cts.Token);
                Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error occured: {e.Error.Reason}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        // Ensure the consumer leaves the group cleanly and final offsets are committed.
        Console.WriteLine("Application is shutting down...");
        consumer.Close();
    }
}


static IConfigurationRoot GetConfiguration()
{
    return new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", false, true)
        .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")}.json", false, true)
        .Build();
}