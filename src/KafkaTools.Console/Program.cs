using Confluent.Kafka;
using KafkaTools;

internal class Program
{
    private static async Task Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide one argument");
            return;
        }

        var option = args[0];
        
        var settings = new Settings()
        {
            AutoOffsetReset = AutoOffsetReset.Earliest,
            BatchSize = 1,
            BootstrapServers = Environment.GetEnvironmentVariable("CONFLUENT_TEST_BROKERS"),
            Sasl = new()
            {
                Password = Environment.GetEnvironmentVariable("CONFLUENT_SASL_PASSWORD"),
                Username = Environment.GetEnvironmentVariable("CONFLUENT_SASL_USERNAME"),
                SecurityProtocol = "SASL_SSL",
                SaslMechanism = "PLAIN"
            },
            NumberOfAcknowledgements = 1,
            SessionTimeout = TimeSpan.FromMilliseconds(45000)
        };

        IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                services.AddSingleton(settings);
                services.AddSingleton<IProducerProvider, ProducerProvider>();
                services.AddSingleton<IConsumerProvider, ConsumerProvider>();
                if (option.Equals("read", StringComparison.OrdinalIgnoreCase))
                {
                    services.AddHostedService<KafkaReader>();
                }
                if (option.Equals("write", StringComparison.OrdinalIgnoreCase))
                {
                    services.AddHostedService<KafkaWriter>();
                }
            })
            .Build();

        await host.RunAsync();
    }
}