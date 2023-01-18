using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace KafkaTools
{
    public class KafkaWriter : BackgroundService
    {
        private readonly IProducerProvider _producerProvider;

        public KafkaWriter(IProducerProvider producerProvider)
        {
            _producerProvider = producerProvider;
        }

        private void LogHandler(IProducer<string, User> arg1, LogMessage arg2)
        {
            if (arg2.Level > SyslogLevel.Info)
            {
                Console.Error.WriteLine(arg2.Message);
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var producer = _producerProvider.GetProducer(LogHandler);

            while (!stoppingToken.IsCancellationRequested)
            {
                Console.Write("Please give a username: ");
                var username = Console.ReadLine();

                Console.Write($"Please provide an age for user {username}: ");
                var age = Console.ReadLine();

                if (!int.TryParse(age, out int convertedAge))
                {
                    Console.WriteLine("Provided age is not a number, please retry");
                    continue;
                }

                var user = new User(username, convertedAge);
                try
                {
                    var result = await producer.ProduceAsync("UserTopic", new Message<string, User> { Key = username, Value = user });
                    if (result.Status == PersistenceStatus.Persisted)
                    {
                        Console.WriteLine("User added");
                    }
                    else
                    {
                        Console.WriteLine("Could not add user");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Couldn't save user, expecption occured: {0}", ex.Message);
                }
            }
        }
    }
}
