using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace KafkaTools
{
    public class KafkaReader : BackgroundService
    {
        private readonly IConsumerProvider _consumerProvider;

        public KafkaReader(IConsumerProvider consumerProvider)
        {
            _consumerProvider = consumerProvider;
        }

        private Dictionary<string, int> _usernamesWithAges = new Dictionary<string, int>();


        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var reader = _consumerProvider.GetConsumer();

            reader.Subscribe("UserTopic");
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = reader.Consume(stoppingToken);
                Message<string, User> message = result.Message;
                Console.WriteLine("User " + message.Value.Username + " received");
                if (message.Key == null)
                {
                    continue;
                }

                _usernamesWithAges[message.Key] = message.Value.Age;
                PrintUsers();
            }

            return Task.CompletedTask;
        }

        private void PrintUsers()
        {
            var keys = _usernamesWithAges.Keys.ToList();
            foreach (var username in keys)
            {
                var age = _usernamesWithAges[username];
                Console.WriteLine(username + " is " + age + " years old");
            }
        }
    }
}