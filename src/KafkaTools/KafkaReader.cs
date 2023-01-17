using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace KafkaTools
{
    internal class KafkaReader
    {
        public Dictionary<string, string> Settings;

        private Dictionary<string, int> _UsersWithNames = new Dictionary<string, int>();

        public KafkaReader(Dictionary<string, string> settings)
        {
            settings["group.id"] = Guid.NewGuid().ToString();
            settings["session.timeout.ms"] = "45000";
            settings["auto.offset.reset"] = "earliest";
            Settings = settings;
        }

        public void Run(CancellationToken Token)
        {
            var reader = new ConsumerBuilder<string, User>(Settings)
                         .SetValueDeserializer(new JsonSerializer<User>())
                         .SetKeyDeserializer(Deserializers.Utf8)
                         .Build();

            reader.Subscribe("UserTopic");
            while (!Token.IsCancellationRequested)
            {
                var result = reader.Consume(Token);
                Message<string, User> message = result.Message;
                Console.WriteLine("User " + message.Value.Name + " received");

                _UsersWithNames[message.Key] = message.Value.Age;
                PrintUsers();
            }

            reader.Dispose();
        }

        private void PrintUsers()
        {
            var keys = _UsersWithNames.Keys.ToList();
            foreach (var username in keys)
            {
                var age = _UsersWithNames[username];
                Console.WriteLine(username + " is " + age + " years old");
            }
        }
    }
}