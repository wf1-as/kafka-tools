using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaTools
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            string option = args[0];
            var settings = GetKafkaSettings();

            var canncel = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) => canncel.Cancel();

                if (option == "read")
                {
                    var reader = new KafkaReader(settings);
                    await Task.Run(() => reader.Run(canncel.Token));
                }
                else if (option == "write")
                {
                    Writer writer = new Writer(settings);
                    await writer.Start(canncel.Token);
                }
                else Console.WriteLine("Invalid command");
        }

        private static Dictionary<string, string> GetKafkaSettings()
        {
            return new Dictionary<string, string>
            {
                ["bootstrap.servers"] = Environment.GetEnvironmentVariable("CONFLUENT_TEST_BROKERS"),
                ["sasl.username"] = Environment.GetEnvironmentVariable("CONFLUENT_SASL_USERNAME"),
                ["sasl.password"] = Environment.GetEnvironmentVariable("CONFLUENT_SASL_PASSWORD"),
                ["sasl.mechanisms"] = "PLAIN",
                ["security.protocol"] = "SASL_SSL"
            };
        }
    }

    internal class Writer
    {
        private IProducer<string, User> Producer;
        public Writer(Dictionary<string, string> settings)
        {
            settings["acks"] = "1";
            settings["batch.size"] = "1";
            Producer = new ProducerBuilder<string, User>(settings)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(new JsonSerializer<User>())
                .SetLogHandler(LogHandler)
                .Build();
        }

        private void LogHandler(IProducer<string, User> arg1, LogMessage arg2)
        {
            if (arg2.Level > SyslogLevel.Info)
            {
                Console.Error.WriteLine(arg2.Message);
            }
        }

        public Task Start(CancellationToken token)
        {
            return Task.Run(
                async () =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        Console.Write ("Username,Age: ");
                        var input = Console.ReadLine().Split(',');

                        var user = new User(input[0], int.Parse(input[1]));
                        try
                        {
                            var result = await Producer.ProduceAsync("UserTopic", new Message<string, User> { Key = input[0], Value = user });
                            if (result.Status != PersistenceStatus.Persisted)
                            {
                                Console.WriteLine("User added");
                            }
                            else
                            {
                                Console.WriteLine("Could not add user");
                            }
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("COuldn't save user");
                        }
                    }
                });
        }
    }

    internal class User
    {
        public User()
        {
        }
        public User(string name, int age)
        {
            name = Name;
            Age = age;
        }

        public string Name { get; set; }

        public int Age { get; set; }
    }
}
