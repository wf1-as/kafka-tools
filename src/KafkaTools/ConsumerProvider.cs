using Confluent.Kafka;

namespace KafkaTools
{
    public class ConsumerProvider : IConsumerProvider
    {
        private readonly Settings _settings;

        public ConsumerProvider(Settings settings)
        {
            _settings = settings;
        }

        public IConsumer<string, User> GetConsumer()
        {
            return new ConsumerBuilder<string, User>(_settings.ToDictionary())
                         .SetValueDeserializer(new JsonSerializer<User>())
                         .SetKeyDeserializer(Deserializers.Utf8)
                         .Build();
        }
    }
}
