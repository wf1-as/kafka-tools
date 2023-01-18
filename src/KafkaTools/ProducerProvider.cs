using Confluent.Kafka;
using System;

namespace KafkaTools
{
    public class ProducerProvider : IProducerProvider
    {
        private readonly Settings _settings;

        public ProducerProvider(Settings settings)
        {
            _settings = settings;
        }

        public IProducer<string, User> GetProducer(Action<IProducer<string, User>, LogMessage> logHanlder)
        {
            return new ProducerBuilder<string, User>(_settings.ToDictionary())
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(new JsonSerializer<User>())
                .SetLogHandler(logHanlder)
                .Build();
        }
    }
}
