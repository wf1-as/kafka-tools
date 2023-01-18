using Confluent.Kafka;

namespace KafkaTools
{
    public interface IConsumerProvider
    {
        IConsumer<string, User> GetConsumer();
    }
}
