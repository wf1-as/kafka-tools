using Confluent.Kafka;
using System;

namespace KafkaTools
{
    public interface IProducerProvider
    {
        IProducer<string, User> GetProducer(Action<IProducer<string, User>, LogMessage> logHanlde);
    }
}
