using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace KafkaTools
{
    public record Settings
    {
        public int NumberOfAcknowledgements { get; set; }
        public int BatchSize { get; set; }
        public Guid GroupId { get; } = new();
        public TimeSpan SessionTimeout { get; set; }
        public AutoOffsetReset AutoOffsetReset { get; set; }
        public string BootstrapServers { get; set; }
        public SaslSettings Sasl { get; set; }

        public Dictionary<string, string> ToDictionary()
        {
            return new() {
                { "acks", NumberOfAcknowledgements.ToString() },
                { "batch.size", BatchSize.ToString() },
                { "group.id", GroupId.ToString() },
                { "session.timeout.ms", SessionTimeout.TotalMilliseconds.ToString() },
                { "auto.offset.reset",  AutoOffsetReset.ToString()},
                { "bootstrap.servers", BootstrapServers },
                { "sasl.username", Sasl.Username },
                { "sasl.password", Sasl.Password },
                { "sasl.mechanisms", Sasl.SaslMechanism.ToString() },
                { "security.protocol", Sasl.SecurityProtocol.ToString() }
            };
        }
    }

    public record SaslSettings
    {
        public string Username { get; set; }
        public string Password { get; set; }
        public string SaslMechanism { get; set; }
        public string SecurityProtocol { get; set; }
    }

}
