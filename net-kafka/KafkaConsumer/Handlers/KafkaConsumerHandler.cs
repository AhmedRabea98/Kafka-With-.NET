using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using System;

namespace KafkaConsumer.Handlers
{
    public class KafkaConsumerHandler : IHostedService
    {
        private readonly string topic = "simpletalk_topic";
        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig {

                GroupId = "st_consumer_group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest

            };
            using (var builder = new ConsumerBuilder<Ignore,
                string>(conf).Build())
            {

                builder.Subscribe(topic);
                var cancelToken =new CancellationTokenSource();
                try {
                    while (true) { 
                        var consumer = builder.Consume(cancellationToken);
                        System.Console.WriteLine($"Message : {consumer.Message.Value}" +
                            $"receiver from {consumer.TopicPartitionOffset}");
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine(e.Message);
                    builder.Close();
                }
            }
                return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
