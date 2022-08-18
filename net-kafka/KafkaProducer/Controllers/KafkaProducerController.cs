using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ServiceStack.Configuration;
using ServiceStack.Text;
using System;
namespace KafkaProducer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"

        };
        private readonly string topic = "simpletalk_topic";
        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {
            return Created(string.Empty, SendToKafka(topic, message));
        }
        public Object SendToKafka(string topic , string message)
        {
            using (var producer =
                  new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Oops, something went wrong: {e}");
                }
            }
            return null;
        }
    }
}
