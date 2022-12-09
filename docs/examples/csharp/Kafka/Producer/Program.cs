
using System.Net.NetworkInformation;
using TDengineDriver;
namespace Producer
{
    internal class EntryPoint
    {
        public static async Task Main(string[] args)
        {
   
            Produce producerSample = new Produce();
            await producerSample.ProduceAsync("bootstrap", "schemaRegisterUrl", "topicmeters");


        }
    }
}
