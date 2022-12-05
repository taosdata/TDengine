using Newtonsoft.Json;
using System.Diagnostics;
using TDengineDriver;

namespace Consumer
{
    internal class EntryPoint
    {
        public static async Task Main(string[] args)
        {
            IntPtr conn = TDengine.Connect("127.0.0.1", "root", "taosdata", "power", 0);


            Consume consumerSample = new Consume();
            //consumerSample.RunSimpleConsumer("topicmeters", conn);
            //consumerSample.RunConsumerWriteInSQL("topicmeters", conn);
            consumerSample.RunConsumerWriteInBatch("topicmeters", conn);

            TDengine.Close(conn);
        }

    }

}
