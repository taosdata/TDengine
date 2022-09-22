using System;
using TDengineTMQ;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace TMQExample
{
    internal class SubscribeDemo
    {
        static void Main(string[] args)
        {
            IntPtr conn = GetConnection();
            string topic = "topic_example";
            //create topic 
            IntPtr res = TDengine.Query(conn, $"create topic if not exists {topic} as select * from meters");

            if (TDengine.ErrorNo(res) != 0 )
            {
                throw new Exception($"create topic failed, reason:{TDengine.Error(res)}");
            }

            var cfg = new ConsumerConfig
            {
                GourpId = "group_1",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
            };

            // create consumer 
            var consumer = new ConsumerBuilder(cfg)
                .Build();

            // subscribe
            consumer.Subscribe(topic);

            // consume 
            for (int i = 0; i < 5; i++)
            {
                var consumeRes = consumer.Consume(300);
                // print consumeResult
                foreach (KeyValuePair<TopicPartition, TaosResult> kv in consumeRes.Message)
                {
                    Console.WriteLine("topic partitions:\n{0}", kv.Key.ToString());

                    kv.Value.Metas.ForEach(meta =>
                    {
                        Console.Write("{0} {1}({2}) \t|", meta.name, meta.TypeName(), meta.size);
                    });
                    Console.WriteLine("");
                    kv.Value.Datas.ForEach(data =>
                    {
                        Console.WriteLine(data.ToString());
                    });
                }

                consumer.Commit(consumeRes);
                Console.WriteLine("\n================ {0} done ", i);

            }

            // retrieve topic list
            List<string> topics = consumer.Subscription();
            topics.ForEach(t => Console.WriteLine("topic name:{0}", t));

            // unsubscribe
            consumer.Unsubscribe();

            // close consumer after use.Otherwise will lead memory leak.
            consumer.Close();
            TDengine.Close(conn);

        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "power";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }
    }

}
