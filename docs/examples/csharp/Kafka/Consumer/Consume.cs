using Confluent.Kafka;
using Newtonsoft.Json;
using System.Diagnostics;

namespace Consumer
{
    internal class Consume
    {
        const int consumerNum = 3;
        ConsumerConfig conf = new ConsumerConfig
        {
            GroupId = "meter_consumer",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false,
            EnableAutoCommit = false,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            EnablePartitionEof = true,
            // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
            // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky

        };

        // ANCHOR: basicUsage
        /// <summary>
        /// Write to TDengine as soon as get a message.One message(record) one SQL.
        /// </summary>
        /// <param name="conn"></param>
        public void RunSimpleConsumer(string topic, IntPtr conn)
        {
            using (var c = new ConsumerBuilder<string, string>(conf)
                .SetErrorHandler((_, e) => Console.WriteLine("{0} Error:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), e.Reason))
                .SetStatisticsHandler((_, json) => Console.WriteLine("{0}, Statistic:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), json))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                       "Partitions incrementally assigned: [" +
                       string.Join(',', partitions.Select(p => p.Partition.Value)) +
                       "], all: [" +
                       string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                       "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                }).Build())
            {

                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();

                };
                var watch = Stopwatch.StartNew();
                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = c.Consume(cts.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            WriteData(consumeResult.Key, consumeResult.Value, conn);
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                            try
                            {
                                // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                                // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                                // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                                // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                                c.StoreOffset(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }

                    }

                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    watch.Stop();

                    Console.WriteLine("Consumer all message and insert into TDengine cost {1} milliseconds", watch.ElapsedMilliseconds);

                    c.Close();
                }
            }
        }
        // ANCHOR_END: basicUsage


        // ANCHOR: insertTasks
        /// <summary>
        /// Submit message to Tasks, which take the response to write data to TDengine through SQL.(also 
        /// one record on insert statement.
        /// </summary>
        public void RunConsumerWriteInSQL(string topic, IntPtr conn)
        {
            List<Task> taskList = new List<Task>();
            using (var c = new ConsumerBuilder<string, string>(conf)
                .SetErrorHandler((_, e) => Console.WriteLine("{0} Error:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), e.Reason))
                .SetStatisticsHandler((_, json) => Console.WriteLine("{0}, Statistic:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), json))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                       "Partitions incrementally assigned: [" +
                       string.Join(',', partitions.Select(p => p.Partition.Value)) +
                       "], all: [" +
                       string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                       "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                }).Build())
            {

                c.Subscribe(topic);
                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();

                };
                var watch = Stopwatch.StartNew();
                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = c.Consume(cts.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            taskList.Add(Task.Factory.StartNew((Object obj) =>
                            {
                                var result = obj as ConsumeResult<string, string>;
                                WriteData(result.Key, result.Value, conn);

                            }, consumeResult));

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                            try
                            {
                                // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                                // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                                // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                                // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                                c.StoreOffset(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }

                    }

                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    watch.Stop();

                    Console.WriteLine("Consumer all message and insert into TDengine cost {1} milliseconds", watch.ElapsedMilliseconds);

                    c.Close();
                }
            }
        }
        // ANCHOR_END: insertTasks

        // ANCHOR: insertBatchTasks
        /// <summary>
        /// restore message to an self defined Dictionary,while reach some condition then write to TDengine
        /// combine the records into a long insert statement.
        /// </summary>
        public void RunConsumerWriteInBatch(string topic, IntPtr conn)
        {
            MessageDictionary msgDic = new MessageDictionary();
            var watch = Stopwatch.StartNew();
            using (var c = new ConsumerBuilder<string, string>(conf)
                .SetErrorHandler((_, e) => Console.WriteLine("{0} Error:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), e.Reason))
                .SetStatisticsHandler((_, json) => Console.WriteLine("{0}, Statistic:{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), json))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                }).Build())
            {

                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();

                };

                try
                {
                    TDengineWriter tdengineWriter = new TDengineWriter();
                    while (true)
                    {
                        try
                        {
                            var consumeResult = c.Consume(cts.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                if (msgDic.keyValuePairs.Count > 0)
                                {
                                    foreach (KeyValuePair<string, Queue<string>> kv in msgDic.keyValuePairs)
                                    {
                                        Task.Factory.StartNew((Object obj) =>
                                        {
                                            var tag = obj as string;
                                            Queue<string> queue = msgDic.Remove(tag);
                                            WriteDataBatch(tag, queue, conn);
                                        }, kv.Key);
                                    }
                                }

                                continue;
                            }
                            //Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");


                            msgDic.Add(consumeResult.Key, consumeResult.Value);

                            if (msgDic.readyList.Contains(consumeResult.Key))
                            {
                                Task.Factory.StartNew((Object obj) =>
                                {
                                    var tag = obj as MeterTag;
                                    Queue<string> meterValues = msgDic.Remove(consumeResult.Key);
                                    WriteDataBatch(consumeResult.Key, meterValues, conn);
                                }, consumeResult.Key);
                            }

                            try
                            {
                                // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                                // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                                // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                                // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                                c.StoreOffset(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }

                    }

                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    watch.Stop();

                    Console.WriteLine("Consumer all message and insert into TDengine cost {1} milliseconds", watch.ElapsedMilliseconds);

                    c.Close();
                }
            }
        }
        // ANCHOR_END: insertBatchTasks

        internal void WriteData(string tag, string values, IntPtr conn)
        {
            MeterTag t = JsonConvert.DeserializeObject<MeterTag>(tag);
            MeterValues v = JsonConvert.DeserializeObject<MeterValues>(values);

            TDengineWriter writer = new TDengineWriter();
            string sql = writer.GenerateSql(t, v);
            //Console.WriteLine("{0} thread #{1} tags:{2}, value:{3}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"),Thread.CurrentThread.ManagedThreadId,result.Key,result.Value);
            writer.InsertData(conn, sql);
        }

        internal void WriteDataBatch(string tag, Queue<string> values, IntPtr conn)
        {
            TDengineWriter writer = new TDengineWriter();
            string sql = writer.GenerateSqlBatch(tag, values);
            //Console.WriteLine("{0} thread #{1} sql:{2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), Thread.CurrentThread.ManagedThreadId, sql);
            writer.InsertData(conn, sql);
        }
    }



}
