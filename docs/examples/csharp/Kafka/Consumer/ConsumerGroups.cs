using System.Collections.Concurrent;

namespace Consumer
{
    internal class ConsumerGroups
    {
        int CONSUMER_CNT = 1;

        Task[] consumerGroup;
        CancellationTokenSource tokenSource;
        CancellationToken token;

        public ConsumerGroups(int consumerCount)
        {
            this.CONSUMER_CNT = consumerCount;
            consumerGroup = new Task[CONSUMER_CNT];
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

        }
        public async Task Run(IntPtr conn)
        {

            var tasks = new ConcurrentBag<Task>();

            for (int i = 0; i < CONSUMER_CNT; i++)
            {
                Console.WriteLine("ConsumerGroups.Run().for");
                consumerGroup[i] = Task.Factory.StartNew((Object obj) =>
                {
                    TDConsumer consumer = obj as TDConsumer;
                    Console.Write("Consumer {0}#", consumer.consumerID);
                    var child = Task.Factory.StartNew((Object obj) =>
                    {
                        var c = new Consume();
                        c.RunSimpleConsumer("test", conn);
                    }
                   , this.token, TaskCreationOptions.AttachedToParent);

                    tasks.Add(child);

                }, new TDConsumer(i), this.token);
                tasks.Add(consumerGroup[i]);
            }

            try
            {
                await Task.WhenAll(tasks.ToArray());
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"\n{nameof(OperationCanceledException)} thrown\n");
            }
            finally
            {
                tokenSource.Dispose();
            }

            // Display status of all tasks.
            foreach (var task in tasks)
                Console.WriteLine("Task {0} status is now {1}", task.Id, task.Status);
        }

    }

    public class TDConsumer
    {
        public int consumerID;
        string consumer_name;
        public TDConsumer(int id)
        {
            consumerID = id;
            consumer_name = "consumer_" + id;
        }
    }
}

