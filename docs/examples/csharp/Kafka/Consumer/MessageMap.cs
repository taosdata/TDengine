using Newtonsoft.Json;
using System.Collections.Concurrent;
namespace Consumer
{
    const int MAX_SQL_RECORD = 500;
    internal class MessageMap
    {
        List<string> Key;
        List<MessageQueue> Value;

        public MessageMap()
        {
            Key = new List<string>();
            Value = new List<MessageQueue>();
        }

        public void Add(string key, string value)
        {
            if (!Key.Contains(key))
            {
                Key.Add(key);
            }
            Value[Key.IndexOf(key)].AddMessage(value);
        }

        public List<string> GetKeys()
        {
            return Key;
        }
        public MessageQueue GetValuesOfKey(string key)
        {
            if (Key.Contains(key))
            {
                return Value[Key.IndexOf(key)];
            }
            else
            {
                throw new Exception($"Message map doesn't contain key {key}");
            }
        }


    }


    public class MessageDictionary_2
    {
        public ConcurrentDictionary<MeterTag, Queue<MeterValues>> keyValuePairs { get;}
        public List<MeterTag> readyList { get; set; }
        public MessageDictionary_2()
        {
            keyValuePairs = new ConcurrentDictionary<MeterTag, Queue<MeterValues>>();
            readyList = new List<MeterTag>();
        }

        public void Add(string key, string value)
        {
            MeterTag tag = JsonConvert.DeserializeObject<MeterTag>(key);
            MeterValues values = JsonConvert.DeserializeObject<MeterValues>(value);
            Add(tag, values);
        }


        public void Add(MeterTag tag, MeterValues values)
        {

            if (keyValuePairs.TryGetValue(tag, out Queue<MeterValues> q))
            {
                q.Enqueue(values);
                if (q.Count > MAX_SQL_RECORD && !readyList.Contains(tag))
                {
                    readyList.Add(tag);
                }
            }
            else
            {
                var newQueue = new Queue<MeterValues>();
                newQueue.Enqueue(values);
                keyValuePairs.TryAdd(tag, newQueue);
            }

        }

        public Queue<MeterValues> GetMessage(string key)
        {
            MeterTag tag = JsonConvert.DeserializeObject<MeterTag>(key);

            if (keyValuePairs.TryGetValue(tag, out Queue<MeterValues> v))
            {
                readyList.Remove(tag);
                return v;
            }
            else
            {
                return null;
            }
        }

        public Queue<MeterValues> Remove(MeterTag tag)
        {
            lock (keyValuePairs)
            {
                readyList.Remove(tag);
                if (keyValuePairs.TryRemove(tag, out Queue<MeterValues> v))
                {
                    return v;
                }
                else
                {
                    return null;
                }

            }
        }
        public void Display()
        {
            foreach (KeyValuePair<MeterTag, Queue<MeterValues>> kv in keyValuePairs)
            {
                Console.Write("tags:{0} ", kv.Key);
                foreach (MeterValues s in kv.Value)
                {
                    Console.Write("{0},", s);
                }
                Console.WriteLine();
            }
        }
    }



    public class MessageDictionary
    {
        public ConcurrentDictionary<string, Queue<string>> keyValuePairs { get; }
        public List<string> readyList { get; set; }
        public MessageDictionary()
        {
            keyValuePairs = new ConcurrentDictionary<string, Queue<string>>();
            readyList = new List<string>();
        }


        public void Add(string tag, string values)
        {

            if (keyValuePairs.TryGetValue(tag, out Queue<string> q))
            {
                q.Enqueue(values);
                if (q.Count > 500 && !readyList.Contains(tag))
                {
                    readyList.Add(tag);
                }
            }
            else
            {
                var newQueue = new Queue<string>();
                newQueue.Enqueue(values);
                keyValuePairs.TryAdd(tag, newQueue);
            }

        }

        public Queue<string> GetMessage(string key)
        {

            if (keyValuePairs.TryGetValue(key, out Queue<string> v))
            {
                readyList.Remove(key);
                return v;
            }
            else
            {
                return null;
            }
        }

        public Queue<string> Remove(string tag)
        {
            lock (keyValuePairs)
            {
                readyList.Remove(tag);
                if (keyValuePairs.TryRemove(tag, out Queue<string> v))
                {
                    return v;
                }
                else
                {
                    return null;
                }

            }
        }
        public void Display()
        {
            foreach (KeyValuePair<string, Queue<string>> kv in keyValuePairs)
            {
                Console.Write("tags:{0} ", kv.Key);
                foreach (string s in kv.Value)
                {
                    Console.Write("{0},", s);
                }
                Console.WriteLine();
            }
        }
    }


    internal class MessageQueue
    {
        Queue<string> _queue;
        public MessageQueue()
        {
            _queue = new Queue<string>();
        }

        public void AddMessage(string msg)
        {
            _queue.Enqueue(msg);
        }

        public Queue<String> GetMessageQueue()
        {
            return _queue;
        }

    }

}
