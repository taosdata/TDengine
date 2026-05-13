<template>
  <div>
    <h2 id="create-project">{{ t('connector.csharp.step1') }}</h2>
    <pre
      v-highlight="
        `dotnet new console -o example
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('connector.csharp.step11desc') }}</p>
    <pre
      v-highlight="
        `cd example
vim example.csproj
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('connector.csharp.step12desc') }}</p>
    <pre
      v-highlight="
        `&lt;ItemGroup&gt;
  &lt;PackageReference Include=&quot;TDengine.Connector&quot; Version=&quot;3.1.*&quot; GeneratePathProperty=&quot;true&quot; /&gt;
&lt;/ItemGroup&gt;
&lt;Target Name=&quot;copyDLLDependency&quot; BeforeTargets=&quot;BeforeBuild&quot;&gt;
  &lt;ItemGroup&gt;
    &lt;DepDLLFiles Include=&quot;$(PkgTDengine_Connector)\\runtimes\\**\\*.*&quot; /&gt;
  &lt;/ItemGroup&gt;
  &lt;Copy SourceFiles=&quot;@(DepDLLFiles)&quot; DestinationFolder=&quot;$(OutDir)&quot; /&gt;
&lt;/Target&gt;`
      "
    ><code class="language-xml"></code></pre>
    <doc-config
      :id="'config'"
      need-token
      :url="endpoint"
      :token="instance.token"
      :url-key="'TDENGINE_CLOUD_ENDPOINT'"
      :url-des="t('docsConfig.endpoint')"
    ></doc-config>
    <h2 id="create-consumer">{{ t('topic.step3') }}</h2>
    <p>{{ t('topic.step3desc') }}</p>
    <pre
      v-highlight
    ><code class="language-csharp">var cloudEndPoint = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
var cfg = new Dictionary&lt;string, string&gt;()
            {
              { "td.connect.type", "WebSocket" },
              { "group.id", "group1" },
              { "auto.offset.reset", "latest" },
              { "td.connect.ip", cloudEndPoint.ToString() },
              { "td.connect.port", "443" },
              { "useSSL", "true" },
              { "token", cloudToken.ToString() },
              { "client.id", "tmq_example" },
              { "enable.auto.commit", "true" },
              { "msg.with.table.name", "false" },
            };
var consumer = new ConsumerBuilder&lt;Dictionary&lt;string, object&gt;&gt;(cfg).Build();
</code></pre>
    <h2 id="subscribe-consume">{{ t('topic.step4') }}</h2>
    <p>{{ t('topic.step4desc', [topicName]) }}</p>
    <pre v-highlight="conSrc"><code class="language-csharp"></code></pre>
    <h2 id="close-consumer">{{ t('topic.step5') }}</h2>
    <p>{{ t('topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-csharp"># Unsubscribe
consumer.unsubscribe()
# Close consumer
consumer.close()</code></pre>
    <h2 id="fullexample">{{ t('topic.step6') }}</h2>
    <p>{{ t('topic.step6desc', [topicName]) }}</p>
    <pre v-highlight="sampleCode"><code class="language-csharp"></code></pre>
  </div>
</template>

<script lang="ts" setup>
import { instance } from 'config';
import DocConfig from '../configTabs.vue';
import { endpoint } from '../utils';
const props = defineProps<{
  topic?: string;
}>();
const { t } = useI18n();
const topicName = computed(() => {
  return props.topic ? props.topic : t('topic.defaultTopic');
});
const conSrc = computed(
  () => `consumer.Subscribe(new List<string>() { "${topicName.value}" });while (true)
{
  using (var cr = consumer.Consume(500))
  {
     if (cr == null) continue;
     foreach (var message in cr.Message)
     {
       // handle message
     }
  }
}`
);
const sampleCode = computed(
  () =>
    `using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace Cloud.Examples
{
    public class SubscribeDemo
    {
        private static string _host = "";
        private static string _token = "";
        private static string _groupId = "";
        private static string _clientId = "";
        private static string _topic = "";

        static void Main(string[] args)
        {

            var cloudEndPoint = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
            _host = cloudEndPoint.ToString();
            _token = cloudToken.ToString();
            
            try
            {
                  var consumer = CreateConsumer();
                  // consume message
                  Consume(consumer);
                  // seek
                  Seek(consumer);
                  // commit
                  CommitOffset(consumer);
                  // close
                  Close(consumer);
                  Console.WriteLine("Done");
            }
            catch (TDengineError e)
            {
               // handle TDengine error
               Console.WriteLine("Failed to insert to table meters using stmt, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
               throw;
            }
            catch (Exception e)
            {
               // handle other exceptions
               Console.WriteLine("Failed to insert to table meters using stmt, ErrMessage: " + e.Message);
               throw;
            }
        }

      static IConsumer<Dictionary<string, object>> CreateConsumer(){
          _groupId = "group1";
          _clientId = "client1";
          var cfg = new Dictionary<string, string>()
               {
                  { "td.connect.type", "WebSocket" },
                  { "group.id", _groupId },
                  { "auto.offset.reset", "latest" },
                  { "td.connect.ip", _host},
                  { "td.connect.port", "443" },
                  { "useSSL", "true" },
                  { "token", _token},
                  { "client.id", _clientId },
                  { "enable.auto.commit", "true" },
                  { "msg.with.table.name", "false" },
               };

         return new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
      }
         static void Consume(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: subscribe
            _topic = "${topicName.value}";
            try
            {
                // subscribe
                consumer.Subscribe(new List<string>() { _topic });
                Console.WriteLine("Subscribe topics successfully");
                for (int i = 0; i < 50; i++)
                {
                    // consume message with using block to ensure the result is disposed
                    using (var cr = consumer.Consume(100))
                    {
                        if (cr == null) continue;
                        foreach (var message in cr.Message)
                        {
                            // handle message
                        }
                    }
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine(
                    $"Failed to poll data, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine($"Failed to poll data, " +
                                  $"topic: {_topic}, " +
                                  $"groupId: {_groupId}, " +
                                  $"clientId: {_clientId}, " +
                                  $"ErrMessage: {e.Message}");
                throw;
            }
            // ANCHOR_END: subscribe
        }

        static void Seek(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: seek
            try
            {
                // get assignment
                var assignment = consumer.Assignment;
                Console.WriteLine($"Now assignment: {assignment}");
                // seek to the beginning
                foreach (var topicPartition in assignment)
                {
                    consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                }

                Console.WriteLine("Assignment seek to beginning successfully");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine(
                    $"Failed to seek offset, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"offset: 0, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    $"Failed to seek offset, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"offset: 0, " +
                    $"ErrMessage: {e.Message}");
                throw;
            }
            // ANCHOR_END: seek
        }

        static void CommitOffset(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: commit_offset
            for (int i = 0; i < 5; i++)
            {
                TopicPartitionOffset topicPartitionOffset = null;
                try
                {
                    // consume message with using block to ensure the result is disposed
                    using (var cr = consumer.Consume(100))
                    {
                        if (cr == null) continue;
                        // commit offset
                        topicPartitionOffset = cr.TopicPartitionOffset;
                        consumer.Commit(new List<TopicPartitionOffset>
                        {
                            topicPartitionOffset,
                        });
                        Console.WriteLine("Commit offset manually successfully.");
                    }
                }
                catch (TDengineError e)
                {
                    // handle TDengine error
                    Console.WriteLine(
                        $"Failed to commit offset, " +
                        $"topic: {_topic}, " +
                        $"groupId: {_groupId}, " +
                        $"clientId: {_clientId}, " +
                        $"offset: {topicPartitionOffset}, " +
                        $"ErrCode: {e.Code}, " +
                        $"ErrMessage: {e.Error}");
                    throw;
                }
                catch (Exception e)
                {
                    // handle other exceptions
                    Console.WriteLine(
                        $"Failed to commit offset, " +
                        $"topic: {_topic}, " +
                        $"groupId: {_groupId}, " +
                        $"clientId: {_clientId}, " +
                        $"offset: {topicPartitionOffset}, " +
                        $"ErrMessage: {e.Message}");
                    throw;
                }
            }
            // ANCHOR_END: commit_offset
        }

        static void Close(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: close
            try
            {
                // unsubscribe
                consumer.Unsubscribe();
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine(
                    $"Failed to unsubscribe consumer, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    $"Failed to execute commit example, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrMessage: {e.Message}");
                throw;
            }
            finally
            {
                // close consumer
                consumer.Close();
                Console.WriteLine("Consumer closed successfully.");
            }
            // ANCHOR_END: close
        }
    }
}`
);
</script>

<style scoped lang="scss"></style>
