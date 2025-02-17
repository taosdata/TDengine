<template>
  <div>
    <h2 id="initialize">{{ t('topic.go.step1') }}</h2>
    <p>{{ t('topic.go.step1desc') }}</p>
    <pre v-highlight><code>go mod init tdengine.com/example</code></pre>
    <pre v-highlight><code class="language-go-mod">module tdengine.com/example

go 1.17

require github.com/taosdata/driver-go/v3 latest
</code></pre>
    <doc-config
      :id="'config'"
      :url="tmq"
      :need-token="false"
      :url-key="tmqKey"
      :url-des="t('docsConfig.tmq')"
    ></doc-config>
    <h2 id="create-consumer">{{ t('topic.step3') }}</h2>
    <p>{{ t('topic.step3desc') }}</p>
    <pre v-highlight><code class="language-go">import (
  "github.com/taosdata/driver-go/v3/common"
  tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
  "github.com/taosdata/driver-go/v3/ws/tmq"
)
tmqStr := os.Getenv("{{tmqKey}}")
consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
  "ws.url":                tmqStr,
  "ws.message.channelLen": uint(0),
  "ws.message.timeout":    common.DefaultMessageTimeout,
  "ws.message.writeWait":  common.DefaultWriteWait,
  "group.id":              "test_group",
  "client.id":             "test_consumer_ws",
  "auto.offset.reset":     "earliest",
})
if err != nil {
  panic(err)
}
</code></pre>
    <h2 id="subscribe-consume">{{ t('topic.step4') }}</h2>
    <p>{{ t('topic.step4desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `consumer, err := tmq.NewConsumer(config)
if err != nil {
  panic(err)
}
err = consumer.Subscribe(&quot;${topicName}&quot;, nil)
if err != nil {
  panic(err)
}
for {
  ev := consumer.Poll(10)
  if ev != nil {
    switch e := ev.(type) {
    case *tmqcommon.DataMessage:
      fmt.Printf(&quot;get message:%v\\n&quot;, e.String())
      consumer.Commit()
    case tmqcommon.Error:
      fmt.Printf(&quot;%% Error: %v: %v\\n&quot;, e.Code(), e)
      return
    default:
      fmt.Printf(&quot;unexpected event:%v\\n&quot;, e)
      return
    }
  }
}    
`
      "
    ><code class="language-go"></code></pre>
    <h2 id="close-consumer">{{ t('topic.step5') }}</h2>
    <p>{{ t('topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-go">consumer.Close()</code></pre>
    <h2 id="fullexample">{{ t('topic.step6') }}</h2>
    <p>{{ t('topic.step6desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `package main

import (
  &quot;fmt&quot;
  &quot;os&quot;
  &quot;github.com/taosdata/driver-go/v3/common&quot;
  tmqcommon &quot;github.com/taosdata/driver-go/v3/common/tmq&quot;
  &quot;github.com/taosdata/driver-go/v3/ws/tmq&quot;
)

func main() {
  tmqStr := os.Getenv(&quot;${tmqKey}&quot;)
  consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
    &quot;ws.url&quot;:                tmqStr,
    &quot;ws.message.channelLen&quot;: uint(0),
    &quot;ws.message.timeout&quot;:    common.DefaultMessageTimeout,
    &quot;ws.message.writeWait&quot;:  common.DefaultWriteWait,
    &quot;group.id&quot;:              &quot;test_group&quot;,
    &quot;client.id&quot;:             &quot;test_consumer_ws&quot;,
    &quot;auto.offset.reset&quot;:     &quot;earliest&quot;,
  })
  if err != nil {
    panic(err)
	}
  err = consumer.Subscribe(&quot;${topicName}&quot;, nil)
  if err != nil {
    panic(err)
  }
  defer consumer.Close()
  for {
    ev := consumer.Poll(10)
    if ev != nil {
      switch e := ev.(type) {
      case *tmqcommon.DataMessage:
        fmt.Printf(&quot;get message:%v\\n&quot;, e.String())
        consumer.Commit()
      case tmqcommon.Error:
        fmt.Printf(&quot;%% Error: %v: %v\\n&quot;, e.Code(), e)
        return
      default:
        fmt.Printf(&quot;unexpected event:%v\\n&quot;, e)
        return
      }
    }
  }
}
`
      "
    ><code class="language-go">
</code></pre>
  </div>
</template>
<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { instance, project } from 'config';

const props = withDefaults(
  defineProps<{
    topic: string;
  }>(),
  {
    topic: ''
  }
);
const tmq = computed(() => {
  const wsPrefix = instance.gatewayUrl.startsWith('https') ? 'wss' : 'ws';
  const uri = instance.gatewayUrl.replace(/https?:\/\//, '');
  const tokenStr = instance.token;
  return `${wsPrefix}://${uri}/rest/tmq?token=${tokenStr}`;
});
const topicName = computed(() => (props.topic ? props.topic : t('topic.defaultTopic')));
const tmqKey = project.isCloud ? 'TDENGINE_CLOUD_TMQ' : 'TDENGINE_TMQ';
</script>
