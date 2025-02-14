<template>
  <div>
    <p>{{ $t("docs.topic.topdesc", [org, instance, topic]) }}</p>
    <h2 id="go-initialize">{{ $t("docs.topic.go.step1") }}</h2>
    <p>{{ $t("docs.topic.go.step1desc") }}</p>
    <pre v-highlight><code>go mod init tdengine.com/example</code></pre>
    <pre v-highlight><code class="language-go-mod">module tdengine.com/example

go 1.17

require github.com/taosdata/driver-go/v3 latest
</code></pre>
    <DocConfig
      :id="'go-config'"
      :url="tmq"
      :need-token="false"
      :url-key="'TDENGINE_TMQ'"
      :url-des="$t('docs.docConfig.tmq')"
    ></DocConfig>
    <h2 id="go-create-consumer">{{ $t("docs.topic.step3") }}</h2>
    <p>{{ $t("docs.topic.step3desc") }}</p>
    <pre v-highlight><code class="language-go">import (
  "github.com/taosdata/driver-go/v3/common"
  tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
  "github.com/taosdata/driver-go/v3/ws/tmq"
)
tmqStr := os.Getenv(`TDENGINE_TMQ`)
consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
  "ws.url":                tmqStr,
  "ws.message.channelLen": uint(0),
  "ws.message.timeout":    common.DefaultMessageTimeout,
  "ws.message.writeWait":  common.DefaultWriteWait,
  "td.connect.user":       "{{ user }}",
  "td.connect.pass":       "{{ password }}",
  "group.id":              "test_group",
  "client.id":             "test_consumer_ws",
  "auto.offset.reset":     "earliest",
})
if err != nil {
  panic(err)
}
</code></pre>
    <h2 id="go-subscribe-consume">{{ $t("docs.topic.step4") }}</h2>
    <p>{{ $t("docs.topic.step4desc", [topicName]) }}</p>
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
    <h2 id="go-close-consumer">{{ $t("docs.topic.step5") }}</h2>
    <p>{{ $t("docs.topic.step5desc", [topicName]) }}</p>
    <pre v-highlight><code class="language-go">consumer.Close()</code></pre>
    <h2 id="go-fullexample">{{ $t("docs.topic.step6") }}</h2>
    <p>{{ $t("docs.topic.step6desc", [topicName]) }}</p>
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
  tmqStr := os.Getenv(&quot;TDENGINE_TMQ&quot;)
  consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
    &quot;ws.url&quot;:                tmqStr,
    &quot;ws.message.channelLen&quot;: uint(0),
    &quot;ws.message.timeout&quot;:    common.DefaultMessageTimeout,
    &quot;ws.message.writeWait&quot;:  common.DefaultWriteWait,
    &quot;td.connect.user&quot;:       &quot;${user}&quot;,
    &quot;td.connect.pass&quot;:       &quot;${password}&quot;,
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
    <p v-if="!$IS_OEM">
      {{ $t("docs.topic.enddesc") }}
      <a :href="`${$t('urlPart')}/develop/tmq/#data-subscription`">{{
        $t("docs.topic.enddesc2")
      }}</a>
      {{ $t("docs.topic.enddesc1") }}
    </p>
  </div>
</template>

<script setup lang="ts">
import DocConfig from "@/components/document/commonConfig.vue";
import { DocsProps } from '../utils'
import { useStore } from "vuex";
const { t } = useI18n()
const store = useStore()
const { $IS_OEM } = inject("globalCustomProperties") as GlobalCustomProperties;

const props = defineProps<DocsProps>()

const tmq = computed(() => {
  // root:taosdata@ws(localhost:6041)
  const wsPrefix = props.url.startsWith("https") ? "wss" : "ws";
  const uri = props.url.replace(/https?:\/\//, "");
  return `${wsPrefix}://${uri}/rest/tmq`;
})
const org = computed(() => {
  return store.state.currentOrganization?.orgName || "";
})
const instance = computed(() => {
  return store.state.app?.current_cluster?.alias || "";
})
const topicName = computed(() => {
  return props.topic ? props.topic : t("docs.topic.defaultTopic");
})
</script>
