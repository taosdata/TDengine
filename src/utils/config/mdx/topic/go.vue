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
    <doc-config
      :id="'go-config'"
      :url="tmq"
      :need-token="false"
      :url-key="'TDENGINE_TMQ'"
      :url-des="$t('component.docConfig.tmq')"
    ></doc-config>
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
    <p v-if="!isOEM">
      {{ $t("docs.topic.enddesc") }}
      <a :href="`https://docs.${urlPart}.com/develop/tmq/#data-subscription`">{{
        `https://docs.${urlPart}.com/develop/tmq/#data-subscription`
      }}</a>
      {{ $t("docs.topic.enddesc1") }}
    </p>
  </div>
</template>

<script>
import DocConfig from "@/components/DocConfig/index.vue";
import { IsAliyun } from "@/const";
export default {
  components: { DocConfig },
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
    topic: {
      type: String,
      default: "",
    },
    user: {
      type: String,
      default: ''
    },
    password: {
      type: String,
      default: ''
    }
  },
  data(){
    return {
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    }
  },
  computed: {
    tmq() {
      // root:taosdata@ws(localhost:6041)
      const wsPrefix = this.url.startsWith("https") ? "wss" : "ws";
      let uri = this.url.replace(/https?:\/\//, "");
      // const tokenStr = this.token;
      return `${this.user}:${this.password}@${wsPrefix}(${uri})`;
    },
    org() {
      return this.$store.state.currentOrganization?.orgName || "";
    },
    instance() {
      return this.$store.state.app?.current_cluster?.alias || "";
    },
    urlPart() {
      return navigator.language.includes('en') ?"tdengine": "taosdata";
    },
    topicName() {
      return this.topic ? this.topic : this.$t("docs.topic.defaultTopic");
    },
  },
};
</script>
