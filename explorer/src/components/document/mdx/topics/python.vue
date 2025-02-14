<template>
  <div>
    <p>{{ $t("docs.topic.topdesc", [org, instance, topic]) }}</p>
    <h2 id="py-install-module">{{ $t("docs.topic.python.step1") }}</h2>
    <p>{{ $t("docs.topic.python.step1desc") }}</p>
    <el-tabs class="tab-python" model-value="pip" group-i-d="package">
      <el-tab-pane name="pip" label="Pip">
        <pre v-highlight><code>pip install -U taos-ws-py
</code></pre>
        <p>{{ $t("docs.topic.python.step1desc1") }}</p>
      </el-tab-pane>
      <el-tab-pane name="conda" label="Conda">
        <pre v-highlight><code>conda install -c conda-forge taos-ws-py
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <DocConfig
      :id="'py-config'"
      :url="endpoint"
      :token="token"
      :url-key="'TDENGINE_ENDPOINT'"
      :url-des="$t('docs.docConfig.endpoint')"
    ></DocConfig>
    <h2 id="py-create-consumer">{{ $t("docs.topic.step3") }}</h2>
    <p>{{ $t("docs.topic.step3desc") }}</p>
    <pre v-highlight="conSrc"><code class="language-python">
</code></pre>
    <h2 id="py-subscribe-consume">{{ $t("docs.topic.step4") }}</h2>
    <p>{{ $t("docs.topic.step4desc", [topicName]) }}</p>
    <pre
      v-highlight="
        `consumer.subscribe([&quot;${topicName}&quot;])

while 1:
  message = consumer.poll(timeout=1.0)
  if message:
    id = message.vgroup()
    topic = message.topic()
    database = message.database()

    for block in message:
      nrows = block.nrows()
      ncols = block.ncols()
      for row in block:
        print(row)
        values = block.fetchall()
        print(nrows, ncols)
  else:
    break
`
      "
    ><code class="language-python"></code></pre>
    <h2 id="py-close-consumer">{{ $t("docs.topic.step5") }}</h2>
    <p>{{ $t("docs.topic.step5desc", [topicName]) }}</p>
    <pre v-highlight><code class="language-python">consumer.close()</code></pre>
    <h2 id="py-fullexample">{{ $t("docs.topic.step6") }}</h2>
    <p>{{ $t("docs.topic.step6desc", [topicName]) }}</p>
    <pre
      v-highlight="
        `import os
from taosws import Consumer

endpoint = os.environ[&quot;TDENGINE_ENDPOINT&quot;]

conf = {
  # auth options
  &quot;td.connect.websocket.scheme&quot;: &quot;${wsPrefix}&quot;,
  &quot;td.connect.ip&quot;: &quot;${endpoint}&quot;,
  &quot;td.connect.user&quot;: &quot;${user}&quot;,
  &quot;td.connect.pass&quot;: &quot;${password}&quot;,
  # consume options
  &quot;group.id&quot;: &quot;test_group_py&quot;,
  &quot;client.id&quot;: &quot;test_consumer_ws_py&quot;,
}
consumer = Consumer(conf)

consumer.subscribe([&quot;${topicName}&quot;])

while 1:
  message = consumer.poll(timeout=1.0)
  if message:
    id = message.vgroup()
    topic = message.topic()
    database = message.database()

    for block in message:
      nrows = block.nrows()
      ncols = block.ncols()
      for row in block:
          print(row)
      values = block.fetchall()
      print(nrows, ncols)
  else:
    break

consumer.close()`
      "
    ><code class="language-python"></code></pre>
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

const { $IS_OEM } = inject("globalCustomProperties") as GlobalCustomProperties;
const { t } = useI18n()
const store = useStore()
const props = defineProps<DocsProps>()

const endpoint = computed(() => {
  return props.url.replace(/https?:\/\//, "");
})
const wsPrefix = computed(() => {
  return props.url.startsWith("https") ? "wss" : "ws";
})
const org = computed(() => {
  return store.state.currentOrganization?.orgName || "";
})
const instance = computed(() => {
  return store.state.app?.current_cluster?.alias || "";
})
const conSrc = computed(() => {
  return `import os
from taosws import Consumer

endpoint = os.environ["TDENGINE_ENDPOINT"]

conf = {
  # auth options
  "td.connect.websocket.scheme": "${wsPrefix.value}",
  "td.connect.ip": "${endpoint.value}",
  "td.connect.user": "${props.user}",
  "td.connect.pass": "${props.password}",
  # consume options
  "group.id": "test_group_py",
  "client.id": "test_consumer_ws_py",
}
consumer = Consumer(conf)`;
})
const topicName = computed(() => {
  return props.topic ? props.topic : t("docs.topic.defaultTopic");
})
</script>

<style scoped lang="scss">
.tab-python {
  :deep(.el-tabs__header) {
    position: unset;
    z-index: unset;
  }
}
</style>
