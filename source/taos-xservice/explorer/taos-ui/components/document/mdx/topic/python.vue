<template>
  <div>
    <h2 id="install-module">{{ t('topic.python.step1') }}</h2>
    <p>{{ t('topic.python.step1desc') }}</p>
    <el-tabs class="tab-python" model-value="pip" group-i-d="package">
      <el-tab-pane name="pip" label="Pip">
        <pre v-highlight><code>pip install -U taos-ws-py
</code></pre>
        <p>{{ t('topic.python.step1desc1') }}</p>
      </el-tab-pane>
      <el-tab-pane name="conda" label="Conda">
        <pre v-highlight><code>conda install -c conda-forge taos-ws-py
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <doc-config
      :id="'config'"
      :url="endpoint"
      :need-token="project.isCloud"
      :token="instance.token"
      :url-key="'TDENGINE_CLOUD_ENDPOINT'"
      :url-des="t('docsConfig.endpoint')"
    ></doc-config>
    <h2 id="create-consumer">{{ t('topic.step3') }}</h2>
    <p>{{ t('topic.step3desc') }}</p>
    <pre v-highlight="conSrc"><code class="language-python">
</code></pre>
    <h2 id="subscribe-consume">{{ t('topic.step4') }}</h2>
    <p>{{ t('topic.step4desc', [topicName]) }}</p>
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
    <h2 id="close-consumer">{{ t('topic.step5') }}</h2>
    <p>{{ t('topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-python"># Unsubscribe
consumer.unsubscribe()
# Close consumer
consumer.close()</code></pre>
    <h2 id="fullexample">{{ t('topic.step6') }}</h2>
    <p>{{ t('topic.step6desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `import os
from taosws import Consumer

endpoint = os.environ[&quot;${endpointKey}&quot;]
token = os.environ[&quot;${tokenKey}&quot;]

conf = {
  # auth options
  &quot;td.connect.websocket.scheme&quot;: &quot;${wsPrefix}&quot;,
  &quot;td.connect.ip&quot;: endpoint,
  &quot;td.connect.token&quot;: token,
  # consume options
  &quot;group.id&quot;: &quot;test_group_py&quot;,
  &quot;client.id&quot;: &quot;test_consumer_ws_py&quot;,
  &quot;enable.auto.commit&quot;: &quot;true&quot;,
  &quot;auto.commit.interval.ms&quot;: &quot;1000&quot;,
  &quot;auto.offset.reset&quot;: &quot;earliest&quot;,
  &quot;msg.with.table.name&quot;: &quot;true&quot;,
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
  </div>
</template>
<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { endpoint, tokenKey } from '../utils';
import { instance, project } from 'config';

const props = withDefaults(
  defineProps<{
    topic: string;
  }>(),
  {
    topic: ''
  }
);
const wsPrefix = computed(() => (instance.gatewayUrl.startsWith('https') ? 'wss' : 'ws'));
const topicName = computed(() => (props.topic ? props.topic : t('topic.defaultTopic')));
const endpointKey = project.isCloud ? 'TDENGINE_CLOUD_ENDPOINT' : 'TDENGINE_ENDPOINT';
const conSrc = computed(() => {
  return `import os
from taosws import Consumer

endpoint = os.environ["${endpointKey}"]
token = os.environ["${tokenKey}"]

conf = {
  # auth options
  "td.connect.websocket.scheme": "${wsPrefix.value}",
  "td.connect.ip": endpoint,
  "td.connect.token": token,
  # consume options
  "group.id": "test_group_py",
  "client.id": "test_consumer_ws_py",
  "enable.auto.commit": "true",
  "auto.commit.interval.ms": "1000",
  "auto.offset.reset": "earliest",
  "msg.with.table.name": "true",
}
consumer = Consumer(conf)`;
});
</script>

<style scoped lang="scss">
.tab-python {
  ::v-deep(.el-tabs__header) {
    position: unset;
    z-index: unset;
  }
}
</style>
