<template>
  <div>
    <h2 id="install-module">{{ t('topic.node.step1') }}</h2>
    <pre
      v-highlight="
        `npm install @tdengine/websocket
`
      "
    ><code class="language-bash"></code></pre>
    <doc-config
      :id="'config'"
      :url="tmq"
      :need-token="false"
      :url-key="'TDENGINE_CLOUD_TMQ'"
      :url-des="t('docsConfig.endpoint')"
    ></doc-config>
    <h2 id="create-consumer">{{ t('topic.step3') }}</h2>
    <p>{{ t('topic.step3desc') }}</p>
    <pre v-highlight><code class="language-javascript">const url = os.environ['TDENGINE_CLOUD_TMQ'];
const configMap = new Map([
  [taos.TMQConstants.GROUP_ID, 'gId'],
  [taos.TMQConstants.CLIENT_ID, 'clientId'],
  [taos.TMQConstants.AUTO_OFFSET_RESET, 'earliest'],
  [taos.TMQConstants.WS_URL, url],
  [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
  [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000'],
]);
// create consumer
const consumer = await taos.tmqConnect(configMap);
      
</code></pre>
    <h2 id="subscribe-consume">{{ t('topic.step4') }}</h2>
    <p>{{ t('topic.step4desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-js">await consumer.subscribe([&quot;{{topicName}}&quot;])
// poll
for (let i = 0; i &lt; 100; i++) {
  let res = await consumer.poll(1000);
  for (let [key, value] of res) {
    // Add your data processing logic here
    console.log(\`data: \${JSON.stringify(value, replacer)}\`);
  }
  // commit
  await consumer.commit();
}

// Custom replacer function to handle BigInt serialization
function replacer(key, value) {
  if (typeof value === 'bigint') {
    return value.toString(); // Convert BigInt to string
  }
  return value;
}</code></pre>
    <h2 id="close-consumer">{{ t('topic.step5') }}</h2>
    <p>{{ t('topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-javascript">await consumer.unsubscribe();
await consumer.close();</code></pre>
    <h2 id="fullexample">{{ t('topic.step6') }}</h2>
    <p>{{ t('topic.step6desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-javascript">const taos = require('@tdengine/websocket');

const url = process.env.TDENGINE_CLOUD_TMQ;
const topic = {{ topicName }};
const topics = [topic];
const groupId = 'group1';
const clientId = 'client2';

async function createConsumer() {
  let configMap = new Map([
    [taos.TMQConstants.GROUP_ID, groupId],
    [taos.TMQConstants.CLIENT_ID, clientId],
    [taos.TMQConstants.AUTO_OFFSET_RESET, 'earliest'],
    [taos.TMQConstants.WS_URL, url],
    [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
    [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000'],
  ]);
  try {
    // create consumer
    let consumer = await taos.tmqConnect(configMap);
    console.log(
      `Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`
    );
    return consumer;
  } catch (err) {
    console.error(
      `Failed to create websocket consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
    );
    throw err;
  }
}

async function testConsumer() {
  let consumer = await createConsumer();

  try {
    // subscribe
    await consumer.subscribe(topics);
    console.log(`Subscribe topics successfully, topics: ${topics}`);

    for (let i = 0; i  &lt; 5000; i++) {
      // poll
      let res = await consumer.poll(1000);
      for (let [key, value] of res) {
        // Add your data processing logic here
        console.log(`data: ${JSON.stringify(value, replacer)}`);
      }
      // commit
      await consumer.commit();
    }

    // seek
    let assignment = await consumer.assignment();
    await consumer.seekToBeginning(assignment);
    console.log('Assignment seek to beginning successfully');

    // clean
    await consumer.unsubscribe();
  } catch (err) {
    console.error(
      `Failed to create websocket consumer, ErrCode: ${err.code}, ErrMessage: ${err.message}`
    );
    throw err;
  } finally {
    if (consumer) {
      await consumer.close();
    }
    taos.destroy();
  }
}

// Custom replacer function to handle BigInt serialization
function replacer(key, value) {
  if (typeof value === 'bigint') {
    return value.toString(); // Convert BigInt to string
  }
  return value;
}

testConsumer();
</code></pre>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { dsn } from '../utils';

const props = defineProps<{
  topic?: string;
}>();
const { t } = useI18n();
const topicName = computed(() => {
  return props.topic ? props.topic : t('topic.defaultTopic');
});
const tmq = computed(() => dsn.value.replace('http', 'ws'));
</script>

<style scoped lang="scss">
.tab-javascript {
  ::v-deep(.el-tabs__header) {
    position: unset;
    z-index: unset;
  }
}
</style>
