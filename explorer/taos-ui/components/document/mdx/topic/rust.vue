<template>
  <div>
    <h2 id="create-project">{{ t('topic.createProject') }}</h2>
    <p>{{ t('topic.step1desc', ['Rust']) }}</p>
    <pre v-highlight><code>cargo new --bin cloud-example
</code></pre>
    <p>{{ t('topic.step1desc1', ['Cargo.toml']) }}</p>
    <pre v-highlight><code class="language-toml">[package]
name = &quot;cloud-example&quot;
version = &quot;0.1.0&quot;
edition = &quot;2023&quot;

[dependencies]
taos = { version = &quot;*&quot;, default-features = false, features = [&quot;ws&quot;, &quot;ws-native-tls&quot;] }
tokio = { version = &quot;1&quot;, features = [&quot;full&quot;]}
anyhow = &quot;1.0.0&quot; 
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
    <pre v-highlight><code class="language-rust">let tmq_str = std::env::var("{{ tmqKey }}")?;
let tmq_uri = format!( "{}&\
group.id=test_group_rs&\
client.id=test_consumer_ws", tmq_str);
println!("request tmq URI is {tmq_uri}\n");
let tmq = TmqBuilder::from_dsn(tmq_uri,)?;
let mut consumer = tmq.build()?;</code></pre>
    <h2 id="subscribe-consume">{{ t('topic.step4') }}</h2>
    <p>{{ t('topic.step4desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `consumer.subscribe([&quot;${topicName}&quot;]).await?;

// consume loop
consumer
  .stream()
  .try_for_each_concurrent(10, |(offset, message)| async {
    let topic = offset.topic();
    // the vgroup id, like partition id in kafka.
    let vgroup_id = offset.vgroup_id();
    println!(&quot;* in vgroup id {vgroup_id} of topic {topic}\\n&quot;);

    if let Some(data) = message.into_data() {
      while let Some(block) = data.fetch_raw_block().await? {
        // A two-dimension matrix while each cell is a [taos::Value] object.
        let values = block.to_values();
        // Number of rows.
        assert_eq!(values.len(), block.nrows());
        // Number of columns
        assert_eq!(values[0].len(), block.ncols());
        println!(&quot;first row: {}&quot;, values[0].iter().join(&quot;, &quot;));
      }
    }
    consumer.commit(offset).await?;
    Ok(())
  })
  .await?;`
      "
    ><code class="language-rust"></code></pre>
    <h2 id="close-consumer">{{ t('topic.step5') }}</h2>
    <p>{{ t('topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-rust">consumer.unsubscribe().await;</code></pre>
    <h2 id="fullexample">{{ t('topic.step6') }}</h2>
    <p>{{ t('topic.step6desc') }}</p>
    <pre
      v-highlight="
        `use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // subscribe
  let tmq_str = std::env::var(&quot;${tmqKey}&quot;)?;
  let tmq_uri = format!( &quot;{}&\\
  group.id=test_group_rs&\\
  client.id=test_consumer_ws&quot;, tmq_str);
  println!(&quot;request tmq URI is {tmq_uri}\n&quot;);
  let tmq = TmqBuilder::from_dsn(tmq_uri,)?;
  let mut consumer = tmq.build()?;
  consumer.subscribe([&quot;${topicName}&quot;]).await?;

  // consume loop
  consumer
    .stream()
    .try_for_each_concurrent(10, |(offset, message)| async {
      let topic = offset.topic();
      // the vgroup id, like partition id in kafka.
      let vgroup_id = offset.vgroup_id();
      println!(&quot;* in vgroup id {vgroup_id} of topic {topic}\\n&quot;);

      if let Some(data) = message.into_data() {
        while let Some(block) = data.fetch_raw_block().await? {
          // A two-dimension matrix while each cell is a [taos::Value] object.
          let values = block.to_values();
          // Number of rows.
          assert_eq!(values.len(), block.nrows());
          // Number of columns
          assert_eq!(values[0].len(), block.ncols());
          println!(&quot;first row: {}&quot;, values[0].iter().join(&quot;, &quot;));
        }
      }
      consumer.commit(offset).await?;
      Ok(())
    })
    .await?;

  consumer.unsubscribe().await;

  Ok(())
}`
      "
    ><code class="language-rust"></code></pre>
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
