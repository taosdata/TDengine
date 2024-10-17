<template>
  <div>
    <p>{{ $t("docs.topic.topdesc", [org, instance, topic]) }}</p>
    <h2 id="rust-create-project">{{ $t("docs.topic.rust.step1") }}</h2>
    <p>{{ $t("docs.topic.rust.step1desc") }}</p>
    <pre v-highlight><code>cargo new --bin tdengine-sub-sample-rust
</code></pre>
    <p>{{ $t("docs.topic.rust.step1desc1") }}</p>
    <pre v-highlight="`[package]
name = &quot;tdengine-sub-sample-rust&quot;
version = &quot;0.1.0&quot;
edition = &quot;2021&quot;

[dependencies]
taos = { version = &quot;*&quot;, default-features = false, features = [&quot;ws&quot;, &quot;ws-native-tls&quot;] }
tokio = { version = &quot;1&quot;, features = [&quot;full&quot;]}
anyhow = &quot;1.0.0&quot; `"><code class="language-toml">
</code></pre>
    <doc-config
      :id="'rust-config'"
      :url="tmq"
      :need-token="false"
      :url-key="'TDENGINE_TMQ'"
      :url-des="$t('component.docConfig.tmq')"
    ></doc-config>
    <h2 id="rust-create-consumer">{{ $t("docs.topic.step3") }}</h2>
    <p>{{ $t("docs.topic.step3desc") }}</p>
    <pre
      v-highlight
    ><code class="language-rust">let tmq_str = std::env::var("TDENGINE_TMQ")?;
let tmq_uri = format!( "{}?\
group.id=test_group_rs&\
client.id=test_consumer_ws", tmq_str);
println!("request tmq URI is {tmq_uri}");
let tmq = TmqBuilder::from_dsn(tmq_uri,)?;
let mut consumer = tmq.build()?;</code></pre>
    <h2 id="rust-subscribe-consume">{{ $t("docs.topic.step4") }}</h2>
    <p>{{ $t("docs.topic.step4desc", [topicName]) }}</p>
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
    <h2 id="rust-close-consumer">{{ $t("docs.topic.step5") }}</h2>
    <p>{{ $t("docs.topic.step5desc", [topicName]) }}</p>
    <pre
      v-highlight
    ><code class="language-rust">consumer.unsubscribe().await;</code></pre>
    <h2 id="rust-fullexample">{{ $t("docs.topic.step6") }}</h2>
    <p>{{ $t("docs.topic.step6desc",[topicName]) }}</p>
    <pre
      v-highlight="
        `use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // subscribe
  let tmq_str = std::env::var(&quot;TDENGINE_TMQ&quot;)?;
  let tmq_uri = format!( &quot;{}?\\
  group.id=test_group_rs&\\
  client.id=test_consumer_ws&quot;, tmq_str);
  println!(&quot;request tmq URI is {tmq_uri}&quot;);
  let tmq = TmqBuilder::from_dsn(tmq_uri,)?;
  let mut consumer = tmq.build().await?;
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
    <p v-if="!isOEM">
      {{ $t("docs.topic.enddesc") }}
      <a :href="`${$t('urlPart')}/develop/tmq/#data-subscription`">{{
        $t("docs.topic.enddesc2")
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
      const wsPrefix = this.url.startsWith("https") ? "wss" : "ws";
      const uri = this.url.replace(/https?:\/\//, "");
      const tokenStr = this.token;
      return `taos+${wsPrefix}://${this.user}:${this.password}@${uri}`;
    },
    org() {
      return this.$store.state.currentOrganization?.orgName || "";
    },
    instance() {
      return this.$store.state.app?.current_cluster?.alias || "";
    },
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    topicName() {
      return this.topic ? this.topic : this.$t("docs.topic.defaultTopic");
    },
  },
};
</script>
