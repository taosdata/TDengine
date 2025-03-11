<template>
  <div>
    <h2 id="create-project">{{ t('connector.rust.step1') }}</h2>
    <pre v-highlight><code>cargo new --bin cloud-example
</code></pre>
    <h2 id="add-dependency">{{ t('connector.rust.step2') }}</h2>
    <p>{{ t('connector.rust.step2desc') }}</p>
    <pre v-highlight><code class="language-toml">[package]
name = &quot;cloud-example&quot;
version = &quot;0.1.0&quot;
edition = &quot;2021&quot;

[dependencies]
taos = { version = &quot;*&quot;, default-features = false, features = [&quot;ws&quot;] }
tokio = { version = &quot;1&quot;, features = [&quot;full&quot;]}
anyhow = &quot;1.0.0&quot; 
</code></pre>
    <doc-config :need-token="false" :url="dsn" :url-key="dsnKey" :url-des="t('docsConfig.dsn')"></doc-config>
    <h2 id="connect">{{ t('connector.rust.step4') }}</h2>
    <p>{{ t('connector.rust.step41desc') }}</p>
    <pre v-highlight><code class="language-rust">use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let dsn = std::env::var(&quot;TDENGINE_CLOUD_DSN&quot;)?;
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;
    let _ = taos.query(&quot;show databases&quot;).await?;
    println!(&quot;Connected&quot;);
    Ok(())
}
</code></pre>
    <p>{{ t('connector.rust.step42desc') }}</p>
    <p>
      {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/programming/insert/`">{{ `${docs.urlPrefix}/programming/insert/` }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/programming/query/`">{{ `${docs.urlPrefix}/programming/query/` }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { dsn, dsnKey } from '../utils';
import { docs } from 'config';
</script>
