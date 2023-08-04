<template>
  <div>
    <h2 id="create-project">{{ $t("docs.connector.rust.step1") }}</h2>
    <pre v-highlight><code>cargo new --bin cloud-example
</code></pre>
    <h2 id="add-dependency">{{ $t("docs.connector.rust.step2") }}</h2>
    <p>{{ $t("docs.connector.rust.step2desc") }}</p>
    <pre v-highlight><code class="language-toml">[package]
name = &quot;cloud-example&quot;
version = &quot;0.1.0&quot;
edition = &quot;2021&quot;

[dependencies]
taos = { version = &quot;*&quot;, default-features = false, features = [&quot;ws&quot;] }
tokio = { version = &quot;1&quot;, features = [&quot;full&quot;]}
anyhow = &quot;1.0.0&quot; 
</code></pre>
    <h2 id="config">{{ $t("docs.connector.rust.step3") }}</h2>
    <p>{{ $t("component.docConfig.content", [" DSN "]) }}</p>
    <el-tabs value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="connect">{{ $t("docs.connector.rust.step4") }}</h2>
    <p>{{ $t("docs.connector.rust.step41desc") }}</p>
    <pre v-highlight><code class="language-rust">use anyhow::Result;
use taos::*;

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let dsn = std::env::var(&quot;TDENGINE_DSN&quot;)?;
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;
    let _ = taos.query(&quot;show databases&quot;).await?;
    println!(&quot;Connected&quot;);
    Ok(())
}
</code></pre>
    <p>{{ $t("docs.connector.rust.step42desc") }}</p>
    <p>
      {{ $t("docs.connector.bottom2") }}
      <a :href="`https://docs.${urlPart}.com/develop/insert-data/`">{{
        `https://docs.${urlPart}.com/develop/insert-data/`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`https://docs.${urlPart}.com/develop/query-data/`">{{
        `https://docs.${urlPart}.com/develop/query-data/`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a :href="`https://docs.${urlPart}.com/${restapi}/rest-api/`">REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script>
export default {
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
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
  data() {
    return {};
  },
  computed: {
    DSN() {
      // https://crates.io/crates/mdsn
      return `taos://${this.user}:${this.password}@${this.url.replace(/https?:\/\//, "")}`
    },
    urlPart() {
      return navigator.language.includes('en') ? "tdengine" : "taosdata";
    },
    restapi() {
      return navigator.language.includes('en') ? "reference" : "connector";
    },
  },
};
</script>
