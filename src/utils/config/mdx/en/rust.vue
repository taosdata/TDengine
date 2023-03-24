<template>
  <div><h2 id="create-project">Create Project</h2>
<pre v-highlight><code>cargo new --bin cloud-example
</code></pre>
<h2 id="add-dependency">Add Dependency</h2>
<p>Add dependency to <code>Cargo.toml</code>. </p>
<pre v-highlight><code class="language-toml">[package]
name = &quot;cloud-example&quot;
version = &quot;0.1.0&quot;
edition = &quot;2021&quot;

[dependencies]
taos = { version = &quot;*&quot;, default-features = false, features = [&quot;ws&quot;] }
tokio = { version = &quot;1&quot;, features = [&quot;full&quot;]}
anyhow = &quot;1.0.0&quot; 
</code></pre>
<h2 id="config">Config</h2>
<p>Run this command in your terminal to save TDengine cloud token as variables:</p>
<el-tabs value="bash">
<el-tab-pane name="bash" label="Bash">

<pre v-highlight="`export TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="cmd" label="CMD">

<pre v-highlight="`set TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="powershell" label="Powershell">

<pre v-highlight="`$env:TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`"><code class="language-powershell"></code></pre>
</el-tab-pane>
</el-tabs>


<h2 id="connect">Connect</h2>
<p>Copy following code to <code>main.rs</code>.</p>
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
<p>Then you can execute <code>cargo run</code> to test the connection.  For how to write data and query data, please refer to <a href=" https://docs.tdengine.com/cloud/programming/insert/"> https://docs.tdengine.com/cloud/programming/insert/</a> and <a href="https://docs.tdengine.com/cloud/programming/query/">https://docs.tdengine.com/cloud/programming/query/</a>.</p>
<p>For more details about how to write or query data via REST API, please check <a href="https://docs.tdengine.com/cloud/programming/connector/rest-api/">REST API</a>.</p>
</div>
  </template>
  
  <script>
  import _ from 'lodash';
  export default {
    props: {
      token: {
        type: String,
        default: "",
      },
      url: {
        type: String,
        default: "",
      }
    },
    data() {
      return {};
    },
    computed: {
      jdbcURL() {
        let username = _.first(atob(this.token.replace("Basic ", "")).split(":"));
        let password = _(atob(this.token.replace("Basic ", ""))).split(":").drop(1).join(":").value();
        return "jdbc:TAOS-RS://" + this.url.replace(
          /https?:\/\//,
          ""
        ) + "?usessl=" + this.url.startsWith("https") + "&user=" + username + "&password=" + password;
      },
      goDSN() {
        return atob(this.token.replace("Basic ", "")) + "@" + (this.url.startsWith("https") ? "https" : "http") + "(" + this.url.replace(/https?:\/\//, "") + ")/";
      },
      DSN() {
        let auth = atob(this.token.replace("Basic ", ""));
        let scheme = _(this.url).split("://").first();
        let url = this.url.replace(/https?:\/\//, "");
        return scheme + "://" + auth + '@' + url;
      },
      cloud_url() {
        return this.url;
      },
      cloud_token() {
        return this.token;
      }
    },
    watch: {
      tokenList: {
        handler() {
          this.token = this.tokenList[0]?.token;
        }
      },
      immediate: true
    }
  };
  </script>
