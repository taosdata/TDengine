<template>
  <div><h2 id="install-connector">Install Connector</h2>
<pre v-highlight="`npm install @tdengine/rest
`"><code class="language-bash"></code></pre>
<h2 id="config">Config</h2>
<p>Run this command in your terminal to save TDengine cloud token as variables:</p>
<el-tabs value="bash">
<el-tab-pane name="bash" label="Bash">

<pre v-highlight="`export TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
export TDENGINE_CLOUD_URL=&quot;${url}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="cmd" label="CMD">

<pre v-highlight="`set TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
set TDENGINE_CLOUD_URL=&quot;${url}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="powershell" label="Powershell">

<pre v-highlight="`$env:TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
$env:TDENGINE_CLOUD_URL=&quot;${url}&quot;
`"><code class="language-powershell"></code></pre>
</el-tab-pane>
</el-tabs>



<h2 id="connect">Connect</h2>
<pre v-highlight><code class="language-javascript">const { options, connect } = require(&quot;@tdengine/rest&quot;);

async function test() {
  options.url = process.env.TDENGINE_CLOUD_URL;
  options.query = { token: process.env.TDENGINE_CLOUD_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query(&quot;show databases&quot;);
    res.toString();
  } catch (err) {
    console.log(err);
  }
}

test();
</code></pre>
<p>For how to write data and query data, please refer to <a href=" https://docs.tdengine.com/cloud/programming/insert/"> https://docs.tdengine.com/cloud/programming/insert/</a> and <a href="https://docs.tdengine.com/cloud/programming/query/">https://docs.tdengine.com/cloud/programming/query/</a>.</p>
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
