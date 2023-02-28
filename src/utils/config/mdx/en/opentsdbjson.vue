<template>
  <div><p>In this section we will explain how to write into TDengine cloud service using schemaless OpenTSDB JSON protocols over REST interface.</p>
<h2 id="config">Config</h2>
<p>Run this command in your terminal to save the TDengine cloud token and URL as variables:</p>
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

<h2 id="insert">Insert</h2>
<p>You can use any client that supports the http protocol to access the RESTful interface address <code>&lt;cloud_url&gt;/opentsdb/v1/put</code> to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:</p>
<pre v-highlight><code class="language-text">/opentsdb/v1/put/json/&lt;db&gt;?token=&lt;cloud_token&gt;
</code></pre>
<h2 id="insert-example">Insert Example</h2>
<pre v-highlight="`curl --request POST &quot;$TDENGINE_CLOUD_URL/opentsdb/v1/put/json/${db_name}?token=$TDENGINE_CLOUD_TOKEN&quot; --data-binary &quot;{\&quot;metric\&quot;:\&quot;meter_current\&quot;,\&quot;timestamp\&quot;:1646846400,\&quot;value\&quot;:10.3,\&quot;tags\&quot;:{\&quot;groupid\&quot;:2,\&quot;location\&quot;:\&quot;Beijing\&quot;,\&quot;id\&quot;:\&quot;d1001\&quot;}}&quot;
`"><code class="language-bash"></code></pre>
<h2 id="query-example-with-sql">Query Example with SQL</h2>
<ul>
<li><code>meter_current</code> is the super table name.</li>
<li>you can filter data by tag, like:<code>where groupid=2</code>.</li>
</ul>
<pre v-highlight="`curl -L -d &quot;select * from ${db_name}.meter_current where groupid=2&quot; $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
`"><code class="language-bash"></code></pre>
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
