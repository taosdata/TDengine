<template>
  <div><p>In this section we will explain how to write into TDengine cloud service using schemaless InfluxDB line protocols over REST interface.</p>
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
<p>You can use any client that supports the http protocol to access the RESTful interface address <code>&lt;cloud_url&gt;/influxdb/v1/write</code> to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:</p>
<pre v-highlight><code class="language-text">/influxdb/v1/write?db=&lt;db_name&gt;&amp;token=&lt;cloud_token&gt;
</code></pre>
<p>Support InfluxDB query parameters as follows.</p>
<ul>
<li><code>db</code> Specifies the database name used by TDengine</li>
<li><code>precision</code> The time precision used by TDengine<ul>
<li>ns - nanoseconds</li>
<li>u - microseconds</li>
<li>ms - milliseconds</li>
<li>s - seconds</li>
<li>m - minutes</li>
<li>h - hours</li>
</ul>
</li>
</ul>
<h2 id="insert-example">Insert Example</h2>
<pre v-highlight="`curl --request POST &quot;$TDENGINE_CLOUD_URL/influxdb/v1/write?db=${db_name}&amp;token=$TDENGINE_CLOUD_TOKEN&amp;precision=ns&quot; --data-binary &quot;measurement,host=host1 field1=2i,field2=2.0 1577846800001000001&quot;
`"><code class="language-bash"></code></pre>
<h2 id="query-example-with-sql">Query Example with SQL</h2>
<ul>
<li><code>measurement</code> is the super table name.</li>
<li>you can filter data by tag, like:<code>where host=&quot;host1&quot;</code>.</li>
</ul>
<pre v-highlight="`curl -L -d &quot;select * from ${db_name}.measurement where host=\&quot;host1\&quot;&quot; $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
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
