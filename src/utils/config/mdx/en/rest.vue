<template>
  <div><h2 id="config">Config</h2>
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

<h2 id="usage">Usage</h2>
<p>The TDengine REST API is based on standard HTTP protocol and provides an easy way to access TDengine. As an example, the code below is to construct an HTTP request with the URL, the token and an SQL command and run it with the command line utility <code>curl</code>.</p>
<pre v-highlight="`curl -L \
  -d &quot;select name, ntables, status from information_schema.ins_databases;&quot; \
  $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
`"><code class="language-bash"></code></pre>
<h2 id="schemaless">Schemaless</h2>
<h3 id="influxdb-line-protocol">InfluxDB Line Protocol</h3>
<p>You can use any client that supports the http protocol to access the RESTful interface address <code>${TDENGINE_CLOUD_URL}/influxdb/v1/write</code> to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:</p>
<pre v-highlight><code class="language-text">/influxdb/v1/write?db=&lt;DB_NAME&gt;&amp;token=${TDENGINE_CLOUD_TOKEN}
</code></pre>
<p>Support InfluxDB query parameters as follows.</p>
<ul>
<li><code>db</code> Specifies the database name used by TDengine</li>
<li><code>precision</code> The time precision used by TDengine</li>
</ul>
<p>Note: InfluxDB token authorization is not supported at present. Only Basic authorization and query parameter validation are supported.</p>
<h3 id="opentsdb-json-and-telnet-protocol">OpenTSDB Json and Telnet Protocol</h3>
<p>You can use any client that supports the http protocol to access the RESTful interface address <code>${TDENGINE_CLOUD_URL}/opentsdb/v1/put</code> to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:</p>
<pre v-highlight><code class="language-text">/opentsdb/v1/put/json/&lt;db&gt;?token=${TDENGINE_CLOUD_TOKEN}
/opentsdb/v1/put/telnet/&lt;db&gt;?token=${TDENGINE_CLOUD_TOKEN}
</code></pre>
</div>
  </template>
  
  <script>
  export default {
    props: {
      token: {
        type: String,
        default: "",
      },
      url:{
          type:String,
          default:"",
      }
    },
    data(){
      return {};
    },
    computed: {
      jdbcURL() {
        return "jdbc:TAOS-RS://"+this.url.replace(
          /https?:\/\//,
          ""
        )+"?usessl="+this.url.startsWith("https")+"&token=" + this.token;
      },
      goDSN() {
        return (this.url.startsWith("https") ? "https" : "http") + "("+this.url.replace(/https?:\/\//, "")+")/?token=" + this.token;
      },
      DSN() {
        return this.url + "?token=" + this.token;
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