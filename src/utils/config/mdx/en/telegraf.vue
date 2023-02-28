<template>
  <div><p>Telegraf is an open-source, metrics collection software. Telegraf can collect the operation information of various components without having to write any scripts to collect regularly, reducing the difficulty of data acquisition.</p>
<p>Telegraf&#39;s data can be written to TDengine by simply adding the output configuration of Telegraf to the URL corresponding to taosAdapter and modifying several configuration items. The presence of Telegraf data in TDengine can take advantage of TDengine&#39;s efficient storage query performance and clustering capabilities for time-series data.</p>
<h2 id="prerequisites">Prerequisites</h2>
<p>Before telegraf can write data into TDengine cloud service, you need to firstly manually create a database. Log in TDengine Cloud, click &quot;Explorer&quot; on the left navigation bar, then click the &quot;+&quot; button besides &quot;Databases&quot; to add a database named as &quot;telegraf&quot; using all default parameters.</p>
<h2 id="install-telegraf">Install Telegraf</h2>
<p>Supposed that you use Ubuntu system:</p>
<pre v-highlight="`wget -q https://repos.influxdata.com/influxdb.key
echo &#39;23a1c8836f0afc5ed24e0486339d7cc8f6790b83886c4c96995b88a061c5bb5d influxdb.key&#39; | sha256sum -c &amp;&amp; cat influxdb.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdb.gpg } /dev/null
echo &#39;deb [signed-by=/etc/apt/trusted.gpg.d/influxdb.gpg] https://repos.influxdata.com/debian stable main&#39; | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo apt-get update &amp;&amp; sudo apt-get install telegraf
`"><code class="language-bash"></code></pre>
<p>After installation, telegraf service should have been started. Lets stop it:</p>
<pre v-highlight="`sudo systemctl stop telegraf
`"><code class="language-bash"></code></pre>
<p>For installation instructions on other platforms please refer to the <a href="https://docs.influxdata.com/telegraf/v1.23/install/">official documentation</a>.</p>
<h2 id="configure">Configure</h2>
<p>Run this command in your terminal to save TDengine cloud token and URL as variables:</p>
<pre v-highlight="`export TDENGINE_CLOUD_URL=&quot;${url}&quot;
export TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
`"><code class="language-bash"></code></pre>
<p>Then run this command to generate new telegraf.conf.</p>
<pre v-highlight="`telegraf --sample-config --input-filter cpu:mem --output-filter http } telegraf.conf
`"><code class="language-bash"></code></pre>
<p>Edit section &quot;outputs.http&quot;.</p>
<pre v-highlight><code class="language-toml">[[outputs.http]]
  url = &quot;${TDENGINE_CLOUD_URL}/influxdb/v1/write?db=telegraf&amp;token=${TDENGINE_CLOUD_TOKEN}&quot;
  method = &quot;POST&quot;
  timeout = &quot;5s&quot;
  data_format = &quot;influx&quot;
  influx_max_line_bytes = 250
</code></pre>
<p>The resulting configuration will collect CPU and memory data and sends it to TDengine database named &quot;telegraf&quot;. Database &quot;telegraf&quot; will be created automatically if it dose not exist in advance.</p>
<h2 id="start-telegraf">Start Telegraf</h2>
<p>Start telegraf using new generated telegraf.conf file.</p>
<pre v-highlight="`telegraf --config telegraf.conf
`"><code class="language-bash"></code></pre>
<h2 id="verify">Verify</h2>
<ul>
<li>Check weather database &quot;telegraf&quot; exist by executing:</li>
</ul>
<pre v-highlight><code class="language-sql">show databases;
</code></pre>
<p><img src="../assets/telegraf/telegraf-show-databases.webp" alt="TDengine show telegraf databases"></p>
<p>Check weather super table cpu and mem exist:</p>
<pre v-highlight><code class="language-sql">show telegraf.stables;
</code></pre>
<p><img src="../assets/telegraf/telegraf-show-stables.webp" alt="TDengine Cloud show telegraf stables"></p>
<p>:::note</p>
<ul>
<li>Telegraf collects the running status measurements of current system. You can enable <a href="https://docs.influxdata.com/telegraf/v1.22/plugins/">input plugins</a> to insert <a href="https://docs.influxdata.com/telegraf/v1.24/data_formats/input/">other formats</a> data to Telegraf then forward to TDengine.</li>
<li>TDengine take influxdb format data and create unique ID for table names by the rule.
The user can configure <code>smlChildTableName</code> parameter to generate specified table names if he/she needs. And he/she also need to insert data with specified data format.
For example, Add <code>smlChildTableName=tname</code> in the taos.cfg file. Insert data <code>st,tname=cpu1,t1=4 c1=3 1626006833639000000</code> then the table name will be cpu1. If there are multiple lines has same tname but different tag_set, the first line&#39;s tag_set will be used to automatically creating table and ignore other lines. Please refer to <a href="/reference/schemaless/#Schemaless-Line-Protocol">TDengine Schemaless</a>
:::</li>
</ul>
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
