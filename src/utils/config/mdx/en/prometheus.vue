<template>
  <div><p>Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.</p>
<p>Prometheus provides <code>remote_write</code> interface to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine&#39;s efficient writing, TDengine also provides support for this interface so that Prometheus data can be stored in TDengine via the <code>remote_write</code> interface with proper configuration to take full advantage of TDengine&#39;s efficient storage performance and clustering capabilities for time-series data.</p>
<h2 id="prerequisites">Prerequisites</h2>
<p>In your TDengine cloud instance, click &quot;Explorer&quot; on the left panel, then click &quot;+&quot; besides Databases, to create a new database named as &quot;prometheus_data&quot;. Then execute <code>show databases</code> to confirm the database has been created successfully.</p>
<h2 id="install-prometheus">Install Prometheus</h2>
<p>Supposed that you use Linux system with architecture amd64:</p>
<ol>
<li>Download<pre v-highlight><code>wget https://github.com/prometheus/prometheus/releases/download/v2.37.0/prometheus-2.37.0.linux-amd64.tar.gz
</code></pre>
</li>
<li>Decompress and rename<pre v-highlight><code>tar xvfz prometheus-*.tar.gz &amp;&amp; mv prometheus-2.37.0.linux-amd64 prometheus
</code></pre>
</li>
<li>Change to directory prometheus<pre v-highlight><code>cd prometheus
</code></pre>
</li>
</ol>
<p>Then Prometheus is installed in current directory. For more installation options, please refer to the <a href="https://prometheus.io/docs/prometheus/latest/installation/">official documentation</a>.</p>
<h2 id="configure-prometheus">Configure Prometheus</h2>
<p>Configuring Prometheus is done by editing the Prometheus configuration file <code>prometheus.yml</code> (If you followed previous steps, you can find prometheus.xml in current directory).</p>
<pre v-highlight="`remote_write:
  - url: &quot;${cloud_url}/prometheus/v1/remote_write/prometheus_data?token=${cloud_token}&quot;

remote_read:
  - url: &quot;${cloud_url}/prometheus/v1/remote_read/prometheus_data?token=${cloud_token}&quot;
    remote_timeout: 10s
    read_recent: true
`"><code class="language-yaml"></code></pre>
<p>The resulting configuration will collect data about prometheus itself from its own HTTP metrics endpoint, and store data to TDengine Cloud.</p>
<h2 id="start-prometheus">Start Prometheus</h2>
<pre v-highlight><code>./prometheus --config.file prometheus.yml
</code></pre>
<p>Prometheus should start up. It also started a web server at <a href="http://localhost:9090">http://localhost:9090</a>. If you want to access the web server from a browser which is not running on the same host as Prometheus, please change <code>localhost</code> to correct hostname, FQDN or IP address, depending on your network environment.</p>
<h2 id="verify-remote-write">Verify Remote Write</h2>
<p>Log in TDengine Cloud, click &quot;Explorer&quot; on the left navigation bar. You will see metrics collected by prometheus.</p>
<p><img src="../assets/prometheus/prometheus_data.webp" alt="TDengine prometheus remote_write result"></p>
<p>:::note</p>
<ul>
<li>TDengine will automatically create unique IDs for sub-table names by the rule.
:::</li>
</ul>
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