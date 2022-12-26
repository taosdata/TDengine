<template>
  <div><p>TDengine can be quickly integrated with the open-source data visualization system <a href="https://www.grafana.com/">Grafana</a> to build a data monitoring and alerting system. The whole process does not require any code development. And you can visualize the contents of the data tables in TDengine on a dashboard.</p>
<p>You can learn more about using the TDengine plugin on <a href="https://github.com/taosdata/grafanaplugin/blob/master/README.md">GitHub</a>.</p>
<h2 id="install-grafana">Install Grafana</h2>
<p>TDengine currently supports Grafana versions 7.5 and above. Users can go to the Grafana official website to download the installation package and execute the installation according to the current operating system. The download address is as follows: <a href="https://grafana.com/grafana/download">https://grafana.com/grafana/download</a>.</p>
<h2 id="install-tdengine-plugin">Install TDengine plugin</h2>
<p>Please copy the following shell commands to export <code>TDENGINE_CLOUD_URL</code> and <code>TDENGINE_CLOUD_TOKEN</code> for the data source installation.</p>
<pre v-highlight="`export TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
export TDENGINE_CLOUD_URL=&quot;${url}&quot;
`"><code class="language-bash"></code></pre>
<p>Run below script from Linux terminal to install TDengine data source plugin.</p>
<pre v-highlight="`bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`"><code class="language-bash"></code></pre>
<p>After that completed, please restart grafana-server.</p>
<pre v-highlight="`sudo systemctl restart grafana-server.service
`"><code class="language-bash"></code></pre>
<h2 id="verify-plugin">Verify plugin</h2>
<p>Users can log in to the Grafana server (initial username/password: admin/admin) directly through the URL <code>http://localhost:3000</code>. Click <code>Configuration -&gt; Data Sources</code> on the left side. Then click <code>Test</code> button to verify if TDengine data source works. You should see a success message if the test worked.</p>
<p><img src="../assets/grafana/verifying-tdengine-datasource.webp" alt="Verify TDengine data source"></p>
<h2 id="use-grafana">Use Grafana</h2>
<p>Please add new dashboard or import exist dashboard to illustrate the data you store in the TDengine.</p>
<p>And refer to the <a href="https://docs.tdengine.com/third-party/grafana#create-dashboard">documentation</a> for more details.</p>
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