<template>
  <div><h2 id="install-connector">Install Connector</h2>
<p>First, you need to install the <code>taospy</code> module version &gt;= <code>2.6.2</code>. Run the command below in your terminal.</p>
<el-tabs value="pip" groupID="package">
<el-tab-pane name="pip" label="pip">

<pre v-highlight><code>pip3 install -U taospy
</code></pre>
<p>You&#39;ll need to have Python3 installed.</p>
</el-tab-pane>
<el-tab-pane name="conda" label="conda">

<pre v-highlight><code>conda install -c conda-forge taospy
</code></pre>
</el-tab-pane>
</el-tabs>

<h2 id="config">Config</h2>
<p>Run this command in your terminal to save TDengine cloud token and URL as variables:</p>
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


<p>Alternatively, you can also set environment variables in your IDE&#39;s run configurations.</p>
<h2 id="connect">Connect</h2>
<p>Copy code bellow to your editor and run it. If you are using jupyter, assuming you have followed the guide about Jupyter in previous secions, you can copy the code into Jupyter editor in your browser.</p>
<pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_CLOUD_URL&quot;]
token = os.environ[&quot;TDENGINE_CLOUD_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
</code></pre>
<p>For how to write data and query data, please refer to <a href=" https://docs.tdengine.com/develop/insert-data/sql-writing/#insert-using-sql"> https://docs.tdengine.com/develop/insert-data/sql-writing/#insert-using-sql</a> and <a href="https://docs.tdengine.com/develop/query-data/#down-sampling-and-interpolation">https://docs.tdengine.com/develop/query-data/#down-sampling-and-interpolation</a>.</p>
<p>For more details about how to write or query data via REST API, please check <a href="https://docs.tdengine.com/reference/rest-api/">REST API</a>.</p>
<h2 id="jupyter">Jupyter</h2>
<p><strong>Step 1: Install</strong></p>
<p>For the users who are familiar with Jupyter to program in Python, both TDengine Python connector and Jupyter need to be ready in your environment. If you have not done yet, please use the commands below to install them.</p>
<el-tabs value="pip" groupID="package">
<el-tab-pane name="pip" label="pip">

<pre v-highlight="`pip install jupyterlab
pip3 install -U taospy
`"><code class="language-bash"></code></pre>
<p>You&#39;ll need to have Python3 installed.</p>
</el-tab-pane>
<el-tab-pane name="conda" label="conda">

<pre v-highlight><code>conda install -c conda-forge jupyterlab
conda install -c conda-forge taospy
</code></pre>
</el-tab-pane>
</el-tabs>

<p><strong>Step 2: Configure</strong></p>
<p>In order for Jupyter to connect to TDengine cloud service, before launching Jupypter, the environment setting must be performed. We use Linux bash as example.</p>
<pre v-highlight="`export TDENGINE_CLOUD_TOKEN=&quot;${token}&quot;
export TDENGINE_CLOUD_URL=&quot;${url}&quot;
jupyter lab
`"><code class="language-bash"></code></pre>
<p><strong>Step 3: Connect</strong></p>
<p>Once jupyter lab is launched, Jupyter lab service is automatically connected and shown in your browser. You can create a new notebook and copy the sample code below and run it.</p>
<pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_CLOUD_URL&quot;]
token = os.environ[&quot;TDENGINE_CLOUD_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
</code></pre>
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
