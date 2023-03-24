<template>
  <div><h2 id="initialize-module">Initialize Module</h2>
<pre v-highlight><code>go mod init tdengine.com/example
</code></pre>
<h2 id="add-dependency">Add Dependency</h2>
<p>add <code>driver-go</code> dependency in <code>go.mod</code> .</p>
<pre v-highlight><code class="language-go-mod">module tdengine.com/example

go 1.17

require github.com/taosdata/driver-go/v3 latest
</code></pre>
<h2 id="config">Config</h2>
<p>Run this command in your terminal to save DSN(data source name) as variable:</p>
<el-tabs value="bash">
<el-tab-pane name="bash" label="Bash">

<pre v-highlight="`export TDENGINE_GO_DSN=&quot;${goDSN}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="cmd" label="CMD">

<pre v-highlight="`set TDENGINE_GO_DSN=&quot;${goDSN}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="powershell" label="Powershell">

<pre v-highlight="`$env:TDENGINE_GO_DSN=&quot;${goDSN}&quot;
`"><code class="language-powershell"></code></pre>
</el-tab-pane>
</el-tabs>



<h2 id="connect">Connect</h2>
<p>Copy code bellow to main.go.</p>
<pre v-highlight><code class="language-go">package main

import (
    &quot;database/sql&quot;
    &quot;fmt&quot;
    &quot;os&quot;

    _ &quot;github.com/taosdata/driver-go/v3/taosRestful&quot;
)

func main() {
    dsn := os.Getenv(&quot;TDENGINE_GO_DSN&quot;)
    taos, err := sql.Open(&quot;taosRestful&quot;, dsn)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer taos.Close()
    rows, err := taos.Query(&quot;show databases&quot;)
    if err != nil {
        fmt.Println(err)
        return
    }
    rows.Close()
    fmt.Println(&quot;connect success&quot;)
}
</code></pre>
<p>Then download dependencies by execute command:</p>
<pre v-highlight><code>go mod tidy
</code></pre>
<p>Finally, test the connection:</p>
<pre v-highlight><code>go run main.go
</code></pre>
<p>The client connection is then established.  For how to write data and query data, please refer to <a href=" https://docs.tdengine.com/cloud/programming/insert/"> https://docs.tdengine.com/cloud/programming/insert/</a> and <a href="https://docs.tdengine.com/cloud/programming/query/">https://docs.tdengine.com/cloud/programming/query/</a>.</p>
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
