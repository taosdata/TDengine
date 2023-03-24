<template>
  <div><h2 id="create-project">Create Project</h2>
<pre v-highlight="`dotnet new console -o example
`"><code class="language-bash"></code></pre>
<h2 id="add-c-tdengine-driver-class-lib">Add C# TDengine Driver class lib</h2>
<pre v-highlight="`cd example
dotnet add package TDengine.Connector
`"><code class="language-bash"></code></pre>
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
<pre v-highlight><code class="language-C#">using System;
using TDengineWS.Impl;

namespace Cloud.Examples
{
    public class ConnectExample
    {
        static void Main(string[] args)
        {
            string dsn = Environment.GetEnvironmentVariable(&quot;TDENGINE_CLOUD_DSN&quot;);
            Connect(dsn);
        }

        public static void Connect(string dsn)
        {
            // get connect
            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn == IntPtr.Zero)
            {
                throw new Exception($&quot;get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}&quot;);
            }
            else
            {
                Console.WriteLine(&quot;Establish connect success.&quot;);
            }

            // do something ...

            // close connect
            LibTaosWS.WSClose(conn);

        }
    }
}
</code></pre>
<p>The client connection is then established. For how to write data and query data, please refer to <a href="https://docs.tdengine.com/cloud/programming/insert/"> https://docs.tdengine.com/cloud/programming/insert/</a> and <a href="https://docs.tdengine.com/cloud/programming/query/">https://docs.tdengine.com/cloud/programming/query/</a>.</p>
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
