<template>
  <div><h2 id="add-dependency">Add Dependency</h2>
<el-tabs value="maven">
<el-tab-pane name="maven" label="Maven">

<pre v-highlight><code class="language-xml">    &lt;dependency&gt;
      &lt;groupId&gt;com.taosdata.jdbc&lt;/groupId&gt;
      &lt;artifactId&gt;taos-jdbcdriver&lt;/artifactId&gt;
      &lt;version&gt;3.0.0&lt;/version&gt;
    &lt;/dependency&gt;
</code></pre>
</el-tab-pane>
<el-tab-pane name="gradel" label="Gradle">

<pre v-highlight><code class="language-groovy">dependencies {
  implementation &#39;com.taosdata.jdbc:taos-jdbcdriver:3.0.0.0&#39;
}
</code></pre>
</el-tab-pane>
</el-tabs>

<h2 id="config">Config</h2>
<p>Run this command in your terminal to save the JDBC URL as variable:</p>
<el-tabs value="bash">
<el-tab-pane name="bash" label="Bash">

<pre v-highlight="`export TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="cmd" label="CMD">

<pre v-highlight="`set TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="powershell" label="Powershell">

<pre v-highlight="`$env:TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`"><code class="language-powershell"></code></pre>
</el-tab-pane>
</el-tabs>


<p>Alternatively, you can set environment variable in your IDE&#39;s run configurations.</p>
<h2 id="connect">Connect</h2>
<p>Code bellow get JDBC URL from environment variables first and then create a <code>Connection</code> object, witch is a standard JDBC Connection object.</p>
<pre v-highlight><code class="language-java">import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv(&quot;TDENGINE_JDBC_URL&quot;);
        System.out.println(jdbcUrl);
        try(Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try(Statement stmt = conn.createStatement()) {
                stmt.executeQuery(&quot;select server_version()&quot;);
            }
        }
    }
}
</code></pre>
<p>The client connection is then established. For how to write data and query data, please refer to <a href="https://docs.tdengine.com/develop/insert-data/sql-writing/#insert-using-sql "> https://docs.tdengine.com/develop/insert-data/sql-writing/#insert-using-sql</a> and <a href="https://docs.tdengine.com/develop/query-data/#down-sampling-and-interpolation">https://docs.tdengine.com/develop/query-data/#down-sampling-and-interpolation</a>.</p>
<p>For more details about how to write or query data via REST API, please check <a href="https://docs.tdengine.com/reference/rest-api/">REST API</a>.</p>
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