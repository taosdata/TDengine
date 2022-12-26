<template>
  <div><p>Using its <a href="https://datastudio.google.com/data?search=TDengine">partner connector</a>, Google Data Studio can quickly access TDengine and create interactive reports and dashboards using its web-based reporting features.The whole process does not require any code development. Share your reports and dashboards with individuals, teams, or the world. Collaborate in real time. Embed your report on any web page.</p>
<p>Refer to <a href="https://github.com/taosdata/gds-connector/blob/master/README.md">GitHub</a> for additional information on utilizing the Data Studio with TDengine.</p>
<h2 id="choose-data-source">Choose Data Source</h2>
<p>The current <a href="https://datastudio.google.com/data?search=TDengine">connector</a> supports two different types of data sources: TDengine Server and TDengine Cloud. Select &quot;TDengine Cloud&quot; and then click &quot;NEXT&quot;.</p>
<p><img src="../assets/gds/gds_data_source.webp" alt="Data Studio Data Source Selection"></p>
<h2 id="connector-configuration">Connector Configuration</h2>
<h3 id="mandatory-config">Mandatory Config</h3>
<h4 id="url">URL</h4>
<p>TDengine Cloud URL.</p>
<pre v-highlight="`${cloud_url}
`"><code class="language-bash"></code></pre>
<h4 id="tdengine-cloud-token">TDengine Cloud Token</h4>
<pre v-highlight="`${cloud_token}
`"><code class="language-bash"></code></pre>
<h4 id="database">database</h4>
<p>The database name that contains the table(no matter if it is a normal table, a super table or a child table) is the one you want to query for data and make reports on.</p>
<h4 id="table">table</h4>
<p>The name of the table that you wish to connect to in order to query its data and run a report.</p>
<p><strong>Notice</strong> The maximum amount of records that may currently be retrieved is 1000000 rows.</p>
<h3 id="optional-config">Optional config</h3>
<h4 id="query-range-start-date--end-date">Query range start date &amp; end date</h4>
<p>The page where we configure our connector has two text boxes.These two date filter conditions are used to limit the amount of data that will be retrieved, and the date should be entered in the format &quot;YYYY-MM-DD HH:MM:SS.&quot;
e.g.</p>
<pre v-highlight="`2022-05-12 18:24:15
`"><code class="language-bash"></code></pre>
<p>The query result&#39;s start timestamp is defined by the <code>start date</code>. To put it another way, records from before this <code>start date</code> won&#39;t be received.</p>
<p>The <code>end time</code> indicates the query result&#39;s end timestamp. Therefore, records that were written after this end date cannot be retrieved.
These conditions are utilized in the where clause in SQL statements, such as:</p>
<pre v-highlight><code class="language-SQL">-- select * from table_name where ts &gt;= start_date and ts &lt;= end_date
select * from test.demo where ts &gt;= &#39;2022-05-10 18:24:15&#39; and ts&lt;=&#39;2022-05-12 18:24:15&#39;
</code></pre>
<p>In fact, you can speed up the data loading in your report by using these filters.</p>
<p><img src="../assets/gds/gds_cloud_login.webp" alt="TDengine Cloud Config Page"></p>
<p>Click &quot;CONNECT&quot; once configuration is complete, then you can connect to your &quot;TDengine Cloud&quot; with the given database and table.</p>
<h2 id="create-report-or-dashboard">Create Report or Dashboard</h2>
<p>Unlock the power of your data with interactive dashboards and beautiful reports with the data stored in TDengine.</p>
<p>And refer to <a href="https://docs.tdengine.com/third-party/google-data-studio/">documentation</a> for more details.</p>
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