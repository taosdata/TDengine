<template>
  <div><p>In this section we will explain how to write into TDengine  service using schemaless OpenTSDB Telnet protocols over REST interface.</p>
<h2 id="config">Config</h2>
<p>Run this command in your terminal to save the TDengine  token and URL as variables:</p>
<el-tabs model-value="bash">
<el-tab-pane name="bash" label="Bash">

<pre
v-highlight="`export TDENGINE_TOKEN=&quot;${token}&quot;
export TDENGINE_URL=&quot;${url}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="cmd" label="CMD">

<pre
v-highlight="`set TDENGINE_TOKEN=&quot;${token}&quot;
set TDENGINE_URL=&quot;${url}&quot;
`"><code class="language-bash"></code></pre>
</el-tab-pane>
<el-tab-pane name="powershell" label="Powershell">

<pre
v-highlight="`$env:TDENGINE_TOKEN=&quot;${token}&quot;
$env:TDENGINE_URL=&quot;${url}&quot;
`"><code class="language-powershell"></code></pre>
</el-tab-pane>
</el-tabs>

<h2 id="insert">Insert</h2>
<p>You can use any client that supports the http protocol to access the RESTful interface address <code>&lt;cloud_url&gt;/opentsdb/v1/put</code> to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:</p>
<pre v-highlight><code class="language-text">/opentsdb/v1/put/telnet/&lt;db&gt;?token=&lt;token&gt;
</code></pre>
<h2 id="insert-example">Insert Example</h2>
<pre
v-highlight="`curl --request POST &quot;$TDENGINE_URL/opentsdb/v1/put/telnet/<db_name>?token=$TDENGINE_TOKEN&quot; --data-binary &quot;sys  1479496100 1.3E0 host=web01 interface=eth0&quot;
`"><code class="language-bash"></code></pre>
<h2 id="query-example-with-sql">Query Example with SQL</h2>
<ul>
<li><code>sys</code> is the super table name.</li>
<li>you can filter data by tag, like:<code>where host=&quot;web01&quot;</code>.</li>
</ul>
<pre
v-highlight="`curl -L -d &quot;select * from <db_name>.sys where host=\&quot;web01\&quot;&quot; $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`"><code class="language-bash"></code></pre>
</div>
  </template>
  
<script setup lang="ts">
import { DocsProps } from '../utils'
  
defineProps<DocsProps>()
</script>
