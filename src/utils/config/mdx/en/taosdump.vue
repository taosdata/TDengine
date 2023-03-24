<template>
  <div><h2 id="introduction">Introduction</h2>
<p>taosdump is a tool that supports backing up data from a running TDengine cluster and restoring the backed up data to the same, or another running TDengine cluster.</p>
<p>taosdump can back up a database, a super table, or a normal table as a logical data unit or backup data records in the database, super tables, and normal tables. When using taosdump, you can specify the directory path for data backup. If you do not specify a directory, taosdump will back up the data to the current directory by default.</p>
<p>If the specified location already has data files, taosdump will prompt the user and exit immediately to avoid data overwriting. This means that the same path can only be used for one backup.</p>
<p>Please be careful if you see a prompt for this and please ensure that you follow best practices and relevant SOPs for data integrity, backup and data security.</p>
<p>Users should not use taosdump to back up raw data, environment settings, hardware information, server configuration, or cluster topology. taosdump uses <a href="https://avro.apache.org/">Apache AVRO</a> as the data file format to store backup data.</p>
<h2 id="installation">Installation</h2>
<p>To use taosdump, you need to download and install <a href="https://docs.tdengine.com/get-started/package/#command-line-interface-(cli)">TDengine CLI</a>.</p>
<p>Decompress the package and install.</p>
<pre v-highlight><code>tar -xzf taosTools-2.1.3-Linux-x64.tar.gz
cd taosTools-2.1.3-Linux-x64.tar.gz
sudo ./install-taostools.sh
</code></pre>
<p>Set environment variable.</p>
<pre v-highlight="`export TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`"><code class="language-bash"></code></pre>
<h2 id="common-usage-scenarios">Common usage scenarios</h2>
<h3 id="taosdump-backup-data">taosdump backup data</h3>
<ol>
<li>backing up all databases: specify <code>-A</code> or <code>-all-databases</code> parameter.</li>
<li>backup multiple specified databases: use <code>-D db1,db2,... </code> parameters;</li>
<li>back up some super or normal tables in the specified database: use <code>dbname stbname1 stbname2 tbname1 tbname2 ... </code> parameters. Note that the first parameter of this input sequence is the database name, and only one database is supported. The second and subsequent parameters are the names of super or normal tables in that database, separated by spaces.</li>
<li>back up the system log database: TDengine clusters usually contain a system database named <code>log</code>. The data in this database is the data that TDengine runs itself, and the taosdump will not back up the log database by default. If users need to back up the log database, users can use the <code>-a</code> or <code>-allow-sys</code> command-line parameter. </li>
<li>Loose mode backup: taosdump version 1.4.1 onwards provides <code>-n</code> and <code>-L</code> parameters for backing up data without using escape characters and &quot;loose&quot; mode, which can reduce the number of backups if table names, column names, tag names do not use escape characters. This can also reduce the backup data time and backup data footprint. If you are unsure about using <code>-n</code> and <code>-L</code> conditions, please use the default parameters for &quot;strict&quot; mode backup. See the <a href="https://docs.tdengine.com/taos-sql/escape/">official documentation</a> for a description of escaped characters.</li>
</ol>
<h3 id="taosdump-recover-data">taosdump recover data</h3>
<p>Restore the data file in the specified path: use the <code>-i</code> parameter plus the path to the data file. You should not use the same directory to backup different data sets, and you should not backup the same data set multiple times in the same path. Otherwise, the backup data will cause overwriting or multiple backups.</p>
<h2 id="detailed-command-line-parameter-list">Detailed command-line parameter list</h2>
<p>The following is a detailed list of taosdump command-line arguments.</p>
<pre v-highlight><code>Usage: taosdump [OPTION...] dbname [tbname ...]
  or:  taosdump [OPTION...] --databases db1,db2,...
  or:  taosdump [OPTION...] --all-databases
  or:  taosdump [OPTION...] -i inpath
  or:  taosdump [OPTION...] -o outpath

  -h, --host=HOST            Server host from which to dump data. Default is
                             localhost.
  -p, --password             User password to connect to server. Default is
                             taosdata.
  -P, --port=PORT            Port to connect
  -u, --user=USER            User name used to connect to server. Default is
                             root.
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is /etc/taos
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump listed databases. Use comma to separate
                             database names.
  -N, --without-property     Dump database without its properties.
  -s, --schemaonly           Only dump table schemas.
  -y, --answer-yes           Input yes for prompt. It will skip data file
                             checking!
  -d, --avro-codec=snappy    Choose an avro codec among null, deflate, snappy,
                             and lzma.
  -S, --start-time=START_TIME   Start time to dump. Either epoch or
                             ISO8601/RFC3339 format is acceptable. ISO8601
                             format example: 2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00:000+0800 or &#39;2017-10-01
                             00:00:00.000+0800&#39;
  -E, --end-time=END_TIME    End time to dump. Either epoch or ISO8601/RFC3339
                             format is acceptable. ISO8601 format example:
                             2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00.000+0800 or &#39;2017-10-01
                             00:00:00.000+0800&#39;
  -B, --data-batch=DATA_BATCH   Number of data per query/insert statement when
                             backup/restore. Default value is 16384. If you see
                             &#39;error actual dump .. batch ..&#39; when backup or if
                             you see &#39;WAL size exceeds limit&#39; error when
                             restore, please adjust the value to a smaller one
                             and try. The workable value is related to the
                             length of the row and type of table schema.
  -I, --inspect              inspect avro file content and print on screen
  -L, --loose-mode           Use loose mode if the table name and column name
                             use letter and number only. Default is NOT.
  -n, --no-escape            No escape char &#39;`&#39;. Default is using it.
  -T, --thread-num=THREAD_NUM   Number of thread for dump in file. Default is
                             8.
  -C, --cloud=CLOUD_DSN      specify a DSN to access TDengine cloud service
  -R, --restful              Use RESTful interface to connect TDengine
  -t, --timeout=SECONDS      The timeout seconds for websocket to interact.
  -g, --debug                Print debug info.
  -?, --help                 Give this help list
      --usage                Give a short usage message
  -V, --version              Print program version

Mandatory or optional arguments to long options are also mandatory or optional
for any corresponding short options.

Report bugs to &lt;support@taosdata.com&gt;.
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
