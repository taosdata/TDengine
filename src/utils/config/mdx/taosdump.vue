<template>
  <div>
    <h2 id="overview">{{ $t("docs.dataout.dump.step1") }}</h2>
    <p>{{ $t("docs.dataout.dump.step1desc") }}</p>
    <p>{{ $t("docs.dataout.dump.step1desc1") }}</p>
    <p>{{ $t("docs.dataout.dump.step1desc2") }}</p>
    <p>{{ $t("docs.dataout.dump.step1desc3") }}</p>
    <p>
      {{ $t("docs.dataout.dump.step1desc4")
      }}<a href="https://avro.apache.org/">Apache AVRO</a
      >{{ $t("docs.dataout.dump.step1desc5") }}
    </p>
    <h2 id="installation">{{ $t("docs.dataout.dump.step2") }}</h2>
    <p>
      {{ $t("docs.dataout.dump.step2desc")
      }}&nbsp;&nbsp;<a
        :href="`https://${urlPart}.com/assets-download/3.0/taosTools-2.4.9-Linux-x64-comp3.tar.gz`"
        >taosTools</a
      >{{ $t("docs.dataout.dump.step2desc1")
      }}&nbsp;&nbsp;<a :href="`https://docs.${urlPart}.com/reference/taos-shell`"
        >TDengine CLI</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>{{ $t("docs.dataout.dump.step2desc2") }}</p>
    <pre v-highlight><code>tar -xzf taosTools-2.4.9-Linux-x64-comp3.tar.gz
cd taosTools-2.4.9-Linux-x64-comp3
sudo ./install-taostools.sh
</code></pre>
    <p>{{ $t("docs.dataout.dump.step2desc3") }}</p>
    <pre
      v-highlight="
        `export TDENGINE_DSN=&quot;${DSN}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <h2 id="common-usage-scenarios">{{ $t("docs.dataout.dump.step3") }}</h2>
    <h3 id="taosdump-backup-data">{{ $t("docs.dataout.dump.step31") }}</h3>
    <ol>
      <li>{{ $t("docs.dataout.dump.step31desc") }}</li>
      <li>{{ $t("docs.dataout.dump.step31desc1") }}</li>
      <li>{{ $t("docs.dataout.dump.step31desc2") }}</li>
      <li>{{ $t("docs.dataout.dump.step31desc3") }}</li>
      <li>
        {{ $t("docs.dataout.dump.step31desc4")
        }}&nbsp;&nbsp;<a :href="`https://docs.${urlPart}.com/taos-sql/escape/`">{{
          $t("docs.dataout.dump.step31desc5")
        }}</a>
        {{ $t("docs.dataout.dump.step31desc6") }}
      </li>
    </ol>
    <h3 id="taosdump-recover-data">{{ $t("docs.dataout.dump.step32") }}</h3>
    <p>{{ $t("docs.dataout.dump.step32desc") }}</p>
    <h2 id="detailed-command-line-parameter-list">
      {{ $t("docs.dataout.dump.step4") }}
    </h2>
    <p>{{ $t("docs.dataout.dump.step4desc") }}</p>
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
  -C, --cloud=CLOUD_DSN      specify a DSN to access TDengine  service
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
import { IsAliyun } from "@/const";
export default {
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
    user: {
      type: String,
      default: ''
    },
    password: {
      type: String,
      default: ''
    }
  },
  data() {
    return {};
  },
  computed: {
    DSN() {
      return `taos://${this.user}:${this.password}@${this.url.replace(/https?:\/\//, "")}`;
    },
    urlPart() {
      return navigator.language.includes('en') ?"tdengine": "taosdata";
    },
  },
};
</script>
