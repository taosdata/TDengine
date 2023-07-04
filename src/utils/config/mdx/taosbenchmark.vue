<template>
  <div>
    <h2 id="introduction">{{ $t("docs.tool.benchmark.step1") }}</h2>
    <p>{{ $t("docs.tool.benchmark.step1desc") }}</p>
    <p>
      <strong>{{ $t("docs.tool.benchmark.step1desc1") }}</strong>
    </p>
    <h2 id="installation">{{ $t("docs.tool.benchmark.step2") }}</h2>
    <p>
      {{ $t("docs.tool.benchmark.step2desc") }}
      <a
        :href="`https://${urlPart}.com/assets-download/3.0/taosTools-2.4.9-Linux-x64-comp3.tar.gz`"
        >taosTools</a
      >{{ $t("docs.tool.benchmark.step2desc1") }}
      <a :href="`https://docs.${urlPart}.com/operation/pkg-install/`">{{
        $t("docs.tool.benchmark.step2desc2")
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>{{ $t("docs.tool.benchmark.step2desc3") }}</p>
    <pre v-highlight><code>tar -xzf taosTools-2.4.9-Linux-x64-comp3.tar.gz
cd taosTools-2.4.9-Linux-x64-comp3
sudo ./install-taostools.sh
</code></pre>
    <h2 id="run">{{ $t("docs.tool.benchmark.step3") }}</h2>
    <h3 id="configuration-and-running-methods">
      {{ $t("docs.tool.benchmark.step31") }}
    </h3>
    <p>{{ $t("docs.tool.benchmark.step31desc") }}</p>
    <pre
      v-highlight="
        `export TDENGINE_DSN=&quot;${DSN}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.tool.benchmark.step31desc1") }}</p>
    <p>{{ $t("docs.tool.benchmark.step31desc2") }}</p>
    <p>
      <strong>{{ $t("docs.tool.benchmark.step31desc3") }}</strong>
    </p>
    <h3 id="run-with-the-configuration-file">
      {{ $t("docs.tool.benchmark.step32") }}
    </h3>
    <p>{{ $t("docs.tool.benchmark.step32desc") }}</p>
    <p>{{ $t("docs.tool.benchmark.step32desc1") }}</p>
    <pre
      v-highlight="
        `taosBenchmark -f json-file
`
      "
    ><code class="language-bash"></code></pre>
    <h3 id="sample-configuration-files">
      {{ $t("docs.tool.benchmark.step33") }}
    </h3>
    <h4 id="configuration-file-examples">
      {{ $t("docs.tool.benchmark.step34") }}
    </h4>
    <pre v-highlight><code class="language-json">{
    &quot;filetype&quot;: &quot;insert&quot;,
    &quot;cfgdir&quot;: &quot;/etc/taos&quot;,
    &quot;host&quot;: &quot;127.0.0.1&quot;,
    &quot;port&quot;: 6030,
    &quot;user&quot;: &quot;root&quot;,
    &quot;password&quot;: &quot;taosdata&quot;,
    &quot;connection_pool_size&quot;: 8,
    &quot;thread_count&quot;: 4,
    &quot;create_table_thread_count&quot;: 7,
    &quot;result_file&quot;: &quot;./insert_res.txt&quot;,
    &quot;confirm_parameter_prompt&quot;: &quot;no&quot;,
    &quot;insert_interval&quot;: 0,
    &quot;interlace_rows&quot;: 100,
    &quot;num_of_records_per_req&quot;: 100,
    &quot;prepared_rand&quot;: 10000,
    &quot;chinese&quot;: &quot;no&quot;,
    &quot;databases&quot;: [
        {
            &quot;dbinfo&quot;: {
                &quot;name&quot;: &quot;test&quot;,
                &quot;drop&quot;: &quot;no&quot;,
                &quot;replica&quot;: 1,
                &quot;precision&quot;: &quot;ms&quot;,
                &quot;keep&quot;: 3650,
                &quot;minRows&quot;: 100,
                &quot;maxRows&quot;: 4096,
                &quot;comp&quot;: 2
            },
            &quot;super_tables&quot;: [
                {
                    &quot;name&quot;: &quot;meters&quot;,
                    &quot;child_table_exists&quot;: &quot;no&quot;,
                    &quot;childtable_count&quot;: 10000,
                    &quot;childtable_prefix&quot;: &quot;d&quot;,
                    &quot;escape_character&quot;: &quot;yes&quot;,
                    &quot;auto_create_table&quot;: &quot;no&quot;,
                    &quot;batch_create_tbl_num&quot;: 5,
                    &quot;data_source&quot;: &quot;rand&quot;,
                    &quot;insert_mode&quot;: &quot;taosc&quot;,
                    &quot;non_stop_mode&quot;: &quot;no&quot;,
                    &quot;line_protocol&quot;: &quot;line&quot;,
                    &quot;insert_rows&quot;: 10000,
                    &quot;childtable_limit&quot;: 10,
                    &quot;childtable_offset&quot;: 100,
                    &quot;interlace_rows&quot;: 0,
                    &quot;insert_interval&quot;: 0,
                    &quot;partial_col_num&quot;: 0,
                    &quot;disorder_ratio&quot;: 0,
                    &quot;disorder_range&quot;: 1000,
                    &quot;timestamp_step&quot;: 10,
                    &quot;start_timestamp&quot;: &quot;2020-10-01 00:00:00.000&quot;,
                    &quot;sample_format&quot;: &quot;csv&quot;,
                    &quot;sample_file&quot;: &quot;./sample.csv&quot;,
                    &quot;use_sample_ts&quot;: &quot;no&quot;,
                    &quot;tags_file&quot;: &quot;&quot;,
                    &quot;columns&quot;: [
                        {
                            &quot;type&quot;: &quot;FLOAT&quot;,
                            &quot;name&quot;: &quot;current&quot;,
                            &quot;count&quot;: 1,
                            &quot;max&quot;: 12,
                            &quot;min&quot;: 8
                        },
                        { &quot;type&quot;: &quot;INT&quot;, &quot;name&quot;: &quot;voltage&quot;, &quot;max&quot;: 225, &quot;min&quot;: 215 },
                        { &quot;type&quot;: &quot;FLOAT&quot;, &quot;name&quot;: &quot;phase&quot;, &quot;max&quot;: 1, &quot;min&quot;: 0 }
                    ],
                    &quot;tags&quot;: [
                        {
                            &quot;type&quot;: &quot;TINYINT&quot;,
                            &quot;name&quot;: &quot;groupid&quot;,
                            &quot;max&quot;: 10,
                            &quot;min&quot;: 1
                        },
                        {
                            &quot;name&quot;: &quot;location&quot;,
                            &quot;type&quot;: &quot;BINARY&quot;,
                            &quot;len&quot;: 16,
                            &quot;values&quot;: [&quot;San Francisco&quot;, &quot;Los Angles&quot;, &quot;San Diego&quot;,
                                &quot;San Jose&quot;, &quot;Palo Alto&quot;, &quot;Campbell&quot;, &quot;Mountain View&quot;,
                                &quot;Sunnyvale&quot;, &quot;Santa Clara&quot;, &quot;Cupertino&quot;]
                        }
                    ]
                }
            ]
        }
    ]
}
</code></pre>
    <h4 id="query-scenario-json-profile-example">
      {{ $t("docs.tool.benchmark.step35") }}
    </h4>
    <pre v-highlight><code class="language-json">{
    &quot;filetype&quot;: &quot;query&quot;,
    &quot;cfgdir&quot;: &quot;/etc/taos&quot;,
    &quot;host&quot;: &quot;127.0.0.1&quot;,
    &quot;port&quot;: 6030,
    &quot;user&quot;: &quot;root&quot;,
    &quot;password&quot;: &quot;taosdata&quot;,
    &quot;confirm_parameter_prompt&quot;: &quot;no&quot;,
    &quot;databases&quot;: &quot;test&quot;,
    &quot;query_times&quot;: 2,
    &quot;query_mode&quot;: &quot;taosc&quot;,
    &quot;specified_table_query&quot;: {
        &quot;query_interval&quot;: 1,
        &quot;concurrent&quot;: 3,
        &quot;sqls&quot;: [
            {
                &quot;sql&quot;: &quot;select last_row(*) from meters&quot;,
                &quot;result&quot;: &quot;./query_res0.txt&quot;
            },
            {
                &quot;sql&quot;: &quot;select count(*) from d0&quot;,
                &quot;result&quot;: &quot;./query_res1.txt&quot;
            }
        ]
    },
    &quot;super_table_query&quot;: {
        &quot;stblname&quot;: &quot;meters&quot;,
        &quot;query_interval&quot;: 1,
        &quot;threads&quot;: 3,
        &quot;sqls&quot;: [
            {
                &quot;sql&quot;: &quot;select last_row(ts) from xxxx&quot;,
                &quot;result&quot;: &quot;./query_res2.txt&quot;
            }
        ]
    }
}
</code></pre>
    <h2 id="configuration-file-parameters-in-detailed">
      {{ $t("docs.tool.benchmark.step4") }}
    </h2>
    <h3 id="general-configuration-parameters">
      {{ $t("docs.tool.benchmark.step41") }}
    </h3>
    <p>{{ $t("docs.tool.benchmark.step41desc") }}</p>
    <ul>
      <li>
        <p>
          <strong>filetype</strong> {{ $t("docs.tool.benchmark.step41desc1") }}
          <strong>cfgdir</strong>{{ $t("docs.tool.benchmark.step41desc2") }}
        </p>
      </li>

      <li>
        <p><strong>host</strong>{{ $t("docs.tool.benchmark.step41desc3") }}</p>
      </li>
      <li>
        <p><strong>port</strong>{{ $t("docs.tool.benchmark.step41desc4") }}</p>
      </li>
      <li>
        <p><strong>user</strong>{{ $t("docs.tool.benchmark.step41desc5") }}</p>
      </li>
      <li>
        <p>
          <strong>password</strong>{{ $t("docs.tool.benchmark.step41desc6") }}
        </p>
      </li>
    </ul>
    <h3 id="insert-scenario-configuration-parameters">
      {{ $t("docs.tool.benchmark.step42") }}
    </h3>
    <p>{{ $t("docs.tool.benchmark.step42desc") }}</p>
    <h4 id="stream-processing-related-configuration-parameters">
      {{ $t("docs.tool.benchmark.step43") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step43desc") }}</p>
    <ul>
      <li>
        <p>
          <strong>stream_name</strong
          >{{ $t("docs.tool.benchmark.step43desc1") }}
        </p>
      </li>
      <li>
        <p>
          <strong>stream_stb</strong>{{ $t("docs.tool.benchmark.step43desc2") }}
        </p>
      </li>
      <li>
        <p>
          <strong>stream_sql</strong>{{ $t("docs.tool.benchmark.step43desc3") }}
        </p>
      </li>
      <li>
        <p>
          <strong>trigger_mode</strong
          >{{ $t("docs.tool.benchmark.step43desc4") }}
        </p>
      </li>
      <li>
        <p>
          <strong>watermark</strong>{{ $t("docs.tool.benchmark.step43desc5") }}
        </p>
      </li>
      <li>
        <p><strong>drop</strong>{{ $t("docs.tool.benchmark.step43desc6") }}</p>
      </li>
    </ul>
    <h4 id="super-table-related-configuration-parameters">
      {{ $t("docs.tool.benchmark.step44") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step44desc") }}</p>
    <ul>
      <li>
        <p><strong>name</strong>{{ $t("docs.tool.benchmark.step44desc1") }}</p>
      </li>
      <li>
        <p>
          <strong>child_table_exists</strong
          >{{ $t("docs.tool.benchmark.step44desc2") }}
        </p>
      </li>
      <li>
        <p>
          <strong>child_table_count</strong
          >{{ $t("docs.tool.benchmark.step44desc3") }}
        </p>
      </li>
      <li>
        <p>
          <strong>child_table_prefix</strong
          >{{ $t("docs.tool.benchmark.step44desc4") }}
        </p>
      </li>
      <li>
        <p>
          <strong>escape_character</strong
          >{{ $t("docs.tool.benchmark.step44desc5") }}
        </p>
      </li>
      <li>
        <p>
          <strong>auto_create_table</strong
          >{{ $t("docs.tool.benchmark.step44desc6") }}
        </p>
      </li>
      <li>
        <p>
          <strong>batch_create_tbl_num</strong
          >{{ $t("docs.tool.benchmark.step44desc7") }}
        </p>
      </li>
      <li>
        <p>
          <strong>data_source</strong
          >{{ $t("docs.tool.benchmark.step44desc8") }}
        </p>
      </li>
      <li>
        <p>
          <strong>insert_mode</strong
          >{{ $t("docs.tool.benchmark.step44desc9") }}
        </p>
      </li>
      <li>
        <p>
          <strong>non_stop_mode</strong
          >{{ $t("docs.tool.benchmark.step44desc10") }}
        </p>
      </li>
      <li>
        <p>
          <strong>line_protocol</strong
          >{{ $t("docs.tool.benchmark.step44desc11") }}
        </p>
      </li>
      <li>
        <p>
          <strong>tcp_transfer</strong
          >{{ $t("docs.tool.benchmark.step44desc12") }}
        </p>
      </li>
      <li>
        <p>
          <strong>insert_rows</strong
          >{{ $t("docs.tool.benchmark.step44desc13") }}
        </p>
      </li>
      <li>
        <p>
          <strong>childtable_offset</strong
          >{{ $t("docs.tool.benchmark.step44desc14") }}
        </p>
      </li>
      <li>
        <p>
          <strong>childtable_limit</strong
          >{{ $t("docs.tool.benchmark.step44desc15") }}
        </p>
      </li>
      <li>
        <p>
          <strong>interlace_rows</strong
          >{{ $t("docs.tool.benchmark.step44desc16") }}
        </p>
      </li>
      <li>
        <p>
          <strong>insert_interval</strong
          >{{ $t("docs.tool.benchmark.step44desc17") }}
        </p>
      </li>
      <li>
        <p>
          <strong>partial_col_num</strong
          >{{ $t("docs.tool.benchmark.step44desc18") }}
        </p>
      </li>
      <li>
        <p>
          <strong>disorder_ratio</strong
          >{{ $t("docs.tool.benchmark.step44desc19") }}
        </p>
      </li>
      <li>
        <p>
          <strong>disorder_range</strong
          >{{ $t("docs.tool.benchmark.step44desc20") }}
        </p>
      </li>
      <li>
        <p>
          <strong>timestamp_step</strong
          >{{ $t("docs.tool.benchmark.step44desc21") }}
        </p>
      </li>
      <li>
        <p>
          <strong>start_timestamp</strong
          >{{ $t("docs.tool.benchmark.step44desc22") }}
        </p>
      </li>
      <li>
        <p>
          <strong>sample_format</strong
          >{{ $t("docs.tool.benchmark.step44desc23") }}
        </p>
      </li>
      <li>
        <p>
          <strong>sample_file</strong
          >{{ $t("docs.tool.benchmark.step44desc24") }}
        </p>
      </li>
      <li>
        <p>
          <strong>use_sample_ts</strong
          >{{ $t("docs.tool.benchmark.step44desc25") }}
        </p>
      </li>
      <li>
        <p>
          <strong>tags_file</strong>{{ $t("docs.tool.benchmark.step44desc26") }}
        </p>
      </li>
    </ul>
    <h4 id="tsma-configuration-parameters">
      {{ $t("docs.tool.benchmark.step45") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step45desc") }}</p>
    <ul>
      <li>
        <p><strong>name</strong>{{ $t("docs.tool.benchmark.step45desc1") }}</p>
      </li>
      <li>
        <p>
          <strong>function</strong>{{ $t("docs.tool.benchmark.step45desc2") }}
        </p>
      </li>
      <li>
        <p>
          <strong>interval</strong>{{ $t("docs.tool.benchmark.step45desc3") }}
        </p>
      </li>
      <li>
        <p>
          <strong>sliding</strong>{{ $t("docs.tool.benchmark.step45desc4") }}
        </p>
      </li>
      <li>
        <p>
          <strong>custom</strong>{{ $t("docs.tool.benchmark.step45desc5") }}
        </p>
      </li>
      <li>
        <p>
          <strong>start_when_inserted</strong
          >{{ $t("docs.tool.benchmark.step45desc6") }}
        </p>
      </li>
    </ul>
    <h4 id="tag-and-data-column-configuration-parameters">
      {{ $t("docs.tool.benchmark.step46") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step46desc") }}</p>
    <ul>
      <li>
        <p><strong>type</strong>{{ $t("docs.tool.benchmark.step46desc1") }}</p>
      </li>
      <li>
        <p><strong>len</strong>{{ $t("docs.tool.benchmark.step46desc2") }}</p>
      </li>
      <li>
        <p><strong>count</strong>{{ $t("docs.tool.benchmark.step46desc3") }}</p>
      </li>
      <li>
        <p><strong>name</strong>{{ $t("docs.tool.benchmark.step46desc4") }}</p>
      </li>
      <li>
        <p><strong>min</strong>{{ $t("docs.tool.benchmark.step46desc5") }}</p>
      </li>
      <li>
        <p><strong>max</strong>{{ $t("docs.tool.benchmark.step46desc6") }}</p>
      </li>
      <li>
        <p>
          <strong>values</strong>{{ $t("docs.tool.benchmark.step46desc7") }}
        </p>
      </li>
      <li>
        <p><strong>sma</strong>{{ $t("docs.tool.benchmark.step46desc8") }}</p>
      </li>
    </ul>
    <h4 id="insertion-behavior-configuration-parameters">
      {{ $t("docs.tool.benchmark.step47") }}
    </h4>
    <ul>
      <li>
        <p>
          <strong>thread_count</strong
          >{{ $t("docs.tool.benchmark.step47desc") }}
        </p>
      </li>
      <li>
        <p>
          <strong>create_table_thread_count</strong
          >{{ $t("docs.tool.benchmark.step47desc1") }}
        </p>
      </li>
      <li>
        <p>
          <strong>connection_pool_size</strong
          >{{ $t("docs.tool.benchmark.step47desc2") }}
        </p>
      </li>
      <li>
        <p>
          <strong>result_file</strong
          >{{ $t("docs.tool.benchmark.step47desc3") }}
        </p>
      </li>
      <li>
        <p>
          <strong>confirm_parameter_prompt</strong
          >{{ $t("docs.tool.benchmark.step47desc4") }}
        </p>
      </li>
      <li>
        <p>
          <strong>interlace_rows</strong
          >{{ $t("docs.tool.benchmark.step47desc5") }}
        </p>
      </li>
      <li>
        <p>
          <strong>insert_interval</strong
          >{{ $t("docs.tool.benchmark.step47desc6") }}
        </p>
      </li>
      <li>
        <p>
          <strong>num_of_records_per_req</strong
          >{{ $t("docs.tool.benchmark.step47desc7") }}
        </p>
      </li>
      <li>
        <p>
          <strong>prepare_rand</strong
          >{{ $t("docs.tool.benchmark.step47desc8") }}
        </p>
      </li>
    </ul>
    <h3 id="query-scenario-configuration-parameters">
      {{ $t("docs.tool.benchmark.step48") }}
    </h3>
    <p>{{ $t("docs.tool.benchmark.step48desc") }}</p>
    <h4
      id="configuration-parameters-for-executing-the-specified-query-statement"
    >
      {{ $t("docs.tool.benchmark.step49") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step49desc") }}</p>
    <ul>
      <li>
        <p>
          <strong>query_interval</strong
          >{{ $t("docs.tool.benchmark.step49desc1") }}
        </p>
      </li>
      <li>
        <p>
          <strong>threads</strong>{{ $t("docs.tool.benchmark.step49desc2") }}
        </p>
      </li>
      <li>
        <p><strong>sql</strong>{{ $t("docs.tool.benchmark.step49desc3") }}</p>
      </li>
      <li>
        <p>
          <strong>result</strong>{{ $t("docs.tool.benchmark.step49desc4") }}
        </p>
      </li>
    </ul>
    <h4 id="configuration-parameters-of-query-super-table">
      {{ $t("docs.tool.benchmark.step410") }}
    </h4>
    <p>{{ $t("docs.tool.benchmark.step410desc") }}</p>
    <ul>
      <li>
        <p>
          <strong>stblname</strong>{{ $t("docs.tool.benchmark.step410desc1") }}
        </p>
      </li>
      <li>
        <p>
          <strong>query_interval</strong
          >{{ $t("docs.tool.benchmark.step410desc2") }}
        </p>
      </li>
      <li>
        <p>
          <strong>threads</strong>{{ $t("docs.tool.benchmark.step410desc3") }}
        </p>
      </li>
      <li>
        <p><strong>sql</strong>{{ $t("docs.tool.benchmark.step410desc4") }}</p>
      </li>
      <li>
        <p>
          <strong>result</strong>{{ $t("docs.tool.benchmark.step410desc5") }}
        </p>
      </li>
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
    }
  },
};
</script>
