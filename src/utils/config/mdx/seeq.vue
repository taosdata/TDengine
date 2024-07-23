<template>
  <div>
    <p> {{ $t('docs.tools.seeq.topdesc') }} <a href="https://www.seeq.com/">Seeq</a>{{ $t('docs.tools.seeq.topdesc1') }} </p>
    <h2 id="seeq-repare">{{ $t('docs.tools.seeq.step1') }}</h2>
    <p>{{ $t('docs.tools.seeq.step1desc') }}<a href="https://www.seeq.com/">www.seeq.com</a>{{ $t('docs.tools.seeq.step1desc1') }} </p>
    <h2 id="seeq-install">{{ $t('docs.tools.seeq.step2') }}</h2>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step2desc') }}</span>
        <pre
          v-highlight="
            `sudo seeq config get Folders/Data
`
          "
        ><code class="language-bash"></code></pre>
      </li>
      <li>
        <span class="seeq-span"
          >{{ $t('docs.tools.seeq.step2desc11') }}<a href="https://maven.org/">maven.org</a>{{ $t('docs.tools.seeq.step2desc12')
          }}<a href="https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.7/taos-jdbcdriver-3.2.7-dist.jar">3.2.7</a>{{ $t('docs.tools.seeq.step2desc13') }}</span
        >
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step2desc2') }}</span>
        <pre
          v-highlight="
            `sudo seeq restart
`
          "
        ><code class="language-bash"></code></pre>
      </li>
    </ol>
    <h2 id="seeq-add-ds">{{ $t('docs.tools.seeq.step3full') }}</h2>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step3desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step3desc1') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step3desc2') }}</span>
        <p>
          <i class="el-icon-s-opportunity" style="color: gold;font-size: 20px"></i>
          <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
        </p>
        <pre
          v-highlight="
            `{
    &quot;QueryDefinitions&quot;: [],
    &quot;Type&quot;: &quot;GENERIC&quot;,
    &quot;Hostname&quot;: null,
    &quot;Port&quot;: 0,
    &quot;DatabaseName&quot;: null,
    &quot;Username&quot;: null,
    &quot;Password&quot;: null,
    &quot;InitialSql&quot;: null,
    &quot;TimeZone&quot;: null,
    &quot;PrintRows&quot;: false,
    &quot;UseWindowsAuth&quot;: false,
    &quot;SqlFetchBatchSize&quot;: 100000,
    &quot;UseSSL&quot;: false,
    &quot;JdbcProperties&quot;: null,
    &quot;GenericDatabaseConfig&quot;: {
        &quot;DatabaseJdbcUrl&quot;: &quot;${jdbcURL}&quot;,
        &quot;SqlDriverClassName&quot;: &quot;com.taosdata.jdbc.rs.RestfulDriver&quot;,
        &quot;ResolutionInNanoseconds&quot;: 1000,
        &quot;ZonedColumnTypes&quot;: []
    }
}
`
          "
        ><code class="language-json"></code></pre>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.seeq.step3desc3') }}</span>
      </li>
    </ol>
    <h2 id="seeq-example">{{ $t('docs.tools.seeq.step4full') }}</h2>
    <p> {{ $t('docs.tools.seeq.step4desc') }} </p>
    <p> {{ $t('docs.tools.seeq.step4desc1') }} </p>
    <pre
      v-highlight="
        `{
    &quot;QueryDefinitions&quot;: [
        {
            &quot;Name&quot;: &quot;SmartMeters&quot;,
            &quot;Type&quot;: &quot;SIGNAL&quot;,
            &quot;Sql&quot;: &quot;SELECT ts, voltage FROM test.\${tablename} &quot;,
            &quot;Enabled&quot;: true,
            &quot;TestMode&quot;: false,
            &quot;TestQueriesDuringSync&quot;: true,
            &quot;InProgressCapsulesEnabled&quot;: false,
            &quot;Variables&quot;: [ {
                    &quot;Names&quot;:  [ &quot;tablename&quot;, &quot;location&quot;, &quot;groupid&quot; ],
                    &quot;Values&quot;: [ [ &quot;\${result}&quot; ] ],
                    &quot;Sql&quot; : &quot;select distinct tbname as tablename, location, groupid from test.meters;&quot;
                } ],
            &quot;Properties&quot;: [ {
                    &quot;Name&quot;: &quot;Name&quot;,
                    &quot;Value&quot;: &quot;\${tablename} voltage at \${location} with groupid=\${groupid}&quot;,
                    &quot;Sql&quot;: null,
                    &quot;Uom&quot;: &quot;string&quot;
                }, {
                    &quot;Name&quot;: &quot;Description&quot;,
                    &quot;Value&quot;: &quot;This is the voltage measured by \${tablename} at location \${location} with groupid: \${groupid} &quot;,
                    &quot;Sql&quot;: null,
                    &quot;Uom&quot;: &quot;string&quot;
                }, {
                    &quot;Name&quot;: &quot;Interpolation Method&quot;,
                    &quot;Value&quot;: &quot;linear&quot;,
                    &quot;Sql&quot;: null,
                    &quot;Uom&quot;: &quot;string&quot;
                }, {
                    &quot;Name&quot;: &quot;Maximum Interpolation&quot;,
                    &quot;Value&quot;: &quot;2day&quot;,
                    &quot;Sql&quot;: null,
                    &quot;Uom&quot;: &quot;string&quot;
                }
            ],
            &quot;CapsuleProperties&quot;: null
        }
    ],
    &quot;Type&quot;: &quot;GENERIC&quot;,
    &quot;Hostname&quot;: null,
    &quot;Port&quot;: 0,
    &quot;DatabaseName&quot;: null,
    &quot;Username&quot;: null,
    &quot;Password&quot;: null,
    &quot;InitialSql&quot;: null,
    &quot;TimeZone&quot;: null,
    &quot;PrintRows&quot;: false,
    &quot;UseWindowsAuth&quot;: false,
    &quot;SqlFetchBatchSize&quot;: 100000,
    &quot;JdbcProperties&quot;: null,
    &quot;GenericDatabaseConfig&quot;: {
        &quot;DatabaseJdbcUrl&quot;: &quot;${jdbcURL}&quot;,
        &quot;SqlDriverClassName&quot;: &quot;com.taosdata.jdbc.rs.RestfulDriver&quot;,
        &quot;ResolutionInNanoseconds&quot;: 1,
        &quot;ZonedColumnTypes&quot;: []
    }
}
`
      "
    ><code class="language-json"></code></pre>
    <p> {{ $t('docs.tools.seeq.step4desc2') }} </p>
    <pre
      v-highlight="
        `select distinct tbname as tablename, location, groupid from test.meters
`
      "
    ><code class="language-sql"></code></pre>
    <p> {{ $t('docs.tools.seeq.step4desc3') }} </p>
    <p>
      {{ $t('docs.tools.seeq.step4desc4')
      }}<a href="https://support.seeq.com/kb/latest/cloud/creating-a-query-definition-for-the-sql-connector#id-(R62)CreatingaQueryDefinitionfortheSQLConnector-UsingVariables">{{
        $t('docs.tools.seeq.step4desc41')
      }}</a
      >{{ $t('docs.tools.seeq.step4desc42') }}
    </p>
  </div>
</template>

<script>
import { decrypt } from '@/utils';
export default {
  props: {
    token: {
      type: String,
      default: ''
    },
    url: {
      type: String,
      default: ''
    }
  },
  data() {
    return {};
  },
  computed: {
    decryptPwd() {
      return decrypt(localStorage.getItem("pwd")) || '';
    },
    jdbcURL() {
      return 'jdbc:TAOS-RS://' + this.url?.replace(/https?:\/\//, '') + '?useSSL=false&user=' + `${localStorage.getItem('username')}&password=${this.decryptPwd}`;
      // return `jdbc:TAOS-RS://localhost:6041?useSSL=false&user=root&password=taosdata`
    }
  }
};
</script>
<style lang="scss" scoped>
.seeq-ol {
  padding-left: 0;
  .seeq-span {
    line-height: 3rem;
  }
}
</style>
