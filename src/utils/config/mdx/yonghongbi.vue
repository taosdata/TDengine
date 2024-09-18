<template>
  <div>
    <p
      ><a href="https://www.yonghongtech.com">{{ $t('docs.tools.yonghongbi.desc') }}</a
      >{{ $t('docs.tools.yonghongbi.desc1') }}
    </p>
    <h2 id="yonghongbi-repare">{{ $t('docs.tools.yonghongbi.step1') }}</h2>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span"
          >{{ $t('docs.tools.yonghongbi.step11desc') }}<a href="https://www.yonghongtech.com/cp/desktop/">{{ $t('docs.tools.yonghongbi.step11desc1') }}</a
          >{{ $t('docs.tools.yonghongbi.step11desc2') }}</span
          >
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step12desc') }}</span>
      </li>
    </ol>
    <h2 id="yonghongbi-install">{{ $t('docs.tools.yonghongbi.step2full') }}</h2>
      <span class="seeq-span"
        >{{ $t('docs.tools.yonghongbi.step2desc') }}<a href="https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver/versions">maven.org</a>{{ $t('docs.tools.yonghongbi.step2desc1')
        }}<a href="https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.7/taos-jdbcdriver-3.2.7-dist.jar">3.2.7</a>{{ $t('docs.tools.yonghongbi.step2desc2') }}
      </span>
    <h2 id="yonghongbi-config">{{ $t('docs.tools.yonghongbi.step3full') }}</h2>
    <p>
      <i class="el-icon-s-opportunity" style="color: gold;font-size: 20px"></i>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step31desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step32desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step33desc') }}
        </span>
        <pre v-highlight="`${jdbcURL}`"><code class="language-json"></code></pre>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step34desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step35desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step36desc') }}</span>
      </li>
    </ol>
    <h2 id="yonghongbi-createds">{{ $t('docs.tools.yonghongbi.step4full') }}</h2>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step41desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step42desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step43desc') }}</span>
      </li>
    </ol>
    <p>
      {{ $t('docs.tools.yonghongbi.step44desc') }}<a :href="`${$t('urlPart')}/reference/taos-sql/distinguished/`">{{ $t('docs.tools.yonghongbi.step44desc1') }}</a>
      {{ $t('docs.tools.yonghongbi.step44desc2') }}</p
    >
    <p> {{ $t('docs.tools.yonghongbi.step45desc') }} </p>
    <pre v-highlight="'select _wstart ws, count(*) cnt from supertable where tbname=?{metric} and ts >= ?{from} and ts < ?{to} interval(?{interval})'"><code class="language-json"></code></pre>
    <p> {{ $t('docs.tools.yonghongbi.step45desc0') }} </p>
    <ul>
      <li>
        <p> <strong>_wstart</strong>{{ $t('docs.tools.yonghongbi.step45desc1') }} </p>
      </li>
      <li>
        <p> <strong>count(*)</strong>{{ $t('docs.tools.yonghongbi.step45desc2') }} </p>
      </li>
      <li>
        <p> <strong>?{interval}</strong>{{ $t('docs.tools.yonghongbi.step45desc3') }} </p>
      </li>
      <li>
        <p> <strong>?{metric}</strong>{{ $t('docs.tools.yonghongbi.step45desc4') }} </p>
      </li>
      <li>
        <p> <strong>?{from} ?{to}</strong>{{ $t('docs.tools.yonghongbi.step45desc5') }} </p>
      </li>
    </ul>
    <p> {{ $t('docs.tools.yonghongbi.step45desc6') }} </p>
    <h2 id="yonghongbi-report">{{ $t('docs.tools.yonghongbi.step5full') }}</h2>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step51desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step52desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step53desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.yonghongbi.step54desc') }}</span>
      </li>
      <li>
        <span class="seeq-span"
          >{{ $t('docs.tools.yonghongbi.step55desc') }}<a href="https://www.yonghongtech.com/help/Z-Suite/10.0/ch/">{{ $t('docs.tools.yonghongbi.step55desc1') }}</a
          >{{ $t('docs.tools.yonghongbi.step55desc2') }}</span
        >
      </li>
    </ol>
  </div>
</template>

<script>
export default {
  props: {
    token: {
      type: String,
      default: ''
    },
    url: {
      type: String,
      default: ''
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
    jdbcURL() {
      // jdbc:TAOS-RS://192.168.1.98:7541/?user=root&password=taosdata&batchfetch=true&conmode=1
      return 'jdbc:TAOS-RS://' + this.url.replace(/https?:\/\//, '') + '?user=' + this.user + '&password=' + this.password;
    },
    docsPart() {
      return this.$i18n.locale.includes('en') ? 'docs-en' : 'docs'
    }
  }
};
</script>
<style lang="scss" scoped>
.seeq-ol {
  padding-left: 0;
  .seeq-span {
    line-height: 30px;
  }
}
</style>
