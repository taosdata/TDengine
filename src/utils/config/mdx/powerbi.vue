<template>
  <div>
    <p><a :href="`https://powerbi.microsoft.com/`">Power BI</a> {{ $t('docs.tools.powerbi.desc') }} </p>
    <h2 id="powerbi-repare">{{ $t('docs.tools.powerbi.step1full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">
          {{ $t('docs.tools.powerbi.step1desc3') }}
        </span>
      </li>
      <li>
        <span class="odbc-span">
          {{ $t('docs.tools.powerbi.step1desc') }} 
          <a :href="`https://www.microsoft.com${biURLPart}/download/details.aspx?id=58494`">{{ $t('docs.tools.powerbi.step1desc1') }}</a>
          {{ $t('docs.tools.powerbi.step1desc2') }}
        </span>
      </li>
    </ol>
    <h2 id="powerbi-install">{{ $t('docs.tools.powerbi.step2full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">
          {{ $t('docs.connector.odbc.step11desc1') }}
          <a :href="`https://learn.microsoft.com${biURLPart}/cpp/windows/latest-supported-vc-redist?view=msvc-170`">{{ $t('docs.connector.odbc.step11desc2') }}</a
          >{{ $t('docs.connector.odbc.step11desc3') }}
        </span>
      </li>
      <li>
        <span class="odbc-span">
          {{ $t('docs.connector.odbc.step12desc1') }}<a :href="`https://www.${urlPart}.com/assets-download/3.0/TDengine${this.urlEnterprise}-client-${TDengineVersion}-Windows-x64.exe`">
            {{ $t('docs.connector.odbc.step12desc2') }}</a
          >{{ $t('docs.connector.odbc.step12desc3') }}
        </span>
      </li>
    </ol>
    <h2 id="powerbi-config">{{ $t('docs.tools.powerbi.step3full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">{{ $t('docs.connector.odbc.step21desc') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.connector.odbc.step22desc') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.connector.odbc.step23desc') }}</span>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc1') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc2') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc3') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc4') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc8') }}</div>
          <CopyText
            :text="endpoint"
            :isShowBtnText="true"
            class="gf-input-value"
          />
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc6') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc7') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc10') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc11') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc12') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc13') }}</div>
        </div>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.connector.odbc.step24desc', [successEndpoint]) }}</span>
      </li>
    </ol>
    <h2 id="powerbi-import">{{ $t('docs.tools.powerbi.step4full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc1') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.powerbi.step4desc2') }}</span>
      </li>
    </ol>
    <br />
    <p>{{ $t('docs.tools.powerbi.step4desc3') }}</p>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc4') }}</span>
      </li>
      <li>
        <span class="odbc-span"
          >{{ $t('docs.tools.powerbi.step4desc5') }}<a :href="`${$t('urlPart')}/reference/taos-sql/distinguished/`">{{ $t('docs.tools.powerbi.step4desc6') }}</a
          >{{ $t('docs.tools.powerbi.step4desc7') }}</span
        >
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc8') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc9') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc10') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step4desc11') }}</span>
      </li>
    </ol>
    <h2 id="powerbi-example">{{ $t('docs.tools.powerbi.step5full') }}</h2>
    <p>
      {{ $t('docs.tools.powerbi.step5desc') }}
      <a :href="`https://docs.${urlPart}.com/concept/`">{{ $t('docs.tools.powerbi.step5desc01') }}</a>
      {{ $t('docs.tools.powerbi.step5desc0') }}
    </p>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step5desc1') }}</span>
        <pre v-highlight="`select distinct tbname, groupid, location from test.meters;`"><code class="language-sql"></code></pre>
      </li>
      <li>
        <span class="odbc-span">{{ $t('docs.tools.powerbi.step5desc2') }}</span>
        <pre v-highlight="`select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h);`"><code class="language-sql"></code></pre>
      </li>
      <li>
        <span class="odbc-span"
          >{{ $t('docs.tools.powerbi.step5desc3') }}<a :href="`https://learn.microsoft.com${biURLPart}/power-bi/`">{{ $t('docs.tools.powerbi.step5desc4') }}</a
          >{{ $t('docs.tools.powerbi.step5desc5') }}</span
        >
      </li>
    </ol>
  </div>
</template>

<script>
import { IsAliyun } from '@/const';
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
    endpoint() {
      return `taos://${this.url.replace(/https?:\/\//, '')}`;
    },
    successEndpoint() {
      return `taos://${this.user}:${this.password}@${this.url.replace(/https?:\/\//, '')}`;
    },
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    TDengineVersion() {
      return localStorage.getItem('agent_version')
    },
    biURLPart() {
      return this.$i18n.locale.includes('en') ? '/en-us' : '/zh-cn';
    },
    docsPart() {
      return this.$i18n.locale.includes('en') ? 'docs-en' : 'docs'
    },
    urlEnterprise() {
      return this.$COMMUNITY ? '' : '-enterprise'
    }
  }
};
</script>
<style lang="scss" scoped>
.odbc-ol {
  padding-left: 0;
  .odbc-span {
    line-height: 30px;
  }
}
.gf-input {
  width: 100%;
  display: flex;
  align-items: center;
  margin: 10px;
  margin-left: 20px;
  font-size: 14px;
  .gf-input-label {
    width: 135px;
  }
  .gf-input-value {
    flex: 1;
    background-color: #f6f8fa;
    border-radius: 5px;
    line-height: 40px;
    padding: 0 10px;
  }
}
</style>
