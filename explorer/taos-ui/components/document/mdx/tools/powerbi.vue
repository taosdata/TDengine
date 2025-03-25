<template>
  <div class="docs">
    <p><a :href="`https://powerbi.microsoft.com/`">Power BI</a> {{ t('tools.powerbi.desc') }}</p>
    <h2 id="powerbi-repare">{{ t('tools.powerbi.step1full') }}</h2>
    <p>
      {{ t('tools.powerbi.step1desc') }}
      <a :href="`https://www.microsoft.com${biURLPart}/download/details.aspx?id=58494`">{{
        t('tools.powerbi.step1desc1')
      }}</a
      >{{ t('tools.powerbi.step1desc2') }}
    </p>
    <h2 id="powerbi-install">{{ t('tools.powerbi.step2full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ t('connector.odbc.step11desc1') }}
        <a
          :href="`https://learn.microsoft.com${runtimeURLPart}/cpp/windows/latest-supported-vc-redist?view=msvc-170`"
          >{{ t('connector.odbc.step11desc2') }}</a
        >{{ t('connector.odbc.step11desc3') }}
      </li>
      <li class="odbc-span">
        {{ t('connector.odbc.step12desc1')
        }}<a
          :href="`${OfficalUrl}/assets-download/3.0/TDengine-enterprise-client-${instance.version}-Windows-x64.exe`"
          >{{ t('connector.odbc.step12desc2') }}</a
        >{{ t('connector.odbc.step12desc3') }}
      </li>
    </ol>
    <h2 id="powerbi-config">{{ t('tools.powerbi.step3full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ t('connector.odbc.step21desc') }}
      </li>
      <li class="odbc-span">
        {{ t('connector.odbc.step22desc') }}
      </li>
      <li class="odbc-span">
        {{ t('connector.odbc.step23desc') }}
        <div class="gf-input">
          <div class="gf-input-label">{{ t('connector.odbc.step23desc1') }}</div>
          <div class="gf-input-value">{{ t('connector.odbc.step23desc2') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ t('connector.odbc.step23desc3') }}</div>
          <div class="gf-input-value">{{ t('connector.odbc.step23desc4') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ t('connector.odbc.step23desc5') }}</div>
          <CopyText :text="dsn" class="gf-input-value" />
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ t('connector.odbc.step23desc6') }}</div>
          <div class="gf-input-value">{{ t('connector.odbc.step23desc7') }}</div>
        </div>
      </li>
      <li class="odbc-span">{{ t('connector.odbc.step24desc', [dsn]) }}</li>
    </ol>
    <h2 id="powerbi-import">{{ t('tools.powerbi.step4full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ t('tools.powerbi.step4desc') }}
      </li>
      <li class="odbc-span">
        {{ t('tools.powerbi.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ t('tools.powerbi.step4desc2') }}
      </li>
    </ol>
    <p>{{ t('tools.powerbi.step4desc3') }}</p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc4label') }}</strong
        >{{ t('tools.powerbi.step4desc4') }}
      </li>
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc5label') }}</strong
        >{{ t('tools.powerbi.step4desc5')
        }}<a :href="`${docs.urlPrefix}/taos-sql/distinguished/`">{{ t('tools.powerbi.step4desc6') }}</a
        >{{ t('tools.powerbi.step4desc7') }}
      </li>
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc8label') }}</strong
        >{{ t('tools.powerbi.step4desc8') }}
      </li>
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc9label') }}</strong
        >{{ t('tools.powerbi.step4desc9') }}
      </li>
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc10label') }}</strong
        >{{ t('tools.powerbi.step4desc10') }}
      </li>
      <li class="odbc-span">
        <strong>{{ t('tools.powerbi.step4desc11label') }}</strong
        >{{ t('tools.powerbi.step4desc11') }}
      </li>
    </ol>
    <h2 id="powerbi-example">{{ t('tools.powerbi.step5full') }}</h2>
    <p>
      {{ t('tools.powerbi.step5desc') }}<a :href="`${TdDocsUrl}/concept/`">{{ t('tools.powerbi.step5desc01') }}</a
      >{{ t('tools.powerbi.step5desc02') }}
    </p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ t('tools.powerbi.step5desc1') }}
        <pre
          v-highlight="`select distinct tbname, groupid, location from test.meters;`"
          class="pre-code"
        ><code class="language-sql"></code></pre>
      </li>
      <li class="odbc-span">
        {{ t('tools.powerbi.step5desc2') }}
        <pre
          v-highlight="
            `select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h);`
          "
          class="pre-code"
        ><code class="language-sql"></code></pre>
      </li>
      <li class="odbc-span">
        {{ t('tools.powerbi.step5desc3')
        }}<a :href="`https://learn.microsoft.com${biURLPart}/power-bi/`">{{ t('tools.powerbi.step5desc4') }}</a
        >{{ t('tools.powerbi.step5desc5') }}
      </li>
    </ol>
  </div>
</template>
<script lang="ts" setup>
import { t } from 'locales';
import { dsn } from '../utils';
import { TdDocsUrl, OfficalUrl, isEn, instance, docs } from 'config';

const biURLPart = computed(() => (isEn.value ? '' : '/zh-cn'));
const runtimeURLPart = computed(() => (isEn.value ? '/en-us' : '/zh-cn'));
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}

.odbc-ol {
  padding-left: 0;

  .odbc-span {
    padding-left: 20px;
    line-height: 30px;
    text-indent: -20px;

    .pre-code {
      text-indent: 0;

      // margin-left: -17px;
    }
  }
}

.gf-input {
  display: flex;
  align-items: center;
  width: 70%;
  margin: 10px;
  margin-left: 0;
  font-size: 14px;
  text-indent: 0;

  .gf-input-label {
    width: 135px;
  }

  .gf-input-value {
    flex: 1;
    padding: 0 10px;
    line-height: 40px;
    background-color: #f6f8fa;
    border-radius: 5px;
  }
}
</style>
