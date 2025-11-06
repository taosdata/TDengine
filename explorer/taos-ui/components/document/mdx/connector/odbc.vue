<template>
  <div>
    <p>
      {{ t('connector.odbc.desc') }}<a :href="`https://powerbi.microsoft.com/`">Power BI</a
      >{{ t('connector.odbc.desc1') }}
    </p>
    <h2 id="install">{{ t('connector.odbc.step1full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">
          {{ t('connector.odbc.step11desc1') }}
          <a
            :href="`https://learn.microsoft.com${runtimeURLPart}/cpp/windows/latest-supported-vc-redist?view=msvc-170`"
            >{{ t('connector.odbc.step11desc2') }}</a
          >{{ t('connector.odbc.step11desc3') }}
        </span>
      </li>
      <li>
        <span class="odbc-span">
          {{ t('connector.odbc.step12desc1') }}<a :href="installUrlWindows">{{ t('connector.odbc.step12desc2') }}</a
          >{{ t('connector.odbc.step12desc3') }}
        </span>
      </li>
    </ol>
    <h2 id="config">{{ t('connector.odbc.step2full') }}</h2>
    <ol class="odbc-ol">
      <li>
        <span class="odbc-span">{{ t('connector.odbc.step21desc') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ t('connector.odbc.step22desc') }}</span>
      </li>
      <li>
        <span class="odbc-span">{{ t('connector.odbc.step23desc') }}</span>
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
      <li>
        <span class="odbc-span">{{ t('connector.odbc.step24desc', [dsn]) }}</span>
      </li>
    </ol>
    <h2 id="example">{{ t('connector.odbc.step3') }}</h2>
    <p>{{ t('connector.odbc.step31desc') }}</p>
    <p>
      {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/programming/insert/`">{{ t('common.insert') }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/programming/query/`">{{ t('common.query') }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
import { dsn } from '../utils';
import { isEn, docs, installUrlWindows } from 'config';

const runtimeURLPart = computed(() => (isEn.value ? '/en-us' : ''));
</script>
<style lang="scss" scoped>
.odbc-ol {
  padding-left: 0;

  .odbc-span {
    line-height: 30px;
  }
}

.gf-input {
  display: flex;
  align-items: center;
  width: 50%;
  margin: 10px;
  margin-left: 20px;
  font-size: 14px;

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
