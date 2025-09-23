<template>
  <div class="docs">
    <p><a :href="`https://www.tableau.com`">Tableau</a> {{ $t('tools.tableau.desc') }}</p>
    <h2 id="tableau-prepare">{{ $t('tools.tableau.step1full') }}</h2>
    <p>
      {{ $t('tools.tableau.step1desc') }}
      <a :href="`https://www.tableau.com${biURLPart}/products/desktop/download`">{{ $t('tools.tableau.step1desc1') }}</a
      >{{ $t('tools.tableau.step1desc2') }}
    </p>
    <h2 id="tableau-install">{{ $t('tools.tableau.step2full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('connector.odbc.step11desc1') }}
        <a
          :href="`https://learn.microsoft.com${runtimeURLPart}/cpp/windows/latest-supported-vc-redist?view=msvc-170`"
          >{{ $t('connector.odbc.step11desc2') }}</a
        >{{ $t('connector.odbc.step11desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('connector.odbc.step12desc1') }}<a :href="installUrlWindows">{{ $t('connector.odbc.step12desc2') }}</a
        >{{ $t('connector.odbc.step12desc3') }}
      </li>
    </ol>
    <h2 id="tableau-config">{{ $t('tools.tableau.step3full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('connector.odbc.step21desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('connector.odbc.step22desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('connector.odbc.step23desc') }}
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('connector.odbc.step23desc1') }}</div>
          <div class="gf-input-value">{{ $t('connector.odbc.step23desc2') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('connector.odbc.step23desc3') }}</div>
          <div class="gf-input-value">{{ $t('connector.odbc.step23desc4') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('connector.odbc.step23desc5') }}</div>
          <CopyText :text="dsn" class="gf-input-value" />
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('connector.odbc.step23desc6') }}</div>
          <div class="gf-input-value">{{ $t('tools.tableau.step23desc1') }}</div>
        </div>
      </li>
      <li class="odbc-span">{{ $t('connector.odbc.step24desc', [dsn]) }}</li>
    </ol>
    <h2 id="tableau-import">{{ $t('tools.tableau.step4full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.tableau.step4desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step4desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step4desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step4desc4') }}
      </li>
    </ol>
    <h2 id="tableau-example">{{ $t('tools.tableau.step5full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.tableau.step5desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step5desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step5desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.tableau.step5desc4') }}
        <a :href="`https://www.tableau.com${biURLPart}/support/knowledgebase#desktop`">{{
          $t('tools.tableau.step5desc5')
        }}</a
        >{{ $t('tools.tableau.step5desc6') }}
      </li>
    </ol>
  </div>
</template>

<script lang="ts" setup>
import { isEn, installUrlWindows } from 'config';
import { dsn } from '../utils';

const biURLPart = isEn.value ? '/zh-cn' : '';
const runtimeURLPart = isEn.value ? '/zh-cn' : '/en-us';
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
