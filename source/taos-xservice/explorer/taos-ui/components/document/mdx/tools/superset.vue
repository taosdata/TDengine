<template>
  <div class="docs">
    <p><a href="https://www.seeq.com/">Superset</a>{{ $t('tools.superset.desc') }}</p>
    <p>{{ $t('tools.superset.topdesc') }}</p>

    <h2 id="superset-prepare">{{ $t('tools.superset.step1') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.superset.step11desc') }}
        <a :href="`https://superset.apache.org/`">{{ $t('tools.superset.step11desc1') }}</a>
        {{ $t('tools.superset.end') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step12desc') }}
        <a :href="`https://pypi.org/project/taospy/`">{{ $t('tools.superset.setup') }}</a>
        {{ $t('tools.superset.end') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step13desc') }}
        <a :href="`https://pypi.org/project/taos-ws-py/`">{{ $t('tools.superset.setup') }}</a>
        {{ $t('tools.superset.end') }}
      </li>
    </ol>

    <h2 id="superset-config">{{ $t('tools.superset.step2') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.superset.step21desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step22desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step23desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step24desc') }}
        <div class="gf-input">
          <CopyText :text="DSN" class="gf-input-value" />
        </div>
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step25desc') }}
      </li>
    </ol>

    <h2 id="superset-import">{{ $t('tools.superset.step3full') }}</h2>
    <ol class="odbc-ol">
      <p>
        {{ $t('tools.superset.step3desc') }}
      </p>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc4') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc5') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step3desc6') }}
      </li>
    </ol>

    <h2 id="superset-analysis">{{ $t('tools.superset.step4full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.superset.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step4desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step4desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step4desc4') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.superset.step4desc5') }}
        <a :href="`https://superset.apache.org/docs/using-superset/creating-your-first-dashboard`">{{
          $t('tools.superset.step4desc6')
        }}</a
        >{{ $t('tools.superset.step4desc7') }}
      </li>
    </ol>
  </div>
</template>

<script lang="ts" setup>
import { dsn } from '../utils';

const DSN = dsn.value.replace('http', 'taosws');
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
    width: 160px;
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
