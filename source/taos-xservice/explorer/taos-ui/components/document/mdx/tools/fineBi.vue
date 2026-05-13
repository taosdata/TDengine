<template>
  <div class="docs">
    <p>{{ $t('tools.finebi.desc') }}</p>
    <p>{{ $t('tools.finebi.desc1') }}</p>
    <h2 id="finebi-prepare">{{ $t('tools.finebi.step1full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.finebi.step11desc') }}
        <a :href="`https://${runtimeURLPart}/download`">{{ $t('tools.finebi.step11desc1') }}</a
        >{{ $t('tools.finebi.step11desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step12desc') }}
        <a :href="`https://market.fanruan.com/plugin/1052a471-0239-4cd8-b832-045d53182c5d`">{{
          $t('tools.finebi.step12desc1')
        }}</a
        >{{ $t('tools.finebi.step12desc2') }}
      </li>
    </ol>

    <h2 id="finebi-install">{{ $t('tools.finebi.step2full') }}</h2>
    <p>
      {{ $t('tools.finebi.step2desc') }} <a href="https://maven.org/">maven.org</a>{{ $t('tools.finebi.step2desc1') }}
    </p>

    <h2 id="finebi-config">{{ $t('tools.finebi.step3full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.finebi.step31desc') }}
        <ol class="odbc-ol">
          <li class="odbc-span">
            {{ $t('tools.finebi.step31desc1') }}
          </li>
          <li class="odbc-span">
            {{ $t('tools.finebi.step31desc2') }}
          </li>
        </ol>
      </li>

      <li class="odbc-span">
        {{ $t('tools.finebi.step32desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step33desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step34desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step35desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step36desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step37desc') }}
        <div class="gf-input">
          <CopyText :text="DSN" class="gf-input-value" />
        </div>
        {{ $t('tools.finebi.step37desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step38desc') }}
      </li>
    </ol>
    <h2 id="finebi-import">{{ $t('tools.finebi.step4full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.finebi.step41desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step41desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step41desc2') }}
      </li>
    </ol>
    <h2 id="finebi-example">{{ $t('tools.finebi.step5full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.finebi.step51desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step51desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.finebi.step51desc2') }}
      </li>
    </ol>
  </div>
</template>

<script lang="ts" setup>
import { jdbcURL } from '../utils';
import { isEn } from 'config';
const runtimeURLPart = isEn.value ? 'www.finebi.com/product' : 'intl.finebi.com';
const DSN = jdbcURL.value + '&fineBIDialect=mysql';
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
  width: 85%;
  margin: 10px;
  margin-left: 0;
  font-size: 14px;
  text-indent: 0;

  .gf-input-label {
    width: 200px;
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
