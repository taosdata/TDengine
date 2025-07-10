<template>
  <div class="docs">
    <p> {{ $t('docs.tools.superset.topdesc') }} <a href="https://superset.apache.org">Superset</a>{{ $t('docs.tools.superset.topdesc1') }} </p>
    <h2 id="superset-repare">{{ $t('docs.tools.superset.step1') }}</h2>
    <p
      >{{ $t('docs.tools.superset.step1desc') }} <a :href="`https://superset.apache.org/`">{{ $t('docs.tools.superset.step1desc1') }}</a
      >{{ $t('docs.tools.superset.step1desc2') }}
    </p>
    <h2 id="superset-install">{{ $t('docs.tools.superset.step2full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step2desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step2desc1') }}
        <pre
          v-highlight="     
`pip3 install taospy
pip3 install taos-ws-py`
          "
        ><code class="language-bash"></code></pre>
      </li>
    </ol>
    <h2 id="superset-config">{{ $t('docs.tools.superset.step3full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step3desc') }} <a :href="`https://superset.apache.org/docs/intro`">{{ $t('docs.tools.superset.step31desc1') }}</a
        >{{ $t('docs.tools.superset.step31desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step32desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step33desc') }}
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.tools.superset.step33desc1') }}</div>
          <div class="gf-input-value">{{ $t('docs.tools.superset.step33desc2') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.tools.superset.step33desc3') }}</div>
          <CopyText
            :text="endpoint"
            class="gf-input-value"
          />
        </div>
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step34desc') }}
      </li>
    </ol>
    <h2 id="superset-import">{{ $t('docs.tools.superset.step4full') }}</h2>
    <ol class="odbc-ol">
      <p>
        {{ $t('docs.tools.superset.step4desc') }}
      </p>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc4') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc5') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step4desc6') }}
      </li>
    </ol>
    <h2 id="superset-example">{{ $t('docs.tools.superset.step5full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step5desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step5desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step5desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.superset.step5desc4') }}
      </li>

      <li class="odbc-span">
        {{ $t('docs.tools.superset.step5desc5') }} <a :href="`https://superset.apache.org/docs/using-superset/creating-your-first-dashboard`">{{ $t('docs.tools.superset.step5desc6') }}</a
        >{{ $t('docs.tools.superset.step5desc7') }}
      </li>
    </ol>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';

const props = defineProps<DocsProps>();

const endpoint = computed(() => {
  return `taosws://${props.user}:${props.password}@${props.url.replace(/^[a-z]+:\/\//, '')}`;
});
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}
.odbc-ol {
  padding-left: 0;
  .odbc-span {
    line-height: 30px;
    padding-left: 20px;
    text-indent: -20px;
    .pre-code {
      text-indent: 0px;
      // margin-left: -17px;
    }
  }
}
.gf-input {
  width: 70%;
  display: flex;
  align-items: center;
  margin: 10px;
  margin-left: 0px;
  font-size: 14px;
  text-indent: 0px;
  .gf-input-label {
    width: 160px;
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
