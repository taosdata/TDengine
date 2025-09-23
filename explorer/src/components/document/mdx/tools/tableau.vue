<template>
  <div class="docs">
    <p><a :href="`https://www.tableau.com`">Tableau</a> {{ $t('docs.tools.is') }}{{ $t('docs.tools.tableau.desc') }} </p>
    <h2 id="tableau-prepare">{{ $t('docs.tools.tableau.step1full') }}</h2>
    <p
      >{{ $t('docs.tools.tableau.step1desc') }} <a :href="`https://www.tableau.com${biURLPart}/products/desktop/download`">{{ $t('docs.tools.tableau.step1desc1') }}</a
      >{{ $t('docs.tools.tableau.step1desc2') }}
    </p>
    <h2 id="tableau-install">{{ $t('docs.tools.tableau.step2full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.connector.odbc.step11desc1') }}
        <a :href="`https://learn.microsoft.com${biURLPart}/cpp/windows/latest-supported-vc-redist?view=msvc-170`">{{ $t('docs.connector.odbc.step11desc2') }}</a
        >{{ $t('docs.connector.odbc.step11desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.connector.odbc.step12desc1')
        }}<a :href="installUrlWindows">{{ $t('docs.connector.odbc.step12desc2') }}</a
        >{{ $t('docs.connector.odbc.step12desc3') }}
      </li>
    </ol>
    <h2 id="tableau-config">{{ $t('docs.tools.tableau.step3full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.connector.odbc.step21desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.connector.odbc.step22desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.connector.odbc.step23desc') }}
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc1') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc2') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc3') }}</div>
          <div class="gf-input-value">{{ $t('docs.connector.odbc.step23desc4') }}</div>
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc5') }}</div>
          <CopyText
            :text="endpoint"
            class="gf-input-value"
          />
        </div>
        <div class="gf-input">
          <div class="gf-input-label">{{ $t('docs.connector.odbc.step23desc6') }}</div>
          <div class="gf-input-value">{{ $t('docs.tools.tableau.step23desc1') }}</div>
        </div>
      </li>
      <li class="odbc-span">{{ $t('docs.connector.odbc.step24desc', [successEndpoint]) }} </li>
    </ol>
    <h2 id="tableau-import">{{ $t('docs.tools.tableau.step4full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step4desc') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step4desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step4desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step4desc4') }}
      </li>
    </ol>
    <h2 id="tableau-example">{{ $t('docs.tools.tableau.step5full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step5desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step5desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step5desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.tableau.step5desc4') }} <a :href="`https://www.tableau.com${biURLPart}/support/knowledgebase#desktop`">{{ $t('docs.tools.tableau.step5desc5') }}</a
        >{{ $t('docs.tools.tableau.step5desc6') }}
      </li>
    </ol>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';
import { getLocalLang } from '@/utils';
const { $IS_COMMUNITY } = inject('globalCustomProperties') as GlobalCustomProperties;

const props = defineProps<DocsProps>();

const endpoint = computed(() => {
  return `taos://${props.url.replace(/https?:\/\//, '')}`;
});
const successEndpoint = computed(() => {
  return `taos://${props.user}:${props.password}@${props.url.replace(/https?:\/\//, '')}`;
});
const urlPart = computed(() => {
  return getLocalLang().includes('en') ? 'tdengine' : 'taosdata';
});
const TDengineVersion = computed(() => {
  return localStorage.getItem('td_version');
});
const biURLPart = computed(() => {
  return getLocalLang().includes('en') ? '/en-us' : '/zh-cn';
});
const urlEnterprise = computed(() => {
  return $IS_COMMUNITY ? '' : '-enterprise';
});


</script>
<style lang="scss" scoped>
.seeq-ol {
  padding-left: 0;

  .seeq-span {
    line-height: 30px;
  }
}

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

