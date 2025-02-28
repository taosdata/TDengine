<template>
  <div class="docs">
    <p>
      <a href="https://www.yonghongtech.com">{{ t('tools.yonghongbi.desc') }}</a
      >{{ t('tools.yonghongbi.desc1') }}
    </p>
    <h2 id="yonghongbi-repare">{{ t('tools.yonghongbi.step1') }}</h2>
    <p>
      {{ t('tools.yonghongbi.step11desc')
      }}<a href="https://www.yonghongtech.com/cp/desktop/">{{ t('tools.yonghongbi.step11desc1') }}</a
      >{{ t('tools.yonghongbi.step11desc2') }}
    </p>
    <h2 id="yonghongbi-install">{{ t('tools.yonghongbi.step2full') }}</h2>
    <p>
      {{ t('tools.yonghongbi.step2desc') }}<a href="https://maven.org/">maven.org</a
      >{{ t('tools.yonghongbi.step2desc1')
      }}<a href="https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.7/taos-jdbcdriver-3.2.7-dist.jar"
        >3.2.7</a
      >{{ t('tools.yonghongbi.step2desc2') }}
    </p>
    <h2 id="yonghongbi-config">{{ t('tools.yonghongbi.step3full') }}</h2>
    <ol class="seeq-ol">
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step31desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step32desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step33desc') }}
        <pre v-highlight="`${jdbcURL}`" class="pre-code"><code class="language-json"></code></pre>
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step34desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step35desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step36desc') }}
      </li>
    </ol>
    <h2 id="yonghongbi-createds">{{ t('tools.yonghongbi.step4full') }}</h2>
    <ol class="seeq-ol">
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step41desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step42desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step43desc') }}
      </li>
    </ol>
    <p>
      {{ t('tools.yonghongbi.step44desc')
      }}<a href="https://taosdata.com/taos-sql/distinguished/">{{ t('tools.yonghongbi.step44desc1') }}</a>
      {{ t('tools.yonghongbi.step44desc2') }}
    </p>
    <p>{{ t('tools.yonghongbi.step45desc') }}</p>
    <ul>
      <li>
        <p><strong>_wstart</strong>{{ t('tools.yonghongbi.step45desc1') }}</p>
      </li>
      <li>
        <p><strong>count(*)</strong>{{ t('tools.yonghongbi.step45desc2') }}</p>
      </li>
      <li>
        <p><strong>?{interval}</strong>{{ t('tools.yonghongbi.step45desc3') }}</p>
      </li>
      <li>
        <p><strong>?{metric}</strong>{{ t('tools.yonghongbi.step45desc4') }}</p>
      </li>
      <li>
        <p><strong>?{from} ?{to}</strong>{{ t('tools.yonghongbi.step45desc5') }}</p>
      </li>
    </ul>
    <p>{{ t('tools.yonghongbi.step45desc6') }}</p>
    <h2 id="yonghongbi-report">{{ t('tools.yonghongbi.step5full') }}</h2>
    <ol class="seeq-ol">
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step51desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step52desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step53desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step54desc') }}
      </li>
      <li class="seeq-span">
        {{ t('tools.yonghongbi.step55desc')
        }}<a href="https://www.yonghongtech.com/help/Z-Suite/10.0/ch/">{{ t('tools.yonghongbi.step55desc1') }}</a
        >{{ t('tools.yonghongbi.step55desc2') }}
      </li>
    </ol>
  </div>
</template>
<script lang="ts" setup>
import { t } from 'locales';
import { jdbcURL } from '../utils';
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}

.seeq-ol {
  padding-left: 0;

  .seeq-span {
    padding-left: 20px;
    line-height: 30px;
    text-indent: -20px;

    .pre-code {
      text-indent: 0;

      // margin-left: -17px;
    }
  }
}
</style>
