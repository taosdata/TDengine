<template>
  <div>
    <p>
      {{ $t('docs.tools.superset.desc') }}
    </p>
    <p>{{ $t('docs.tools.superset.desc1') }}</p>
    <h2 id="superset-repare">{{ $t('docs.tools.superset.step1') }}</h2>
    <ol class="seeq-ol">
      <span class="seeq-span"
        >{{ $t('docs.tools.superset.step11desc')
        }}<a href="https://superset.apache.org/">{{ $t('docs.tools.superset.step11desc1') }}</a
        >{{ $t('docs.tools.superset.step11desc2') }}</span
      >
    </ol>
    <h2>{{ $t('docs.tools.superset.step2full') }}</h2>
    <span class="seeq-span">{{ $t('docs.tools.superset.step2desc') }} </span>
    <pre
      v-highlight="
        `pip3 install taospy
pip3 install taos-ws-py`
      "
    ><code class="language-json"></code></pre>
    <h2 id="superset-config">{{ $t('docs.tools.superset.step3full') }}</h2>
    <p>
      <i class="el-icon-s-opportunity" style="font-size: 20px; color: gold"></i>
      <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
    </p>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step31desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step32desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step33desc') }} </span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step34desc') }}</span>
        <pre v-highlight="`${connectUrl}`"><code class="language-json"></code></pre>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step35desc') }}</span>
      </li>
    </ol>
    <h2 id="superset-createds">{{ $t('docs.tools.superset.step4full') }}</h2>
    <span>{{ $t('docs.tools.superset.step40desc') }}</span>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step41desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step42desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step43desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step44desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step45desc') }}</span>
      </li>
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step46desc') }}</span>
      </li>
    </ol>

    <h2 id="superset-report">{{ $t('docs.tools.superset.step5full') }}</h2>
    <span>{{ $t('docs.tools.superset.step5desc') }}</span>
    <ol class="seeq-ol">
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step51desc') }}</span>
      </li>
      <img src="../assets/superset/superset-demo1.jpeg" alt="" />
      <li>
        <span class="seeq-span">{{ $t('docs.tools.superset.step52desc') }}</span>
      </li>
      <img src="../assets/superset/superset-demo2.jpeg" alt="" srcset="" />
    </ol>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';

const props = defineProps<DocsProps>();

const connectUrl = computed(() => {
  return `taosws://${props.user}:${props.password}@${props.url.replace(/^[a-z]+:\/\//, '')}`;
});
</script>
<style lang="scss" scoped>
.seeq-ol {
  padding-left: 0;

  .seeq-span {
    line-height: 30px;
  }
}
</style>
