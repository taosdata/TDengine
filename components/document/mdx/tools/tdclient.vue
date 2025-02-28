<template>
  <div>
    <h2 :id="props.installid">{{ t('tools.cli.step1') }}</h2>
    <slot></slot>
    <p>
      {{ t(props.step1desc) }}
      <b>{{ t('tools.cli.step1desc1') }}</b
      >{{ t('tools.cli.step1desc2') }}<a target="_blank" :href="installUrlLinux">Linux</a>{{ t('tools.cli.step1desc3')
      }}<a target="_blank" :href="installUrlWindows">Windows</a>{{ t('tools.cli.step1desc3') }}
      <a target="_blank" :href="installUrlMac">Mac</a>{{ t('tools.cli.step1desc3')
      }}<a target="_blank" :href="installUrlMacArm">Mac(Arm)</a>{{ t('tools.cli.step1desc4') }}
    </p>
    <h2 :id="props.configid">{{ t('tools.cli.step2') }}</h2>
    <el-tabs model-value="linux">
      <el-tab-pane name="linux" label="Linux">
        <p>{{ t('tools.cli.step2desc') }}</p>
        <pre
          v-highlight="
            `export ${dsnKey}=&quot;${dsn}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <p>{{ t('tools.cli.step2desc1') }}</p>
        <pre
          v-highlight="
            `set ${dsnKey}=${dsn}
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ t('tools.cli.step2desc2') }}</p>
        <pre
          v-highlight="
            `$env:${dsnKey}='${dsn}'
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac">
        <p>{{ t('tools.cli.step2desc3') }}</p>
        <pre
          v-highlight="
            `export ${dsnKey}=&quot;${dsn}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script lang="ts" setup>
import { t } from 'locales';
import { dsn, dsnKey } from '../utils';
import { compareVersion } from 'utils/tdengine';
import { OfficalUrl, instance } from 'config';

interface Props {
  url?: string;
  token?: string;
  installid?: string;
  configid?: string;
  step1desc?: string;
}
const props = withDefaults(defineProps<Props>(), {
  url: '',
  token: '',
  installid: 'installation',
  configid: 'config',
  step1desc: 'tools.cli.step1desc'
});
const isLessThen3_1_1_11 = computed(() => compareVersion(instance.version, '<3.1.1.11'));
const isLessThen3_3_2_1 = computed(() => compareVersion(instance.version, '<3.3.2.1'));
const commonDownloadUrl = computed(
  () =>
    `${OfficalUrl.value}/assets-download/3.0/TDengine${isLessThen3_1_1_11.value ? '' : '-enterprise'}-client-${instance.version}-Linux-x64.tar.gz`
);
const installUrlLinux = computed(() => commonDownloadUrl.value + `Linux-x64.tar.gz`);
const macDownloadPrefix = computed(() => {
  const prefix = commonDownloadUrl.value + 'macOS-';
  return isLessThen3_3_2_1.value ? prefix.replace('-enterprise', '') : prefix;
});
const installUrlMac = computed(() => macDownloadPrefix.value + `x64.pkg`);
const installUrlMacArm = computed(() => macDownloadPrefix.value + `arm64.pkg`);
const installUrlWindows = computed(() => commonDownloadUrl.value + `Windows-x64.exe`);
</script>
