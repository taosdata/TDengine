<template>
  <section class="markdown-body">
    <el-steps :active="active" finish-status="success">
      <el-step :title="t('dataIn.downloadInstall')"> </el-step>
      <el-step :title="t('dataIn.generateToken')"></el-step>
      <el-step :title="t('common.configure')"></el-step>
      <el-step :title="t('dataIn.runAgent')"></el-step>
    </el-steps>
    <section v-if="active == 1" class="mt20">
      <p v-dompurify-html="t('dataIn.taosxAgent.1', downloadUrl)"></p>
      <pre v-highlight><code>taosx-agent -V</code></pre>
    </section>
    <section v-else-if="active == 2" class="mt20">
      <p v-dompurify-html="t('dataIn.taosxAgent.2')"></p>
      <el-input
        v-model="name"
        maxlength="32"
        size="small"
        :placeholder="t('dataIn.pInName')"
        class="mb10"
        @keyup.enter="next"
      ></el-input>
      <p v-if="isNameError" class="error-text">
        {{ t('dataIn.agentNameExist', [name]) }}
      </p>
    </section>
    <section v-else-if="active == 3" class="mt20">
      <p v-dompurify-html="t('dataIn.taosxAgent.3')"></p>
      <pre v-highlight><code style="text-wrap: wrap;word-wrap:break-word">{{ toml }}</code></pre>
      <p>
        <a target="_blank" :href="agentAddress">{{ t('dataIn.taosxAgent.6') }}</a>
      </p>
    </section>
    <section v-else-if="active == 4" class="step4">
      <p v-dompurify-html="t('dataIn.taosxAgent.4')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane label="Linux" name="0">
          <pre v-highlight><code>systemctl start taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane label="Windows" name="1">
          <pre v-highlight><code>sc start taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>
      <p v-dompurify-html="t('dataIn.taosxAgent.5')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane label="Linux" name="0">
          <pre v-highlight><code>systemctl status taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane label="Windows" name="1">
          <pre v-highlight><code>sc query taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>

      <el-button class="mb20" size="default" :type="checkBtnType" @click="checkAgentStatus">{{
        checkBtnText
      }}</el-button>
      <el-tag
        v-if="agentStatus == 'failed' || agentStatus == 'success'"
        class="ml20 mb20"
        size="large"
        :type="statusMap[agentStatus].type"
      >
        {{ t(statusMap[agentStatus].label) }}
      </el-tag>
      <template v-if="agentStatus == 'failed'">
        <p v-dompurify-html="t('dataIn.taosxAgent.11')"></p>
        <el-tabs v-model="tabActive">
          <el-tab-pane label="Linux" name="0">
            <pre v-highlight><code>journalctl -u taosx-agent</code></pre>
          </el-tab-pane>
          <el-tab-pane label="Windows" name="1">
            <pre v-highlight><code>C:\TDengine\log\agent.log</code></pre>
          </el-tab-pane>
        </el-tabs>
        <p v-dompurify-html="t('dataIn.taosxAgent.12')" style="margin-bottom: 16px"></p>
      </template>
    </section>
    <section class="flex-center">
      <el-button :disabled="active == 1" size="small" @click="active--">{{ t('common.prev') }}</el-button>
      <el-button size="small" type="primary" :loading="loading" :disabled="nextButton" @click="next">{{
        nextButtonText
      }}</el-button>
    </section>
  </section>
</template>

<script lang="ts" setup>
import 'github-markdown-css/github-markdown-light.css';
import { t } from 'locales';
import { trim } from 'lodash-es';
import { getDataInProps } from '../../model/useDataIn';
import { isEn, TdDocsUrl, AgentDownloadUrlForLinux, AgentDownloadUrlForWindows } from 'config';
const dataInProps = getDataInProps();

interface Props {
  agent?: Recordable;
  agentList: Recordable[];
}
const props = withDefaults(defineProps<Props>(), {
  agent: () => ({}),
  agentList: () => []
});

const emit = defineEmits(['close', 'update']);

const statusMap: Record<string, { label: string; type: 'danger' | 'success' | 'primary' | 'warning' | 'info' }> = {
  failed: {
    label: 'dataIn.taosxAgent.9',
    type: 'danger'
  },
  success: {
    label: 'dataIn.taosxAgent.8',
    type: 'success'
  }
};

const active = ref<number>(1);
const name = ref<string>('');
const tokenMap = reactive<Recordable>({});
const loading = ref<boolean>(false);
const tabActive = ref<string>('0');
const agentStatus = ref<'success' | 'failed' | 'noCheck'>();
const checkIng = ref<boolean>(false);

const agentAddress = computed(() => {
  const agenturl = dataInProps.isCloud
    ? TdDocsUrl + '/cloud/data-in/ds/install-agent'
    : isEn.value
      ? '/docs-en/tdengine-reference/components/taosx-agent/'
      : '/docs/reference/components/taosx-agent/';
  return agenturl;
});
const checkBtnText = computed(() => {
  return t('dataIn.taosxAgent.' + (checkIng.value ? '10' : '7'));
});
const checkBtnType = computed(() => {
  return checkIng.value ? 'primary' : '';
});
const downloadUrl = computed(() => {
  return {
    linuxDL: AgentDownloadUrlForLinux.value,
    windowDL: AgentDownloadUrlForWindows.value
  };
});
const agentList = computed(() => {
  return props.agentList.filter(item => item.id !== props.agent?.id);
});
const isNameError = computed(() => {
  if (name.value) {
    return agentList.value.some(item => item.name == name.value);
  } else {
    return false;
  }
});
// const token = computed(() => {
//   return tokenMap[name.value].token ?? '';
// });
// const ca = computed(() => {
//   return tokenMap[name.value].ca ?? '';
// });
// const taoxAddress = computed(() => {
//   return dataInProps.taoxAddress;
// });
const toml = computed(() => {
  const addr = dataInProps.taoxAddress;
  const { token, ca } = tokenMap[name.value];
  if (ca) {
    return `endpoint="${addr}"\ntoken="${token}"\nca="""\n${trim(ca)}\n"""\n`;
  } else {
    return `endpoint="${addr}"\ntoken="${token}"\n`;
  }
});
const nextButton = computed(() => {
  if (loading.value) return true;
  if (active.value == 2) {
    if (!name.value || isNameError.value) {
      return true;
    }
  }
  return false;
});
const nextButtonText = computed(() => {
  return active.value == 4 ? t('dataIn.finish') : t('common.next');
});

onMounted(() => {
  if (props.agent?.id) {
    nextTick(() => {
      name.value = props.agent.name;
    });
    active.value = 2;
  }
});

function checkAgentStatus() {
  if (checkIng.value) return;
  checkIng.value = true;
  emit('update');
  const status = props.agentList.find(item => item.name == name.value)?.status;
  agentStatus.value = ['idle', 'busy', 'online', 'connected'].includes(status) ? 'success' : 'failed';
  checkIng.value = false;
}
async function submit() {
  if (loading.value) return;
  loading.value = true;
  const fn = props.agent?.id ? dataInProps.agent.api.editAgent : dataInProps.agent.api.addNewAgent;

  const { token, id, ca } = await fn(name.value, props.agent?.id);
  tokenMap[name.value] = { token, ca };
  active.value++;
  emit('update', name.value);
  Object.assign(
    props.agent,
    props.agentList.find(item => item.id == id)
  );

  loading.value = false;
}
function next() {
  if (active.value == 4) {
    agentStatus.value = 'noCheck';
    emit('close');
    console.log('output:jieshu');
  }
  if (active.value == 2) {
    if (nextButton.value) return;
    submit();
  } else {
    active.value++;
  }
}
</script>

<style scoped lang="scss">
.markdown-body {
  padding: 0;
  word-break: break-word;

  p {
    line-height: 24px;
  }

  .step4 p {
    margin-bottom: 0;
  }

  .mb10 {
    margin-bottom: 10px;
  }

  .mb20 {
    margin-bottom: 20px;
  }

  .ml20 {
    margin-left: 20px;
  }

  .mt20,
  .step4 {
    margin-top: 20px;
  }
}
</style>
