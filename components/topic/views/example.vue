<template>
  <div class="topic-sample">
    <el-tabs v-model="activeLang" type="card" class="topic-tab" @tab-click="changeLang">
      <el-tab-pane v-for="lang in langList" :key="lang" :class="'topic-' + lang" :name="lang" :label="lang">
        <docs :category="'topic'" :lang="lang" :topic="topicTitle">
          <template #header>
            <p>{{ t('topic.topdesc', [organization.orgName, instance.alias, topicTitle]) }}</p>
          </template>
          <template #footer>
            <p>
              {{ t('topic.enddesc') }}
              <a :href="TdDocsUrl + `/cloud/data-subscription/`">{{ TdDocsUrl + `/cloud/data-subscription/` }}</a>
              {{ t('topic.enddesc1') }}
            </p>
          </template>
        </docs>
      </el-tab-pane>
    </el-tabs>
    <div class="topic-example-select flex-start">
      <label class="topic-title">{{ t('topic.topic') }}</label>
      <el-select v-model="currentTopic" class="topic-select-content" placeholder="Topic Select">
        <el-option
          v-for="item in topicList"
          :key="item.topicId"
          :label="item.topicName"
          :value="item.topicId"
        ></el-option>
      </el-select>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { TdDocsUrl, instance, organization } from 'config';
import { getTopicList } from '../api';
import Docs from '../../document/index.vue';
import { t } from 'locales';
import { useRoute } from 'hooks/useCurrentRouter';

const langList = ['Go', 'Rust', 'Python', 'Java'];
type langType = (typeof langList)[number];
const currentTopic = ref('');
const activeLang = ref<langType>(langList[0]);
const topicList = ref<Recordable[]>([]);
let mainEl: HTMLElement | null = null;
const route = useRoute();

const topicTitle = computed(() => {
  const foundItem = topicList.value.find(item => {
    return item.topicId === currentTopic.value;
  });
  return foundItem?.topicName ?? '';
});

init();

onMounted(() => {
  mainEl = document.querySelector('.content');
});

async function init() {
  await getTopics();
  if (route.query?.topicId) {
    currentTopic.value = route.query.topicId;
  }
}
function getTopics() {
  getTopicList().then(data => {
    topicList.value = data;
    if (!currentTopic.value && data && data.length > 0) {
      currentTopic.value = data[0].topicId;
    }
  });
}
function changeLang() {
  if (!mainEl) return;
  mainEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
}
</script>

<style scoped lang="scss">
.topic-sample {
  position: relative;
  height: 100%;

  .topic-tab {
    height: 100%;

    :deep(.el-tabs__header) {
      position: sticky;
      top: 0;
      z-index: 1000;
      background-color: white;
    }

    :deep(.el-tabs__content) {
      overflow: unset;
    }

    :deep(.tab-python),
    :deep(.doc-config-tab) {
      .el-tabs__header {
        z-index: unset;
      }
    }

    :deep(#tab-python.is-active),
    :deep(#tab-go.is-active),
    :deep(#tab-rust.is-active) {
      font-weight: 600;
      color: white;
      background-color: #4259ce;
    }

    :deep(.view-header) {
      top: 40px;
      width: 85%;
    }
  }

  .topic-title {
    margin-right: 10px;
  }

  .topic-example-select {
    position: fixed;
    top: 205px;
    right: 55px;
    z-index: 1000;
    background-color: white;

    .topic-select-content {
      :deep(.el-input__inner) {
        height: 35px;
      }
    }
  }
}
</style>
