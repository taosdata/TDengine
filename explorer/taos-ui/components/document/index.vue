<template>
  <div class="view">
    <section v-if="isPartyCategory" class="data-collector-title">
      <span class="breadcrumb-link" @click="goBackToParty">{{ t('dataIn.datacollection') }}</span>
      <span class="breadcrumb-separator"> &gt; </span>
      <span class="breadcrumb-current">{{ currentConfig.name }}</span>
    </section>
    <section class="view-header">
      <section class="left">
        <Icon class="image-contains" :name="iconName" />
      </section>
      <section class="right">
        <el-steps align-center :active="activeTab" process-status="finish" finish-status="success">
          <el-step
            v-for="(item, index) in steps"
            :key="item.title"
            :title="item.title"
            @click="handleClickStep(index)"
          ></el-step>
        </el-steps>
      </section>
    </section>
    <section id="view-content" class="markdown-body">
      <slot name="header"></slot>
      <component :is="component" :topic="props.topic"></component>
      <slot name="footer"></slot>
    </section>
  </div>
</template>

<script lang="ts" setup>
import * as docsConfigMap from './index';
import { debounce } from 'lodash-es';
import 'github-markdown-css/github-markdown-light.css';
import { i18n } from 'locales';
import { t } from 'locales';

const props = defineProps<{
  lang: string;
  category: string;
  topic?: string;
}>();
const topList: { start: number; end: number }[] = [];
const activeTab = ref(0);
let element: HTMLElement | null = null;

const currentConfig = computed(() => {
  const lang = window.decodeURIComponent(props.lang);
  return (
    (docsConfigMap as Recordable)[props.category].find((item: Recordable) => {
      return item.name == lang;
    }) || {}
  );
});
const component = computed(() => {
  return typeof currentConfig.value.docs === 'string' || !currentConfig.value.docs ? '' : currentConfig.value.docs;
});
const steps = computed<Recordable[]>(() => {
  return currentConfig.value.steps || [];
});
const domList = computed<HTMLElement[]>(
  () => steps.value.map(item => document.getElementById(item.dom)).filter(item => item) as HTMLElement[]
);
const iconName = computed(() => currentConfig.value.icon || currentConfig.value.name);

const isPartyCategory = computed(() => props.category === 'party');

function goBackToParty() {
  window.history.back();
}

watch(
  () => (i18n.global.locale as WritableComputedRef<string>).value,
  () => {
    window.location.reload();
  }
);

onMounted(() => {
  // 在这里保存元素
  element = document.querySelector('.main-content');
  if (!element) return;
  getOffsetTop();
  handleScroll();

  // 处理a标签，添加属性target="_blank"
  document.querySelectorAll('#view-content a').forEach(item => {
    item.setAttribute('target', '_blank');
  });
});

function handleScroll() {
  const handleFn = debounce(() => {
    const firstStepDomOffsetTop = domList.value[0]?.offsetTop ?? 0;
    const top = element?.scrollTop ?? 0;
    const currentTop = Math.ceil(top + firstStepDomOffsetTop);
    const tabIndex = topList.findLastIndex(item => {
      return (item.start <= top && item.end >= currentTop) || (item.start <= currentTop && item.end >= currentTop);
    });
    activeTab.value = tabIndex == -1 ? domList.value.length : tabIndex;
  }, 200);
  element?.addEventListener('scroll', handleFn);
  onBeforeUnmount(() => {
    element?.removeEventListener('scroll', handleFn);
  });
}
function handleClickStep(index: number) {
  const dom = domList.value[index];
  if (dom) {
    scrollTo(dom);
  }
}
function getOffsetTop() {
  domList.value.forEach((dom, index) => {
    const data = {
      start: dom.offsetTop,
      end: 0
    };
    if (index > 0 && index < steps.value.length - 1) {
      topList[index - 1].end = data.start;
    } else if (index != 0) {
      topList[index - 1].end = data.start;
      data.end = element?.scrollHeight ?? 0;
    }
    topList.push(data);
  });
}

function scrollTo(dom: HTMLElement) {
  if (!element) return;
  element.scrollTo({
    top: dom.offsetTop + dom.offsetHeight + 20,
    behavior: 'smooth'
  });
}
</script>
<style lang="scss" scoped>
.view {
  position: relative;
  padding: 20px 30px;
  background-color: #fff;

  &:deep(.markdown-body pre) {
    position: relative;
  }

  &:deep(.el-empty) {
    padding: 0;
  }
}

.view-header {
  position: sticky;
  top: 10px;
  right: 0;
  z-index: 6;
  display: flex;
  padding: 10px 0;
  background-color: #fff;
}

.data-collector-title {
  display: flex;
  align-items: center;
  height: 44px;
  padding: 12px 16px;
  margin-bottom: 8px;
  font-size: 16px;
  color: #333;
  background-color: #ecf8ff;
  border-left: 5px solid #50bfff;
  border-radius: 4px;

  .breadcrumb-link {
    cursor: pointer;

    &:hover {
      color: #409eff;
    }
  }

  .breadcrumb-separator {
    margin: 0 4px;
    color: #606266;
  }

  .breadcrumb-current {
    font-weight: 500;
  }
}

#view-content {
  margin-top: 20px;
}

.left {
  display: inline-block;
  flex-shrink: 0;
  width: auto;
  height: 81px;

  .image-contains {
    width: 100%;
    max-width: 200px;
    height: 100%;
    object-fit: contain;
  }
}

.right {
  flex: 1;
  margin-left: -40px;
  overflow: hidden;

  $step-icon-size: 48px;

  &:deep(.el-step__icon) {
    width: $step-icon-size;
    height: $step-icon-size;
    font-size: 18px;
    cursor: pointer;
  }

  &:deep(.el-step__icon.is-icon) {
    width: 48px;
    border: 2px solid;
    border-color: inherit;
    border-radius: 50%;
  }
}
</style>
