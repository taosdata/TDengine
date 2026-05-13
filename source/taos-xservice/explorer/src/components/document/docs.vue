<template>
  <div class="view">
    <section class="view-header">
      <section class="left">
        <Icon class="image-contains" :name="getImg(configData.name, configData.icon)"></Icon>
      </section>
      <section class="right">
        <el-steps align-center :active="activeTab" finish-status="success">
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
      <component
        :is="component"
        :url="url"
        :token="token"
        :user="username"
        :password="decryptPwd"
        :language="language"
        :dsn="dsn"
        :restapi="restapi"
        :topic="topic"
        :category="category"
        :version="TDengineVersion"
        :install-url-linux="installUrlLinux"
        :install-url-mac="installUrlMac"
        :install-url-mac-arm="installUrlMacArm"
        :install-url-windows="installUrlWindows"
      ></component>
    </section>
  </div>
</template>

<script setup lang="ts">
import * as config from '@/components/document/index';
import { decrypt, getLocalLang } from '@/utils';
import { debounce } from 'lodash-es';
import { installUrlLinux, installUrlMac, installUrlMacArm, installUrlWindows } from 'taos-ui/config';
import 'github-markdown-css/github-markdown-light.css';

interface DocsProps {
  lang: string;
  category: string;
  topic?: string;
}
const props = withDefaults(defineProps<DocsProps>(), {
  lang: '',
  category: '',
  topic: ''
});

const activeTab = ref(1);
const domList = ref([]);
const element = ref<HTMLElement | null>(null);

const configData: Record<string, any> = computed(() => {
  const lang = window.decodeURIComponent(props.lang);
  return (
    config[props.category](language.value).find(item => {
      return (item.path ?? item.name) == lang;
    }) || {}
  );
});
const steps = computed(() => {
  return configData.value.steps || [];
});
const url = computed(() => {
  return localStorage.getItem('base_url') ?? '';
});
const token = computed(() => {
  return localStorage.getItem('TDengine-Token') ? localStorage.getItem('TDengine-Token') : '';
});
const username = computed(() => {
  return localStorage.getItem('username') ? localStorage.getItem('username') : '';
});
const decryptPwd = computed(() => {
  return decrypt(localStorage.getItem('pwd')) || '';
});

const language = computed(() => {
  return getLocalLang();
});
const component = computed(() => {
  return typeof configData.value.docs?.[language.value] == 'string' || !configData.value.docs?.[language.value]
    ? ''
    : configData.value.docs?.[language.value];
});
const TDengineVersion = computed(() => {
  return localStorage.getItem('td_version') ?? '';
});

const dsn = computed(() => {
  return `taos://${username.value}:${decryptPwd.value}@${url.value.replace(/^[a-z]+:\/\//, '')}`;
});
const restapi = computed(() => {
  return language.value.includes('en') ? 'reference' : 'connector';
});

function handleClickStep(index: number) {
  const dom = domList.value[index];
  if (dom) {
    scrollTo(dom);
  }
  activeTab.value = index + 1;
}
function getOffsetTop() {
  const topList: any[] = [];
  domList.value.forEach((dom: HTMLElement, index) => {
    topList.push({
      start: dom?.offsetTop
    });
    if (index > 0 && index < steps.value.length - 1) {
      topList[index - 1].end = topList[index].start;
    }
    if (index == steps.value.length - 1) {
      topList[index - 1].end = topList[index].start;
      topList[index].end = element?.value?.scrollHeight;
    }
  });
  return topList;
}
function getImg(name: string, icon: string) {
  return icon || name;
}
function scrollTo(dom: HTMLElement) {
  element?.value?.scrollTo({
    top: dom.offsetTop,
    behavior: 'smooth'
  });
}
onMounted(() => {
  // 在这里保存元素
  nextTick(() => {
    domList.value = steps.value.map(item => document.getElementById(item.dom));
  });
  const fn = debounce(e => {
    const top = e.target.scrollTop;
    const currentTop = Math.floor(top + elementHeight);

    activeTab.value =
      getOffsetTop().findLastIndex(item => {
        return (item.start <= top && item.end >= currentTop) || (item.start <= currentTop && item.end >= currentTop);
      }) + 1;
    activeTab.value = activeTab.value || 1;
  }, 100);
  element.value = document.querySelector('.main-content');
  const currentElement = element.value;

  const elementHeight = parseFloat(
    document?.defaultView?.getComputedStyle(currentElement)?.height || currentElement.offsetHeight
  );
  currentElement?.addEventListener('scroll', fn);
  onBeforeUnmount(() => {
    element?.value?.removeEventListener('scroll', fn);
  });
  // 处理a标签，添加属性target="_blank"
  const aList = document.querySelectorAll('#view-content a');
  aList.forEach(item => {
    item.setAttribute('target', '_blank');
  });
});
</script>
<style lang="scss" scoped>
.view {
  position: relative;
  background-color: #fff;

  @include content-padding;
}

.view-header {
  position: sticky;
  top: -20px;
  z-index: 6;
  display: flex;
  padding-top: 10px;
  padding-bottom: 10px;
  background-color: #fff;
}

.view:deep(.markdown-body .highlight pre),
.view:deep(.markdown-body pre) {
  position: relative;
}

.view:deep(.token-select) {
  display: flex;
  align-items: center;
  margin-bottom: 20px;

  .label {
    margin-right: 20px;
    font-family: 'Amazon Ember', 'Helvetica Neue', Roboto, Arial, sans-serif;
    font-size: 18px;
  }
}

.token-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
  font-size: 14px;
  font-weight: 600;
}

.view :deep(.el-empty) {
  padding: 0;
}

#view-content {
  margin-top: 10px;
}

.left {
  display: inline-block;
  flex-shrink: 0;
  width: auto;
  width: 81px;
  height: 81px;

  .image-contains {
    width: 100%;
    height: 100%;
    object-fit: contain;
  }
}

.right {
  flex: 1;

  // margin-left: -40px;
  overflow: hidden;
}
</style>
