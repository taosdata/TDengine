import { computed, reactive, provide, type WritableComputedRef } from 'vue';
import { i18n } from 'locales';
import { ZINDEX_INJECTION_KEY, useZIndex } from 'element-plus';
export { setLocale } from '../locales';
export { setTimezone } from '../utils/date';
export { setExecuteSqlFn, setGetDbListFn } from 'components/api';
export { setTopicApi } from 'components/topic/api';

export const isEn = computed(() => (i18n.global.locale as WritableComputedRef<string>).value == 'en');
export const OfficialUrl = computed(() => (isEn.value ? 'https://tdengine.com' : 'https://taosdata.com'));
export const TdDocsUrl = computed(() => (isEn.value ? 'https://docs.taosdata.com' : 'https://docs.tdengine.com'));
export function setElementPlusZIndexDefaultValue(zIndexOverrides = 4000) {
  provide(ZINDEX_INJECTION_KEY, {
    current: zIndexOverrides
  });
  useZIndex().nextZIndex();
}
export const DownloadUrl = computed(() =>
  isEn.value ? 'https://downloads.taosdata.com' : 'https://downloads.tdengine.com'
);
export const organization = reactive({
  orgName: '',
  orgId: ''
});

export const instance = reactive({
  version: '',
  token: '',
  gatewayUrl: '',
  id: '',
  ha: false,
  alias: '',
  user: '',
  password: '',
  tdClusterId: ''
});

export const user = reactive({
  token: '',
  id: ''
});
export const project = {
  isCloud: false,
  isAliyun: false
};

export function setOrganizationData(data: Partial<typeof organization>) {
  Object.assign(organization, data);
}
export function setInstanceData(data: Partial<typeof instance>) {
  Object.assign(instance, data);
}

export function setUserData(data: Partial<typeof user>) {
  Object.assign(user, data);
}

export function setProjectData(data: Partial<typeof project>) {
  Object.assign(project, data);
}

export const docs = reactive({
  urlPrefix: 'https://docs.tdengine.com/cloud'
});

export function setDocsData(data: Partial<typeof docs>) {
  Object.assign(docs, data);
}
