import { computed, reactive, provide, type WritableComputedRef } from 'vue';
import { i18n } from 'locales';
import { ZINDEX_INJECTION_KEY, useZIndex } from 'element-plus';
import { compareVersion } from 'utils/tdengine';
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

export const ClientUrlForWindows = computed(() => DownloadUrl.value + '/client/taos-tools-for-windows.zip');

const isLessThen3_1_1_11 = computed(() => compareVersion(instance.version, '<3.1.1.11'));
const isLessThen3_3_2_1 = computed(() => compareVersion(instance.version, '<3.3.2.1'));
const isLessThen3_3_7_0 = computed(() => compareVersion(instance.version, '<3.3.7.0'));

export const installClientPackageName = computed(() =>
  isLessThen3_3_7_0.value
    ? `TDengine${isLessThen3_1_1_11.value ? '' : '-enterprise'}-client-${instance.version}-Linux-x64.tar.gz`
    : `tdengine-tsdb-enterprise-client-${instance.version}-linux-x64.tar.gz`
);
const commonDownloadUrl = computed(() =>
  isLessThen3_3_7_0.value
    ? `${OfficialUrl.value}/assets-download/3.0/TDengine${isLessThen3_1_1_11.value ? '' : '-enterprise'}-client-${instance.version}-Linux-x64.tar.gz`
    : DownloadUrl.value +
      `/tdengine-tsdb-enterprise/${instance.version}/tdengine-tsdb-enterprise-client-${instance.version}-`
);
const macDownloadPrefix = computed(() => {
  const prefix = commonDownloadUrl.value + (isLessThen3_3_7_0.value ? 'macOS-' : 'macos-');
  return isLessThen3_3_2_1.value ? prefix.replace('-enterprise', '') : prefix;
});
export const installUrlMac = computed(() => macDownloadPrefix.value + `x64.pkg`);
export const installUrlMacArm = computed(() => macDownloadPrefix.value + `arm64.pkg`);
export const installUrlWindows = computed(() => {
  console.log('installUrlWindows', commonDownloadUrl.value, isLessThen3_3_7_0.value);
  return commonDownloadUrl.value + `${isLessThen3_3_7_0.value ? 'W' : 'w'}indows-x64.exe`;
});
export const installUrlLinux = computed(
  () => commonDownloadUrl.value + `${isLessThen3_3_7_0.value ? 'L' : 'l'}inux-x64.tar.gz`
);

export const AgentDownloadUrlForLinux = computed(() =>
  isLessThen3_3_7_0.value
    ? `${OfficialUrl.value}/assets-download/3.0/taosx-agent-${instance.version}-linux-x64.tar.gz`
    : DownloadUrl.value +
      '/tdengine-taosx-agent-enterprise/' +
      instance.version +
      `/tdengine-taosx-agent-${instance.version}-linux-x64.tar.gz`
);
export const AgentDownloadUrlForWindows = computed(() =>
  isLessThen3_3_7_0.value
    ? `${OfficialUrl.value}/assets-download/3.0/taosx-agent-${instance.version}-windows-x64-installer.exe`
    : DownloadUrl.value +
      '/tdengine-taosx-agent-enterprise/' +
      instance.version +
      `/tdengine-taosx-agent-${instance.version}-windows-x64.exe`
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
