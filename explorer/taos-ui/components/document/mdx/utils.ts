import { instance, project } from 'config';

export const dsn = computed(() => {
  let url = instance.gatewayUrl;
  if (project.isCloud && instance.token) {
    url += '?token=' + instance.token;
  }
  // TODO 企业版的 dsn 需要自己添加逻辑实现
  return url;
});

export const jdbcURL = computed(
  () =>
    'jdbc:TAOS-RS://' +
    instance.gatewayUrl.replace(/https?:\/\//, '') +
    '?useSSL=' +
    instance.gatewayUrl.startsWith('https') +
    '&token=' +
    instance.token
);
export const endpoint = computed(() => instance.gatewayUrl.replace(/https?:\/\//, ''));
export const urlKey = project.isCloud ? 'TDENGINE_CLOUD_URL' : 'TDENGINE_URL';

export const tokenKey = project.isCloud ? 'TDENGINE_CLOUD_TOKEN' : 'TDENGINE_TOKEN';

export const dsnKey = project.isCloud ? 'TDENGINE_CLOUD_DSN' : 'TDENGINE_DSN';
