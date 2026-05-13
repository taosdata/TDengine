import { HttpRequest, RequestConfig } from 'taos-ui/utils/axios';
import { ElMessage } from 'element-plus';
import { getToken } from '@/utils/token';
import router from '@/router/index';
import store from '../store';
import { ReLoginCode, SuccessCode } from '@/const';
import { t } from '@/lang';
import { $IS_OEM } from './init';

const errorMsgDuration = 20000;

const httpRequest = new HttpRequest({
  // timeout: 20000,
  baseURL: import.meta.env.VITE_APP_BASE_URL,
  withCredentials: true
});

httpRequest.setRequestInterceptor(
  async (config: RequestConfig) => {
    if (config.autoLogoutOn401 === undefined) config.autoLogoutOn401 = true;
    // Normalize headers object and guard access to header fields to avoid TS errors
    const headers = ((config as any).headers = (config as any).headers || {});

    if (headers.noAuth !== true) {
      // Always send credentials (session cookie) for authentication
      try {
        (config as any).withCredentials = true;
      } catch (e) {
        // ignore if config type doesn't support withCredentials
      }
    }
    return config;
  },
  error => {
    return Promise.reject(error);
  }
);

httpRequest.setResponseInterceptor(
  async response => {
    if (response.data) {
      const res = response.data;

      if (res && res.type) return Promise.resolve(res);
      if (res.code) {
        //针对最新的tasks接口无code情况做出的判断
        res.code += '';
      }
      if (res.code && checkRegion(res.code)) {
        // token过期, 让用户重新登录
        store.dispatch('app/logout', false);
        return Promise.reject(null);
      }
      if (res.code && checkStatus(res.code)) {
        return Promise.resolve(res.data);
      }
      if (Object.is(res.code, 0) && res.code === '0') {
        //针对 'show databases'
        return Promise.resolve(res);
      }
      if (res.code && res.code === '21200') {
        //测试用---后续删除
        return Promise.resolve(res);
      }
      return Promise.resolve(res);
    } else if (response.status == 200) {
      return Promise.resolve(response);
    }
  },
  async error => {
    console.log('this', error);
    console.log('api response error:', error?.response);
    if (error?.response?.status === 401 && error.config.autoLogoutOn401) {
      store.dispatch('app/logout', false);
      router.push({
        path: '/login'
      });
      return Promise.reject(null);
    }

    // handle Blob error responses
    if (error?.response?.data?.constructor === Blob) {
      blobToJson(error.response.data);
      ElMessage.closeAll();
      return;
    }

    try {
      const respData = error?.response?.data;
      if (respData) {
        // parse JSON if it's a string
        let parsed = respData;
        if (typeof respData === 'string') {
          try {
            parsed = JSON.parse(respData);
          } catch (_) {
            // keep original respData if parsing fails
          }
        }
        if (parsed && typeof parsed === 'object' && (parsed.code !== undefined || parsed.desc !== undefined)) {
          return Promise.resolve(parsed);
        }
      }
    } catch (_) {
      // ignore parsing errors and continue
    }

    if (error?.response?.status === 400) {
      ElMessage.closeAll();
      ElMessage.error({
        message: error.response.data?.desc || 'Bad Request',
        duration: errorMsgDuration,
        showClose: true
      });
      return Promise.reject(error.response);
    }

    const hasToken = getToken();
    if (hasToken) {
      ElMessage.closeAll();
      if (error.response?.data?.code) {
        return Promise.resolve(error.response.data);
      }
      const msg = error.response?.data?.message || error.response?.data?.desc || error.message || 'Unexpected error';
      ElMessage.error({
        message: msg,
        duration: errorMsgDuration,
        showClose: true
      });

      if (error.config.baseURL.includes('/api/x')) {
        ElMessage.closeAll();
        if (error.response && error.response.status === 404) {
          ElMessage.error(
            $IS_OEM ? t('login.taosx404').replace('TaosX', '').replace('taosx', '') : t('login.taosx404')
          );
        } else if (error.response && error.response.status === 500) {
          ElMessage.error(
            $IS_OEM ? t('login.taosx500').replace('TaosX', '').replace('taosx', '') : t('login.taosx500')
          );
        } else {
          error.message && ElMessage.error(error.message);
        }
      }
      error.message = msg;

      return Promise.reject(error);
    } else {
      ElMessage.closeAll();
    }

    return Promise.reject(error || {});
  }
);
const request = httpRequest.request.bind(httpRequest);

function checkStatus(code: string) {
  const c = String(code || '');
  return SuccessCode.some(item => c.includes(item));
}
function checkRegion(code: string) {
  const c = String(code || '');
  return ReLoginCode.some(item => c.includes(item));
}
function blobToJson(blob: Blob | any) {
  const reader = new FileReader();
  reader.readAsText(blob);
  reader.onload = () => {
    const text = reader.result;
    if (typeof text === 'string') {
      try {
        const json = JSON.parse(text);
        ElMessage.error({
          message: json?.ElMessage,
          duration: errorMsgDuration,
          showClose: true
        });
      } catch (e) {
        ElMessage.error({
          message: 'Failed to parse error response',
          duration: errorMsgDuration,
          showClose: true
        });
      }
    } else {
      ElMessage.error({
        message: 'Unexpected error response',
        duration: errorMsgDuration,
        showClose: true
      });
    }
  };
}

export { request };
