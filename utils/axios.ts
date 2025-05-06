import axios, {
  type AxiosInstance,
  type ResponseType as AxioResponseType,
  type CreateAxiosDefaults,
  type AxiosRequestConfig
} from 'axios';
import qs from 'qs';
import { ElLoading } from 'element-plus';
import { nextTick } from 'vue';

export type optionsType = {
  loading?: boolean;
  responseType?: AxioResponseType;
  timeout?: number;
  mfa?: string;
  language?: string;
  [key: string]: any;
};
export type RequestConfig = AxiosRequestConfig<any> & optionsType;
export type HttpRequestConfig = CreateAxiosDefaults & { loading?: boolean };
export class HttpRequest {
  instance: AxiosInstance = axios.create({
    paramsSerializer: (params: Recordable) => {
      return qs.stringify(params, { arrayFormat: 'repeat' });
    }
  });

  private baseConfig: HttpRequestConfig = {
    loading: false,
    headers: {
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept-Language': 'q=0.8, ' + 'en'
    },
    timeout: 60000
  };
  private requestCount = 0;
  private loading: any = null;
  private loadingConfig: Recordable = {
    lock: true,
    text: 'loading...',
    background: 'rgba(0,0,0,0.7)'
  };
  private loadingTimer: any = null;
  constructor(options: HttpRequestConfig) {
    this.baseConfig = {
      ...this.baseConfig,
      ...options
    };
  }
  setBaseConfig(config: HttpRequestConfig) {
    this.baseConfig = Object.assign(this.baseConfig, config);
  }

  setRequestInterceptor(onFullfilled: (config: RequestConfig) => any, onRejected: (error: any) => any) {
    this.instance.interceptors.request.use(onFullfilled, onRejected);
  }
  setResponseInterceptor(onFullfilled: (response: any) => any, onRejected: (error: any) => any) {
    this.instance.interceptors.response.use(onFullfilled, onRejected);
  }
  setLoadingConfig(config: Recordable) {
    this.loadingConfig = config;
  }
  openLoading() {
    this.requestCount++;
    if (this.loadingTimer) return;
    this.loadingTimer = setTimeout(() => {
      this.loading = ElLoading.service(this.loadingConfig);
      this.loadingTimer = null;
    }, 800);
  }

  closeLoading() {
    this.requestCount--;
    if (!this.requestCount) {
      nextTick(() => {
        this.loading?.close();
        this.loading = null;
      });
      if (this.loadingTimer) {
        clearTimeout(this.loadingTimer);
        this.loadingTimer = null;
      }
    }
  }

  request<T = any>(options: RequestConfig): Promise<T> {
    const params = Object.assign({}, this.baseConfig, options);
    const isLoading = params.loading;
    isLoading && this.openLoading();
    return this.instance.request<any, T>(params).finally(() => {
      isLoading && this.closeLoading();
    });
  }
}
export const isCancel = axios.isCancel;
