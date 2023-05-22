import axios from "axios";
import { Message } from "element-ui";
import store from "../store";
import { refreshTokenExpire } from "./token";
import { ReLoginCode, SuccessCode, RequestCommonConfig } from "@/const";
const request = axios.create({
  ...RequestCommonConfig,
  baseURL: process.env.VUE_APP_BASE_URL,
  headers: {
    "Content-Type": "application/json"
  },
});

let msg = "";
let setTokenTimer = null;

request.interceptors.request.use(
  config => {
    if (store.getters.token) {
      // 让每个请求都携带token
      config.headers["Authorization"] = store.getters.token;
      if (!config.noRefreshToken) {
        //token延期
        if (setTokenTimer) {
          clearTimeout(setTokenTimer);
        }
        setTokenTimer = setTimeout(() => {
          setTokenTimer = null;
          refreshTokenExpire();
        }, 5000);
      }
    }
    config.headers["Accept-Language"] = "q=0.8, " + store?.state?.language;
    return config;
  },
  error => {
    // do something with request error
    console.log(error, 'request');
    return Promise.reject(error);
  }
);

request.interceptors.response.use(
  /**
   * If you want to get http information such as headers or status
   * Please return response => response
   */
  // Determine the request status by custom code
  response => {
    if (response.data) {
      const res = response.data;

      if (res && res.type) return Promise.resolve(res);
      if (res.code) { //针对最新的tasks接口无code情况做出的判断
        res.code += "";
      }
      if (res.code && checkRegion(res.code)) {
        // token过期, 让用户重新登录
        store.dispatch("app/logout", false);
        return Promise.reject(null);
      }
      if (res.code && checkStatus(res.code)) {
        return Promise.resolve(res.data);
      }
      if (Object.is(res.code, 0) && res.code === '0') {//针对 'show databses'
        return Promise.resolve(res)
      }
      if (res.code && res.code === '21200') {//测试用---后续删除
        return Promise.resolve(res)
      }
      return Promise.resolve(res);
    }else if(response.status==200){
      return Promise.resolve(response)
    }

  },
  (error) => {
    Message.closeAll();
    Message({
      message: error.message || "Unknown Error",
      type: "error",
      duration: 3000,
      showClose: true,
    });
    let taosx404en = 'The Taosx API is not configured. Please check the explorer configuration'
    let taosx500en = 'The Taosx API cannot be accessed. Please check the Taosx service status'
    let taosx404 = '未配置 TaosX API，请检查 Explorer 配置'
    let taosx500 = 'TaosX API 无法访问，请检查 taosx 服务状态'
    Message.closeAll()
    if (error.config.baseURL.includes('/api/x')) {
      if (error.response && error.response.status === 404) {
        Message.error(navigator.language.includes('zh') ? taosx404 : taosx404en)
      } else
        if (error.response && error.response.status === 500) {
          Message.error(navigator.language.includes('zh') ? taosx500 : taosx500en)
        } else {
          error.message && Message.error(error.message)
        }


    }

    return Promise.reject(error);
  }
);
function checkStatus(code) {
  return SuccessCode.some(item => code.includes(item));
}
function checkRegion(code) {
  return ReLoginCode.some(item => code.includes(item));
}
const requestOffical = axios.create({
  baseURL: process.env.VUE_APP_OFFICIAL_SITE,
  ...RequestCommonConfig,
  headers: {
    "Content-Type": "form-data",
  },
});
requestOffical.interceptors.response.use(
  response => {
    return response.data;
  },
  error => {
    // 网络或者服务器错误
    // Message({
    //   message: error.message,
    //   type: "error",
    //   duration: 3000,
    // });
    return Promise.reject(error);
  }
);
export { request, requestOffical };
