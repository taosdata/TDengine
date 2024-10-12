import axios from "axios";
import { Message } from "element-ui";
import { getToken } from "@/utils/token";
import router from "@/router/index.js";
import store from "../store";
import { refreshTokenExpire } from "./token";
import { ReLoginCode, SuccessCode, RequestCommonConfig } from "@/const";
import Vue from 'vue';



const request = axios.create({
  ...RequestCommonConfig,
  baseURL: process.env.VUE_APP_BASE_URL,
  headers: {
    "Content-Type": "application/json",
  },
});

let msg = "";
let setTokenTimer = null;
function blobToJson(blob) {
  const reader = new FileReader();
  reader.readAsText(blob);
  reader.onload = () => {
    const text = reader.result;
    const json = JSON.parse(text);
    Message.error({
      message: json?.Message,
      duration: 20000,
      showClose: true,
    })
  };
}
request.interceptors.request.use(
  (config) => {
    const hasToken = getToken();
    if (config.headers.noAuth !== true) {
      if (hasToken) {
        // 让每个请求都携带token
        config.headers["Authorization"] = hasToken;
        if (!config.noRefreshToken) {
          refreshTokenExpire();
        }
      } else {
        config.cancelToken = axios.CancelToken.source;
        Vue.prototype.$message = () => {};
        router.push({
          path: "/login",
        });
      }
    } 
    config.headers["Accept-Language"] = "q=0.8, " + store?.state?.language;
    return config;
  },
  (error) => {
    // do something with request error
    return Promise.reject(error);
  }
);

request.interceptors.response.use(
  /**
   * If you want to get http information such as headers or status
   * Please return response => response
   */
  // Determine the request status by custom code
  (response) => {
    if (response.data) {
      const res = response.data;

      if (res && res.type) return Promise.resolve(res);
      if (res.code) {
        //针对最新的tasks接口无code情况做出的判断
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
      if (Object.is(res.code, 0) && res.code === "0") {
        //针对 'show databses'
        return Promise.resolve(res);
      }
      if (res.code && res.code === "21200") {
        //测试用---后续删除
        return Promise.resolve(res);
      }
      return Promise.resolve(res);
    } else if (response.status == 200) {
      return Promise.resolve(response);
    }
  },
  (error) => {
    if(error?.response?.data?.constructor===Blob){
      blobToJson(error.response.data)
      Message.closeAll();
      return
    }

    if (error?.response?.status === 400) {
      Message.closeAll();
      Message.error({
        message: error.response.data,
        duration: 20000,
        showClose: true,
      })
      return Promise.reject(error.response);
    }
   
    const hasToken = getToken();
    if (hasToken) {
      Message.closeAll();
      if (error.response?.data?.code) {
        return Promise.resolve(error.response.data);
      }
      let msg =
        error.response?.data?.message ||
        error.response?.data?.desc ||
        error.message ||
        "Unexpected error";
      Message.error({
        message: msg,
        duration: 20000,
        showClose: true,
      })
      let taosx404en =
        "The TaosX API is not configured. Please check the explorer configuration";
      let taosx500en =
        "The TaosX API cannot be accessed. Please check the taosx service status";
      let taosx404 = "未配置 TaosX API，请检查 Explorer 配置";
      let taosx500 = "TaosX API 无法访问，请检查taosx服务状态";
      // Message.closeAll()
      let isoem = false;
      if (
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine"
      ) {
        isoem = true;
      }
      if (error.config.baseURL.includes("/api/x")) {
        Message.closeAll()
        if (error.response && error.response.status === 404) {
          Message.error(
            localStorage.getItem('local_language')?.includes("zh")
              ? isoem
                ? taosx404.replace("TaosX", "").replace('taosx','')
                : taosx404
              : isoem
              ? taosx404en.replace("TaosX", "").replace('taosx','')
              : taosx404en
          );
        } else if (error.response && error.response.status === 500) {
          Message.error(
            localStorage.getItem('local_language')?.includes("zh")
              ? isoem
                ? taosx500.replace("TaosX", "").replace('taosx','')
                : taosx500
              : isoem
              ? taosx500en.replace("TaosX", "").replace('taosx','')
              : taosx500en
          );
        } else {
          error.message && Message.error(error.message);
        }
      }
      error.message = msg;

      return Promise.reject(error);
    } else {
      Message.closeAll();
    }
  }
);
function checkStatus(code) {
  return SuccessCode.some((item) => code.includes(item));
}
function checkRegion(code) {
  return ReLoginCode.some((item) => code.includes(item));
}
const requestOffical = axios.create({
  baseURL: process.env.VUE_APP_OFFICIAL_SITE,
  ...RequestCommonConfig,
  headers: {
    "Content-Type": "form-data",
  },
});
requestOffical.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error) => {
    return Promise.reject(error);
  }
);
export { request, requestOffical };
