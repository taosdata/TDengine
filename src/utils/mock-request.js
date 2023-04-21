import axios from "axios";
import store from "../store";
const request = axios.create({
  baseURL: process.env.VUE_APP_MOCK_URL,
  headers: {
    "Accept-Language": "q=0.8, " + store?.getters?.language,
    "Content-Type": "application/json",
  },
});
const normalCode = [200, 302];
const reLogin = ["502", "401", "432", "512"]; //重新登录的状态码
function checkStatus(code) {
  return normalCode.some(item => code.includes(item));
}
function checkRegion(code) {
  return reLogin.some(item => code.includes(item));
}
request.interceptors.request.use(config => {
  if (store.getters.token) {
    // 让每个请求都携带token
    config.headers["Authorization"] = store.getters.token;
  }
  config.headers["Accept-Language"] = "q=0.8, " + store?.getters?.language;
  return config;
});

request.interceptors.response.use(
  response => {
    const res = response.data;
    const code = res.code + "";
    if (checkStatus(code)) {
      return res.data;
    } else {
      if (checkRegion(code)) {
        // 退出时token过期，重新登录不需要再次触发重新登录
        if (response.config.url == "/auth/logout") {
          return Promise.reject(res);
        }
        // token过期, 让用户重新登录
        store.dispatch("app/logout");
      }
      return Promise.reject(res);
    }
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
export { request };
