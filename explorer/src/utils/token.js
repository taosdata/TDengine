import Cookies from "js-cookie";
import { TokenKey, AppIDKey, TokenExpire, RedirectKey } from "@/const";
import { jsonToObj } from "./index";
import { isIPUrl } from "./validate";
import router from "@/router/index.js";
const Domain = isIPUrl(document.domain) ? document.domain : document.domain.split(".").slice(-2).join(".");
const currentDomain = document.domain;
export function getToken() {
  return Cookies.get(TokenKey);
}

export function setToken(token) {
  setLoginSign();
  return Cookies.set(TokenKey, token, {
    domain: Domain,
    expires: TokenExpire,
    path: "/",
  });
}

export function refreshTokenExpire() {
  let token = getToken();
  if (token) {
    setToken(token);
  } else {
    removeToken();
    router.push({
      path:'/login'
    })
  }
}
export function removeToken() {
  return Cookies.remove(TokenKey, {
    domain: Domain,
  });
}

/**
 * 每个域名保存不同的id
 */
export function getAppID() {
  return jsonToObj(Cookies.get(AppIDKey))[currentDomain] || "";
}

export function setAppId(appID, domain = currentDomain) {
  // 先获取id列表
  let idMap = jsonToObj(Cookies.get(AppIDKey));
  idMap[domain] = appID;
  return Cookies.set(AppIDKey, JSON.stringify(idMap), {
    domain: Domain,
  });
}

export function removeAppID() {
  let idMap = jsonToObj(Cookies.get(AppIDKey));
  delete idMap[currentDomain];
  return Cookies.set(AppIDKey, JSON.stringify(idMap), {
    domain: Domain,
  });
}

export function setRedirect(url) {
  return Cookies.set(RedirectKey, url, {
    domain: Domain,
  });
}

const loginSignKey = 'login_TDC'
/** 设置登陆标志位 */
export function setLoginSign() {
  Cookies.set(loginSignKey, 'true')
}

/** 判断是否存在登陆标志位 */
export function isLogin() {
  return !!Cookies.get(loginSignKey)
}

export function clearLoginStateWhenReopen() {
  if (!isLogin()) {
    removeToken()
  }
}