import Cookies from 'js-cookie';
import { TokenKey, AppIDKey, TokenExpire, RedirectKey, OAuthTokenKey } from '@/const.ts';
import { jsonToObj } from './index';
import { isIPUrl } from './validate';
import router from '@/router/index';
const Domain = isIPUrl(document.domain) ? document.domain : document.domain.split('.').slice(-2).join('.');
const currentDomain = document.domain;
export function getToken() {
  // Only return the cookie-based token. OAuth tokens should no longer be read
  // from localStorage; server-side sessions (httpOnly cookies) are preferred.
  return Cookies.get(TokenKey);
}
export function getOAuthToken() {
  return localStorage.getItem(OAuthTokenKey);
}
export function setOAuthToken(token: string) {
  return localStorage.setItem(OAuthTokenKey, token);
}
export function removeOAuthToken() {
  return localStorage.removeItem(OAuthTokenKey);
}
export function getOAuthBearerToken() {
  const token = getOAuthToken();
  return token ? `Bearer ${token}` : null;
}

export function setToken(token: string) {
  setLoginSign();
  return Cookies.set(TokenKey, token, {
    domain: Domain,
    expires: TokenExpire,
    path: '/'
  });
}

export function refreshTokenExpire() {
  const token = getToken();
  if (token) {
    setToken(token);
  } else {
    removeToken();
    router.push({
      path: '/login'
    });
  }
}
export function removeToken() {
  // Ensure we remove the cookie-based token. Do not remove client-side
  // oauth_token here — that value is managed separately by dedicated helpers.
  return Cookies.remove(TokenKey, {
    domain: Domain
  });
}

/**
 * 每个域名保存不同的id
 */
export function getAppID() {
  return jsonToObj(Cookies.get(AppIDKey))[currentDomain] || '';
}

export function setAppId(appID, domain = currentDomain) {
  // 先获取id列表
  const idMap = jsonToObj(Cookies.get(AppIDKey));
  idMap[domain] = appID;
  return Cookies.set(AppIDKey, JSON.stringify(idMap), {
    domain: Domain
  });
}

export function removeAppID() {
  const idMap = jsonToObj(Cookies.get(AppIDKey));
  delete idMap[currentDomain];
  return Cookies.set(AppIDKey, JSON.stringify(idMap), {
    domain: Domain
  });
}

export function setRedirect(url) {
  return Cookies.set(RedirectKey, url, {
    domain: Domain
  });
}

const loginSignKey = 'login_TDC';
/** 设置登陆标志位 */
export function setLoginSign() {
  Cookies.set(loginSignKey, 'true');
}

/** 判断是否存在登陆标志位 */
export function isLogin() {
  return !!Cookies.get(loginSignKey);
}

export function clearLoginStateWhenReopen() {
  if (!isLogin()) {
    removeToken();
  }
}
