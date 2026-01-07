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
  // No longer set basic auth token in cookies
  // Session ID is used for verification instead
  return token;
}

export function refreshTokenExpire() {
  const token = getToken();
  if (token) {
    setToken(token);
  } else {
    router.push({
      path: '/login'
    });
  }
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
