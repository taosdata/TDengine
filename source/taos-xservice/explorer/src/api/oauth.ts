import Cookies from 'js-cookie';

import { request } from '@/utils/request.ts';
import pathDetector from '@/utils/pathDetector';
import AesCbc from '@/utils/aesCbcMac';

const apiPath = pathDetector.getApiBasePath();

/**
 * Get OAuth status - check if OAuth is enabled
 */
export function getOAuthStatus() {
  return request({
    baseURL: apiPath,
    url: `/oauth/status`,
    method: 'get',
    headers: {
      noAuth: true
    }
  });
}

/**
 * Initiate OAuth authorization flow
 * This will redirect to the OAuth provider
 */
export function oauthAuthorize() {
  const url = `${apiPath}/oauth/authorize`;
  window.location.href = url;
}

/**
 * OAuth bind - bind OAuth session to tsdb account
 */
export async function oauthBindTsdb(username: string, password: string) {
  const key = Cookies.get('encrypt_key') || '';
  console.log('OAuth key:', key);
  const encryptedPassword = AesCbc.encryptCbcMac(password, key);
  console.log(encryptedPassword);
  const decryptedPassword = AesCbc.decryptCbcMac(encryptedPassword, key);
  console.log(decryptedPassword);
  const data = { username, credential: encryptedPassword };
  return await request({
    baseURL: apiPath,
    url: `/oauth/bind`,
    method: 'post',
    withCredentials: true,
    data
  });
}

/**
 * Sync users from OAuth provider
 */
export function oauthSyncUsers(credentials: { username: string; password: string }) {
  return request({
    baseURL: apiPath,
    url: `/oauth/sync-users`,
    method: 'post',
    withCredentials: true,
    data: credentials
  });
}

/**
 * Revoke OAuth user
 */
export function oauthRevoke(userId: number) {
  return request({
    baseURL: apiPath,
    url: `/oauth/revoke`,
    method: 'post',
    withCredentials: true,
    data: { id: userId }
  });
}

/**
 * List existing OAuth users stored in Explorer
 */
export function oauthListExistingUsers(provider?: string) {
  const query = provider ? `?provider=${encodeURIComponent(provider)}` : '';
  return request({
    baseURL: apiPath,
    url: `/oauth/users${query}`,
    method: 'get',
    withCredentials: true
  });
}

/**
 * OAuth logout - invalidate the current session
 */
export function oauthLogout() {
  console.log('Logging out...');
  return request({
    baseURL: apiPath,
    url: `/logout`,
    method: 'post',
    withCredentials: true
  });
}

/**
 * Check if user is authenticated via OAuth
 * This will use the Bearer token stored in localStorage
 */
export function checkOAuthSession() {
  return request({
    baseURL: apiPath,
    url: `/profile`,
    method: 'get',
    withCredentials: true
  });
}

/**
 * Check if user is authenticated via OAuth
 * This will use the Bearer token stored in localStorage
 */
export function oauthMe(autoLogoutOn401 = true) {
  return request({
    baseURL: apiPath,
    url: `/me`,
    method: 'get',
    withCredentials: true,
    autoLogoutOn401
  });
}
