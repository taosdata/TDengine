import { request } from '@/utils/request.ts';
import pathDetector from '@/utils/pathDetector';

const apiPath = pathDetector.getApiBasePath();

/** Token login: POST /api/-/login/token */
export function loginWithToken(token: string) {
  return request({
    baseURL: apiPath,
    url: '/login/token',
    method: 'post',
    autoLogoutOn401: false,
    data: { token },
  });
}

/** TOTP enable step 1 (generate secret) or step 2 (verify binding) */
export function totpEnable(totp_code?: string, encrypted_password?: string) {
  const data: Record<string, string> = {};
  if (totp_code) data.totp_code = totp_code;
  if (encrypted_password) data.encrypted_password = encrypted_password;
  return request({
    baseURL: apiPath,
    url: '/profile/totp/enable',
    method: 'post',
    data,
  });
}

/** TOTP disable: verify totp_code + password then drop TOTP_SECRET */
export function totpDisable(totp_code: string, encrypted_password: string) {
  return request({
    baseURL: apiPath,
    url: '/profile/totp/disable',
    method: 'post',
    data: { totp_code, encrypted_password },
  });
}
