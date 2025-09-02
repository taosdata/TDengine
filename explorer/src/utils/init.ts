import { setLang } from "@/lang/index.ts";
import { clearLoginStateWhenReopen } from '@/utils/token';
import { setLocale, setExecuteSqlFn, setGetDbListFn } from 'taos-ui/config';
import { sendSQLReq } from '@/api/explorer';
import { getDBListReq } from '@/api/database';
export const $IS_TSDBLITE = import.meta.env.VITE_APP_CUS_NAME && import.meta.env.VITE_APP_CUS_NAME === "TDengine TSDB-Lite";
export const $IS_COMMUNITY = (import.meta.env.VITE_APP_COMMUNITY && import.meta.env.VITE_APP_COMMUNITY === "community") ? true : false;
export const $INDUSTRY = import.meta.env.VITE_APP_INDUSTRY
export const $SYSINFO = true // 是否开启系统信息
const is_tdengine = import.meta.env.VITE_APP_CUS_NAME.includes("TDengine")
export const $IS_OEM = !$IS_TSDBLITE && import.meta.env.VITE_APP_CUS_NAME && !is_tdengine
export const OEM_NAME =
  import.meta.env.VITE_APP_CUS_NAME && !is_tdengine
    ? import.meta.env.VITE_APP_CUS_NAME
    : $IS_COMMUNITY ? "TDengine TSDB-OSS" : $IS_TSDBLITE ? "TDengine TSDB-Lite" : "TDengine TSDB-Enterprise";

export const GRAFANA_GDS =
  import.meta.env.VITE_APP_CUS_NAME && !is_tdengine
    ? ""
    : "TDengine TSDB";

/**
 * 是否火狐浏览器
 * @returns {boolean}
 */
export function isFirefox(): boolean {
  return navigator.userAgent.includes("Firefox");
}
/**
 * 火狐浏览器添加类名firefox
 */
export function setFirefoxClass(): void {
  if (isFirefox()) {
    document.documentElement.classList.add('firefox')
  }
}
/**
 * 获取浏览器语言
 * @returns {string}
 */
export function getBrowserLang(): string {
  const nav = window.navigator;
  const browserLang = localStorage.getItem('local_language') || (nav.language || '').toLowerCase();
  if (browserLang.includes('zh')) return 'zh';
  if (browserLang.includes('en')) return 'en';
  return 'en';
}
/**
 * 根据打包版本修改网页标题
 */
export function setTitle(): void {
  const title = $IS_COMMUNITY ? "TDengine TSDB-OSS" : import.meta.env.VITE_APP_CUS_NAME || "TDengine TSDB-Enterprise"
  document.title = title
}

export function setInit() {
  setFirefoxClass()
  setTitle()
  setLang(getBrowserLang())
  clearLoginStateWhenReopen()
  // taos-ui
  setExecuteSqlFn(sendSQLReq);
  setGetDbListFn(getDBListReq);
  setLocale(getBrowserLang())
}
