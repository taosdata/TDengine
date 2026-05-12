import type { App } from 'vue';
import { type I18n, createI18n, I18nOptions, type VueI18nTranslation } from 'vue-i18n';
// import elEnLocale from "element-plus/es/locale/lang/en"; // English
// import elZhLocale from "element-plus/es/locale/lang/zh-cn"; // Simplified Chinese

const modules: Record<string, any> = {};
const modulesFiles = import.meta.glob<true, string, any>(['./**/*.ts', '!./**/*.test.ts', '!./**/*.spec.ts'], {
  eager: true
});

for (const path in modulesFiles) {
  if (/\.(test|spec)\.ts$/.test(path)) {
    continue;
  }
  const moduleName = path.replace(/\.\/([^/]+).*/, '$1');
  // const namespace = path.replace(/.+\/([^/.]+)\.\w+/, '$1');
  if (!modules[moduleName]) {
    modules[moduleName] = {};
  }
  modules[moduleName] = Object.assign(modules[moduleName], modulesFiles[path].default);
  // modules[moduleName][namespace] = modulesFiles[path].default;
}

// const messages = {
//   en: {
//     ...enLocale,
//     ...elEnLocale,
//   },
//   zh: {
//     ...zhLocale,
//     ...elZhLocale,
//   },
// };

const localeData: I18nOptions = {
  locale: localStorage.getItem('local_language') || 'en',
  messages: modules,
  warnHtmlMessage: false,
  legacy: false,
  globalInjection: true // 全局模式，可以直接使用 $t
};
const i18n = createI18n(localeData) as I18n;
export function setupI18n(app: App) {
  app.use(i18n);
}
export function setLang(lang: string) {
  (i18n.global.locale as WritableComputedRef<string>).value = lang;
  localStorage.setItem('local_language', lang);
}
// locale.i18n((key, value) => i18n.global.t(key, value));
export const t: VueI18nTranslation = i18n.global.t;
export default i18n;
