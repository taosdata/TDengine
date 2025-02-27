import type { App } from 'vue';
import { type I18n, I18nOptions, createI18n, type VueI18nTranslation } from 'vue-i18n';
import elEnLocale from 'element-plus/es/locale/lang/en';
import elZhLocale from 'element-plus/es/locale/lang/zh-cn';

const modules: Record<string, any> = {};
const modulesFiles = import.meta.glob<true, string, any>('./**/*.ts', { eager: true });

for (const path in modulesFiles) {
  const moduleName = path.replace(/\.\/([^/]+).*/, '$1');
  const namespace = path.replace(/.+\/([^/.]+)\.\w+/, '$1');
  if (!modules[moduleName]) {
    modules[moduleName] = {};
  }
  modules[moduleName][namespace] = modulesFiles[path].default;
}
const messages = {
  en: {
    ...modules.en,
    ...elEnLocale
  },
  zh: {
    ...modules.zh,
    ...elZhLocale
  }
};
const localeData: I18nOptions = {
  legacy: false, // composition API
  locale: 'en',
  messages,
  warnHtmlMessage: false
};
export const i18n = createI18n(localeData) as I18n;

export function setupI18n(app: App) {
  app.use(i18n);
}

export function setLocale(lang: string) {
  (i18n.global.locale as WritableComputedRef<string>).value = lang;
}

export const t: VueI18nTranslation = i18n.global.t;

export default modules;
