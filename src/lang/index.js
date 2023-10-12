import Vue from "vue";
import VueI18n from "vue-i18n";
import elEnLocale from "element-ui/lib/locale/lang/en"; // English
import elZhLocale from "element-ui/lib/locale/lang/zh-CN"; // Simplified Chinese
import locale from "element-ui/lib/locale";
import enLocale from "./en";
import zhLocale from "./zh";

Vue.use(VueI18n);

const messages = {
  en: {
    ...enLocale,
    ...elEnLocale,
  },
  zh: {
    ...zhLocale,
    ...elZhLocale,
  },
};
window.languageList = [
  {
    label: "English",
    value: "en",
  },
  {
    label: "中文",
    value: "zh",
  },
];
const i18n = new VueI18n({
  locale: "en",
  messages,
});
export function setLang(lang) {
  i18n.locale = lang || "en";
}
locale.i18n((key, value) => i18n.t(key, value));
console.log(i18n,'i18n语言');
export default i18n;
