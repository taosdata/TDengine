import Vue from "vue";
import App from "./App.vue";
import store from "./store";
import router from "./router";
import i18n from "./lang";
import { setLang } from "@/lang";
import "@/styles/reset.css"; // CSS resets
import ELEMENT from "element-ui";
import 'element-ui/lib/theme-chalk/index.css'
import MainContentHeader from "@/components/MainContentHeader";
import Icon from "@/components/Icon";
import CopyText from "@/components/CopyText";
import "@/styles/element-variables.scss";
import "@/assets/fonts/index"; //svgs
import "@/styles/index.scss"; //global css
import "@/assets/fonts/iconfont/iconfont.css"
import directive, { LazyLoad } from "./directive";
import computed from "@/common/computed";
import { $bus } from "./const";
import { BusOnAndAutoOff } from "@/utils";
import "./permission";
import mavonEditor from 'mavon-editor'
import 'mavon-editor/dist/css/index.css'
import LinkTab from "@/components/LinkTab";
Vue.use(mavonEditor)
Vue.use(directive);
Vue.use(LazyLoad);
Vue.component("LinkTab", LinkTab);
Vue.component("MainContentHeader", MainContentHeader);
Vue.component("Icon", Icon);
Vue.component("CopyText", CopyText);
Vue.config.productionTip = false;
Vue.use(ELEMENT);
Vue.prototype.$bus = $bus;
Vue.prototype.$BusOnAndAutoOff = BusOnAndAutoOff;
export function getBrowserLang() {
  const nav = window.navigator;
  const browserLang = (nav.language || nav.browserLanguage || '').toLowerCase();
  if (browserLang.includes('zh')) return 'zh';
  if (browserLang.includes('en')) return 'en';
  return 'en';
}
setLang(getBrowserLang())
new Vue({
  router,
  store,
  i18n,
  computed,
  render: h => h(App),
}).$mount("#app");
