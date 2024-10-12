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
import VueDOMPurifyHTML from 'vue-dompurify-html';
import { clearLoginStateWhenReopen } from '@/utils/token';
import { isFirefox } from '@/utils/is';
import './utils/update';  
Vue.use(mavonEditor)
Vue.use(directive);
Vue.use(LazyLoad);
Vue.component("LinkTab", LinkTab);
Vue.component("MainContentHeader", MainContentHeader);
Vue.component("Icon", Icon);
Vue.component("CopyText", CopyText);
Vue.config.productionTip = false;
// 修改tooltip的openDelay属性默认值为1000
ELEMENT.Tooltip.props.openDelay = {type: Number, default: 1000};
Vue.use(ELEMENT);
Vue.prototype.$bus = $bus;
Vue.prototype.$BusOnAndAutoOff = BusOnAndAutoOff;
Vue.prototype.$eventBus = new Vue();
Vue.prototype.$error = function (msg) {
  this.$message({
    showClose: true,
    message: msg,
    type: 'error',
    duration: 30000
  });
}
Vue.prototype.$COMMUNITY = (process.env.VUE_APP_COMMUNITY && process.env.VUE_APP_COMMUNITY === "community") ? true : false;
Vue.prototype.$INDUSTRY = process.env.VUE_APP_INDUSTRY
export function getBrowserLang() {
  const nav = window.navigator;
  const browserLang = localStorage.getItem('local_language') || (nav.language || nav.browserLanguage || '').toLowerCase();
  if (browserLang.includes('zh')) return 'zh';
  if (browserLang.includes('en')) return 'en';
  return 'en';
}
function setTitle() {
  const lang = getBrowserLang()
  const title = lang === 'en' 
    ? Vue.prototype.$COMMUNITY ? 'TDengine OSS' : Vue.prototype.$INDUSTRY ? 'TDengine Power Edition' : 'TDengine Enterprise' 
    : Vue.prototype.$COMMUNITY ? 'TDengine OSS' : Vue.prototype.$INDUSTRY ? 'TDengine 电力版' : 'TDengine 企业版'
  document.title = title
}
function checkFirefox() {
  if (isFirefox()) {
    document.documentElement.classList.add('firefox')
  }
}

checkFirefox()
setLang(localStorage.getItem('local_language') || getBrowserLang())
setTitle()
clearLoginStateWhenReopen()

Vue.use(VueDOMPurifyHTML, {
  default: {
    ALLOWED_ATTR: ['target', 'href', 'title', 'rel']
  }
});
new Vue({
  router,
  store,
  i18n,
  computed,
  render: h => h(App),
}).$mount("#app");
