import { debounce } from 'lodash-es';
import { createApp } from 'vue';

import store from './store';
import router from './router/index.ts';
import { setupI18n } from './lang/index.ts';
import 'virtual:uno.css';
import '@/styles/font.css';
import ELEMENT, { ElMessage, ElTooltip } from 'element-plus';
import { setupElementIcons, setupPinia } from './plugins';
import './styles/reset.css';
import '@/assets/fonts/iconfont/iconfont.css';
import { registerDirective } from './directive.ts';
import './permission.ts';
import { setInit, $IS_COMMUNITY, $IS_TSDBLITE, $INDUSTRY, $IS_OEM, OEM_NAME } from '@/utils/init.ts';
import { TextCopy, SvgIcon, Pagination, PageHeader, RouterTabs, DatePicker } from 'taos-ui/components';
import VueDOMPurifyHTML from 'vue-dompurify-html';
import 'virtual:svg-icons-register';
import './utils/update.ts';
import App from './App.vue';

const app = createApp(App);
setupI18n(app);
setInit();
registerDirective(app);
// 修改tooltip的showAfter属性默认值为 500
ElTooltip.props.showAfter.default = 500;
app.use(ELEMENT);
app.use(VueDOMPurifyHTML);
app.component('Icon', SvgIcon);
app.component('Pagination', Pagination);
app.component('TextCopy', TextCopy);
app.component('PageHeader', PageHeader);
app.component('RouterTabs', RouterTabs);
app.component('TimezoneDatePicker', DatePicker);

const $error = function (msg: any) {
  ElMessage({
    showClose: true,
    message: msg,
    type: 'error',
    duration: 30000
  });
};

app.provide(
  'globalCustomProperties',
  reactive(
    readonly({
      $IS_COMMUNITY,
      $IS_TSDBLITE,
      $INDUSTRY,
      $IS_OEM,
      OEM_NAME,
      $error
    })
  )
);

setupPinia(app);
setupElementIcons(app);
app.use(store);
app.use(router);

app.mount('#app');
