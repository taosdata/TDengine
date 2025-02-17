import { createApp } from 'vue';
import 'virtual:uno.css';
import 'virtual:svg-icons-register';
import App from './App.vue';
import VueDOMPurifyHTML from 'vue-dompurify-html';
import { setupI18n } from 'locales';
import Icon from './components/SvgIcon.vue';
import * as ElementPlusIconsVue from '@element-plus/icons-vue';
import './styles/reset.css';
import './styles/index.scss';
import { highlight } from './directives';

const app = createApp(App);
app.use(VueDOMPurifyHTML);
app.component('Icon', Icon);
// 调试和开发选择了全部引入，所以链接到项目中是也需要全部引入后图标才可以使用
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component);
}
setupI18n(app);
app.directive('highlight', highlight);
app.mount('#app');
