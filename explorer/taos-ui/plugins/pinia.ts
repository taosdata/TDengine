import { createPinia, setActivePinia } from 'pinia';
import { App as VueApp } from 'vue';
const store = createPinia();

export default store;

export function setupPinia(app: VueApp) {
  app.use(store);
  setActivePinia(store);
  (window as any).pinia = store;
}
