import { createPinia, setActivePinia } from 'pinia';
import { App as VueApp } from 'vue';

export function setupPinia(app: VueApp) {
  const store = createPinia();
  app.use(store);
  setActivePinia(store);
  (window as any).pinia = store;
}
