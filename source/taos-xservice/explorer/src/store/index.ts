import { createPinia } from 'pinia';
import { App } from 'vue';

export function setupPinia(app: App) {
  const store = createPinia();
  app.use(store);
}

const modules: Recordable = {};
const modulesFiles = import.meta.glob<true, string, any>('./**/*.ts', { eager: true });

for (const path in modulesFiles) {
  const moduleName = path.replace(/.+\/([^/.]+)\.\w+/, '$1');
  modules[moduleName] = modulesFiles[path].default;
  if (import.meta.hot) {
    import.meta.hot.accept(acceptHMRUpdate(modules[moduleName], import.meta.hot));
  }
}

export default modules;
