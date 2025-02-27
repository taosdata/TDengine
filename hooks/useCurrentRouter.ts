import { getCurrentInstance } from 'vue';
export function useRoute() {
  const instance = getCurrentInstance();
  if (!instance) return {};
  const currentRoute = instance.appContext.config.globalProperties.$router.currentRoute;
  const reactiveRoute = reactive<Recordable>({});
  for (const key in currentRoute.value) {
    Object.defineProperty(reactiveRoute, key, {
      get: () => currentRoute.value[key],
      enumerable: true
    });
  }
  return reactiveRoute;
}
export function useRouter() {
  const instance = getCurrentInstance();
  if (!instance) return {};
  return instance.appContext.config.globalProperties.$router;
}
