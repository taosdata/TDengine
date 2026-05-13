import { defineAsyncComponent } from 'vue';
export default function () {
  const AsyncDialogComp = defineAsyncComponent(() => import('../components/Dialog.vue'));
  const dialog = ref(false);
  const dialogConfig = ref<InstanceType<typeof AsyncDialogComp>['$props']>({
    config: {},
    comp: null
  });
  return {
    dialog,
    dialogConfig,
    AsyncDialogComp
  };
}
