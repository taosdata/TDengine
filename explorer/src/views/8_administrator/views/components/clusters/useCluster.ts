import { ref } from 'vue';
import { FormInstance, FormRules } from 'element-plus';

export default function useCounter() {
  const { t } = useI18n();
  const pageSize = ref(10);
  const currentPage = ref(1);
  const total = ref(10);
  const dialog = ref(false);
  const username = localStorage.getItem('username');
  const isDisable = ref(username === 'root');
  const ruleForm = reactive({
    endpoint: '',
    DNodes: ''
  });
  const rules = reactive<FormRules>({
    DNodes: [
      {
        required: true,
        message: t('required', ['DNodes'])
      }
    ]
  });

  const closeDialog = (formEl: FormInstance | undefined) => {
    if (!formEl) return;
    formEl.resetFields();
    formEl.clearValidate();
    dialog.value = false;
  };

  const openDialog = (formEl: FormInstance | undefined) => {
    dialog.value = true;
    if (!formEl) return;
    formEl.resetFields();
  };

  return {
    pageSize,
    currentPage,
    total,
    dialog,
    isDisable,
    ruleForm,
    rules,
    openDialog,
    closeDialog
  };
}
