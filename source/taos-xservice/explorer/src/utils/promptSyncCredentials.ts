import { h, reactive } from 'vue';
import type { FormRules } from 'element-plus';
import { ElForm, ElFormItem, ElInput, ElMessage, ElMessageBox } from 'element-plus';

export type SyncCredentials = {
  password: string;
};

export type PromptSyncCredentialsOptions = {
  title?: string;
  pleaseInputPassword?: string;
  confirmSyncText?: string;
  confirmButtonText?: string;
  cancelButtonText?: string;
};

export async function promptSyncCredentials(options: PromptSyncCredentialsOptions = {}): Promise<SyncCredentials> {
  const form = reactive<SyncCredentials>({
    password: ''
  });

  const rules: FormRules<SyncCredentials> = {
    password: [{ required: true, message: 'Password is required', trigger: 'blur' }]
  };

  const renderForm = () =>
    h(
      ElForm,
      {
        model: form,
        rules,
        labelPosition: 'top',
        size: 'small',
        class: 'sync-credentials-form'
      },
      {
        default: () => [
          h(ElFormItem, { label: options.pleaseInputPassword ?? 'Please Input Password', prop: 'password' }, () =>
            h(ElInput, {
              modelValue: form.password,
              'onUpdate:modelValue': (val: string) => (form.password = val),
              autocomplete: 'current-password',
              showPassword: true,
              type: 'password'
            })
          )
        ]
      }
    );

  return new Promise<SyncCredentials>((resolve, reject) => {
    ElMessageBox({
      title: options.title ?? 'Enter SSO credentials to sync users',
      message: renderForm(),
      showCancelButton: true,
      closeOnClickModal: false,
      closeOnPressEscape: false,
      showClose: false,
      confirmButtonText: options.confirmButtonText ?? 'Sync',
      cancelButtonText: options.cancelButtonText ?? 'Cancel',
      async beforeClose(action, instance, done) {
        if (action !== 'confirm') {
          done();
          reject(new Error('cancel'));
          return;
        }
        instance.confirmButtonLoading = true;
        try {
          done();
        } catch (err: any) {
          const msg = err?.message || 'Please fill in required fields';
          ElMessage.error(msg);
          instance.confirmButtonLoading = false;
        }
      }
    })
      .then(() => {
        resolve({
          password: form.password
        });
      })
      .catch(err => {
        reject(err instanceof Error ? err : new Error(String(err)));
      });
  });
}
