<template>
  <div class="advance-filter">
    <el-checkbox v-model="filterForm.enable">{{ t('explorer.enableAdvancedFilter') }}</el-checkbox>
    <el-tabs v-show="filterForm.enable" v-model="activeTab">
      <el-tab-pane :label="t('explorer.conditions')" name="0">
        <el-form ref="formRef" size="small" :model="filterForm">
          <el-form-item label-width="0" prop="conditionJson">
            <SqlCondition :data="filterForm.conditionJson" top :fields="fields" :parent-field="'conditionJson.'" />
          </el-form-item>
        </el-form>
      </el-tab-pane>
    </el-tabs>
    <section class="mt-20px">
      <el-button class="operate-btn" @click="cancel">{{ t('common.cancel') }}</el-button>
      <el-button class="operate-btn" type="primary" @click="confirm">{{ t('common.confirm') }}</el-button>
    </section>
  </div>
</template>

<script lang="ts" setup>
import { generateConditionString } from 'components/SqlCondition/utils';
import { getCurrentInfoDataProvider, applyStbAdvancedEvent, stableAdvancedFilterData } from './utils';
import { FormInstance } from 'element-plus';
import { t } from 'locales';
const currentInfoData = getCurrentInfoDataProvider();
const currentStbData = computed(() => currentInfoData.stb);
const filterForm = ref({
  enable: false,
  conditionJson: [],
  sql: ''
});
const activeTab = ref('0');
const fields = computed(() =>
  currentStbData.value.tags.map((item: Recordable) => ({ field: item.tag_name, type: item.tag_type }))
);

const formRef = shallowRef<FormInstance | null>(null);
const cancel = inject('CANCEL_DETAIL', () => {});
watch(
  () => currentStbData.value.name,
  () => {
    init();
  },
  { immediate: true }
);

watch(stableAdvancedFilterData, () => {
  init();
});
function init() {
  filterForm.value = { ...stableAdvancedFilterData.data };
}
async function confirm() {
  if (
    activeTab.value === '0' &&
    formRef.value &&
    !(await formRef.value
      .validate()
      .then(() => true)
      .catch(() => false))
  )
    return;
  applyStbAdvancedEvent.emit({
    key: stableAdvancedFilterData.key,
    data: {
      ...filterForm.value,
      condition: generateConditionString(filterForm.value.conditionJson, fields.value, true),
      type: activeTab.value
    }
  });
}
</script>

<style scoped lang="scss">
.advance-filter:deep(.el-tabs .el-tabs__content) {
  padding-bottom: 0;
}

.advance-filter:deep(.el-tabs .el-form-item--mini.el-form-item) {
  margin-bottom: 0;
}

.operate-btn {
  width: 140px;
}
</style>
