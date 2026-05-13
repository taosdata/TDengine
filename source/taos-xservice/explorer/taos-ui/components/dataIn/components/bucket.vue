<template>
  <div class="flex-start flex-1">
    <el-select
      v-model="localData[config.field]"
      :allow-create="true"
      style="flex: 1"
      class="mr20"
      :disabled="loading"
      :placeholder="config.placeholder"
      :multiple="config.multiple"
      clearable
      filterable
      @change="change"
    >
      <el-option v-for="item in bucketList" :key="item.value" v-bind="item"></el-option>
    </el-select>
    <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
      <template #content>
        <span v-dompurify-html="t('common.communityTip')"></span>
      </template>
      <el-button :loading="loading" :disabled="loading || dataInProps.isCommunity" type="primary" @click="search">{{
        t('dataIn.get' + (isInfluxdb ? 'schema' : 'metrics'))
      }}</el-button>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import type { ComponentInternalInstance } from 'vue';
import { t } from 'locales';
import { getDataInProps } from '../model/useDataIn';
import { sourceForm, currentPageType, validateFormFields } from '../model/util';
import { jsonToObj } from 'utils';
const dataInProps = getDataInProps();

const sourceParent = inject<ComponentInternalInstance>('sourceParent') as any;
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    parentConfigList: Record<string, any>[];
  }>(),
  {}
);
const localData = reactive(props.data);
const loading = ref<boolean>(false);

const bucketList = ref<any[]>([]);

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});

const measurementConfig = computed(() => {
  return props.parentConfigList.find(item => item.field === 'measurements') ?? {};
});
const isEdit = computed(() => currentPageType.value === 'edit');
const isInfluxdb = computed(() => sourceForm.type === 'influxdb');

onMounted(() => {
  if (isEdit.value) {
    search();
  }
});

function onValid(param: string, agent: number) {
  const params: any = {
    from_json: param,
    categories: ['nodes'],
    pattern: 'api',
    offset: 0,
    limit: 10
  };
  if (sourceForm.agent) {
    params['via'] = agent;
  }
  isInfluxdb.value ? searchBucket(params) : searchMetrics(params);
}

function search() {
  validateFormFields(sourceParent?.refs.formRef, onValid);
}
function searchBucket(params: Recordable) {
  if (loading.value) return;
  loading.value = true;
  dataInProps.dataSource.api
    .fechSets(params)
    .then(res => {
      if (!res[0] || !res?.[0]?.id) return (bucketList.value = []);
      const data = jsonToObj(res[0].id);
      bucketList.value = Object.keys(data).map(item => {
        return {
          label: item,
          value: item,
          children: data[item].map((ite: any) => ({ label: ite, value: ite }))
        };
      });
      if (!bucketList.value.some(item => item.value === localData[props.config.field])) {
        localData[props.config.field] = bucketList.value?.[0]?.value;
      }
      change(localData[props.config.field]);
    })
    .catch(() => {
      bucketList.value = [];
    })
    .finally(() => {
      loading.value = false;
    });
}
function searchMetrics(params: Recordable) {
  if (loading.value) return;
  loading.value = true;
  dataInProps.dataSource.api
    .fechSets(params)
    .then(res => {
      if (!res[0] || !res?.[0]?.id) return (bucketList.value = []);
      bucketList.value = jsonToObj(res[0].id).map((item: any) => ({
        label: item?.id ?? item,
        value: item?.id ?? item
      }));
    })
    .catch(() => {
      bucketList.value = [];
    })
    .finally(() => {
      loading.value = false;
    });
}
function change(val: string) {
  const measurementsOptions = bucketList.value.find(item => item.value === val)?.children ?? [];
  measurementConfig.value.options = measurementsOptions;
  if (!measurementsOptions.some((item: any) => item.value === localData.measurements)) {
    localData.measurements = '';
  }
}
</script>

<style scoped lang="scss"></style>
