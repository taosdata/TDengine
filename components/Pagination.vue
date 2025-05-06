<template>
  <div v-if="props.total" class="px-[10px] py-[16px] mt-[20px] flex justify-center">
    <el-pagination
      v-model:current-page="currentPage"
      v-model:page-size="pageSize"
      :layout="props.layout"
      :page-sizes="props.pageSizes"
      :size="props.size"
      :total="props.total"
      :hide-on-single-page="props.hideOnSinglePage"
      v-bind="attrs"
      @size-change="sizeChange"
      @current-change="pageChange"
    />
  </div>
</template>

<script lang="ts" setup>
import type { ComponentSize } from 'element-plus';
interface Props {
  currentPage: number;
  pageSize: number;
  pageSizes?: number[];
  layout?: string;
  hideOnSinglePage?: boolean;
  size?: ComponentSize;
  total: number;
}
const props = withDefaults(defineProps<Props>(), {
  currentPage: 1,
  pageSize: 10,
  total: 0,
  pageSizes: () => [10, 20, 30, 50],
  layout: 'total, sizes, prev, pager, next, jumper',
  hideOnSinglePage: true,
  size: 'small'
});
const emits = defineEmits(['update:currentPage', 'update:pageSize', 'pageChange', 'sizeChange']);
const currentPage = computed({
  get: () => props.currentPage,
  set: (val: number) => {
    emits('update:currentPage', val);
  }
});

const pageSize = computed({
  get: () => props.pageSize,
  set: (val: number) => {
    emits('update:pageSize', val);
  }
});
const attrs = useAttrs();
function pageChange(val: number) {
  emits('pageChange', val);
}
function sizeChange(val: number) {
  emits('sizeChange', val);
}
</script>

<style scoped lang="scss"></style>
