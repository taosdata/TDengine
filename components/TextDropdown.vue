<template>
  <div v-if="textList.length" class="text-dropdown">
    <el-popover placement="bottom-start" trigger="hover">
      <ol class="tag-list">
        <li v-for="(item, index) in textList.slice(1)" :key="item">
          <slot v-if="slots.item" :data="item" :index="index" name="item"></slot>
          <el-tag v-else size="small">{{ item }}</el-tag>
        </li>
      </ol>
      <template #reference>
        <el-badge :value="textList.length - 1" :show-zero="false" :offset="[0, 5]" type="primary">
          <slot v-if="slots.reference" name="reference" :original-data="texts" :data="textList"></slot>
          <el-tag v-else size="small">{{ textList[0] || '' }}</el-tag>
        </el-badge>
      </template>
    </el-popover>
  </div>
</template>

<script lang="ts" setup>
const props = withDefaults(
  defineProps<{
    texts: string[];
  }>(),
  {
    texts: () => [] as string[]
  }
);
const slots = useSlots();
const textList = computed(() => props.texts.filter(item => item));
</script>

<style scoped>
.text-dropdown {
  display: flex;
  align-items: center;
  height: 30px;
}

.tag-list {
  max-height: 200px;
  padding-left: 10px;

  li + li {
    margin-top: 5px;
  }
}
</style>
