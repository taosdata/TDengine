<template>
  <div ref="myDivRef" class="custom-select" @click="divClicked">
    <template v-if="depth !== undefined && depth !== null">
      <div class="custom-input" @click="showOption">
        <el-input
          v-model="expression"
          autocomplete="off"
          :readonly="true"
          :placeholder="t('dataIn.transformer.jsonPlaceholder')"
          size="default"
        >
          <template #suffix>
            <i :class="['el-input__icon', isShow ? 'el-icon-arrow-up' : 'el-icon-arrow-down']"></i>
          </template>
        </el-input>
      </div>
      <ul v-if="isShow" class="custom-ul">
        <li v-for="proper in props.allProperties" :key="proper.defaultValue" class="custom-li">
          <el-checkbox v-model="proper.checked" class="my-checkbox">
            <span style="width: 200px">{{ proper.defaultValue }}</span>
          </el-checkbox>
          <el-input
            :key="proper.defaultValue"
            v-model="proper.rename"
            style="width: 200px; margin-left: 4px"
            size="default"
          ></el-input>
        </li>
      </ul>
    </template>
    <el-input
      v-else
      v-model="expression"
      :placeholder="t('dataIn.transformer.jsonPlaceholder')"
      size="default"
      @blur="$emit('update-data', expression)"
    >
    </el-input>
  </div>
</template>

<script setup lang="ts">
import { t } from 'locales';

const props = defineProps<{
  allProperties: Recordable[];
  depth: number | undefined;
  keep: boolean | undefined;
  modelValue: string;
}>();

const isShow = ref(false);
const expression = ref('');
const myDivRef = ref();

const emit = defineEmits(['update-data', 'select-json']);

watch(
  () => props.allProperties,
  newVal => {
    const result: string[] = [];
    newVal.map(item => {
      if (item.checked) {
        item.rename ? result.push(`${item.defaultValue}=${item.rename}`) : result.push(item.defaultValue);
      }
    });

    if (String(props.depth) !== 'undefined') {
      expression.value = result?.join(',');
    } else {
      expression.value = props.modelValue;
    }
    emit('update-data', expression.value);
  },
  {
    deep: true,
    immediate: true
  }
);

onMounted(() => {
  // 在mounted钩子中添加事件监听
  document.addEventListener('click', documentClicked);
});
onBeforeUnmount(() => {
  // 在组件销毁前移除事件监听
  document.removeEventListener('click', documentClicked);
});

function divClicked(event: MouseEvent) {
  // 阻止冒泡
  event.stopPropagation();
}
function documentClicked(event: MouseEvent) {
  // 如果点击的是div外部，执行外部点击的操作
  if (!myDivRef.value?.contains(event.target)) {
    isShow.value = false;
  }
}
function showOption() {
  isShow.value = !isShow.value;
  if (isShow.value) {
    emit('select-json');
  }
}
</script>

<style scoped>
.custom-select {
  position: relative;
  display: inline-block;
  width: 100%;
}

.custom-input ::v-deep .el-input__inner:hover {
  cursor: pointer;
}

.custom-ul {
  position: absolute;
  z-index: 100;
  width: 100%;
  max-height: 300px;
  padding: 10px;
  overflow: auto;
  background: white;
  border: 1px solid #eee;
  border-radius: 4px;
}

.custom-li {
  display: flex;
  justify-content: space-between;
  margin-bottom: 5px;
}

.my-checkbox {
  display: block;
}
</style>
