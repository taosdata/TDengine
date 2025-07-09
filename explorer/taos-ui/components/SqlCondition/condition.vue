<template>
  <el-form :model="currentValue" class="condition" :class="{ top }">
    <div>
      <template v-for="(item, index) in currentValue">
        <section
          v-if="isGroupItemType(item)"
          :key="'group-' + item.id"
          class="condition-group has-border"
          :class="{
            'border-none': currentValue.length == 1,
            'no-child': !item.children?.length
          }"
        >
          <section class="condition-group__header">
            <el-tooltip effect="light" :content="t('explorer.addRule')">
              <el-button
                size="small"
                icon="plus"
                :disabled="!getIsAddNewRule(currentValue)"
                @click="append(item.children)"
              ></el-button>
            </el-tooltip>
            <el-tooltip effect="light" :content="t('explorer.addRuleGroup')">
              <el-button
                size="small"
                icon="folderAdd"
                :disabled="!getIsAddNewRule(currentValue)"
                @click="appendGroup(item.children)"
              ></el-button>
            </el-tooltip>
            <el-tooltip effect="light" :content="t('explorer.delRuleGroup')">
              <el-button size="small" icon="delete" @click="delGroup(index)"></el-button>
            </el-tooltip>
          </section>
          <section class="condition-group__content">
            <Condition v-model="item.children" :fields="props.fields" :is-form="props.isForm" />
            <div v-if="index != currentValue.length - 1" :key="item.id + 1">
              <el-radio-group v-model="item.connector" size="small">
                <el-radio-button value="AND">AND</el-radio-button>
                <el-radio-button value="OR">OR</el-radio-button>
              </el-radio-group>
            </div>
          </section>
        </section>
        <ConditionItem
          v-else
          :key="item.id"
          v-model="currentValue[index] as RuleDataItem"
          :is-form="props.isForm"
          class="has-border"
          :class="{
            'border-none': currentValue.length == 1
          }"
          :parent-field="index + '.'"
          :lasted="index === currentValue.length - 1"
          :fields="props.fields"
          @del="delRule(index)"
        />
      </template>
    </div>
    <section v-if="props.top" class="mt-20px">
      <el-tooltip effect="light" :content="t('explorer.addRule')">
        <el-button
          size="small"
          icon="plus"
          plain
          :disabled="!getIsAddNewRule(currentValue)"
          @click="append(currentValue)"
        ></el-button>
      </el-tooltip>
      <el-tooltip effect="light" :content="t('explorer.addRuleGroup')">
        <el-button
          size="small"
          icon="folderAdd"
          plain
          :disabled="!getIsAddNewRule(currentValue)"
          @click="appendGroup(currentValue)"
        ></el-button>
      </el-tooltip>
    </section>
  </el-form>
</template>

<script lang="ts" setup>
import { NoValueOperator } from 'constants1/tdengine';
import ConditionItem from './conditionItem.vue';
import { ConditionProps, DataItem, isGroupItemType, generateConditionString, RuleDataItem } from './utils';
import { t } from 'locales';

const props = withDefaults(defineProps<ConditionProps>(), {
  modelValue: () => [],
  fields: () => [],
  top: false,
  isForm: true
});

const currentValue = computed({
  get: () => props.modelValue,
  set: val => {
    emit('update:modelValue', val);
  }
});
const emit = defineEmits(['update:modelValue']);
defineOptions({
  name: 'Condition'
});
defineExpose({
  generateSql: (isTag: boolean) => generateConditionString(currentValue.value, props.fields, isTag)
});
function delGroup(index: number) {
  currentValue.value.splice(index, 1);
}
function delRule(index: number) {
  currentValue.value.splice(index, 1);
}
function append(data: DataItem[]) {
  data.push({
    id: Date.now(),
    field: '',
    operator: '=',
    value: '',
    connector: 'AND'
  });
}
function appendGroup(data: DataItem[]) {
  data.push({
    id: Date.now(),
    connector: 'AND',
    children: [
      {
        id: Date.now(),
        field: '',
        operator: '=',
        value: '',
        connector: 'AND'
      }
    ]
  });
}
function getIsAddNewRule(data: DataItem[]): boolean {
  return data.every(item => {
    if (isGroupItemType(item)) {
      return getIsAddNewRule(item.children);
    } else {
      return item.field && item.operator && (item.value || NoValueOperator.includes(item.operator));
    }
  });
}
</script>

<style scoped lang="scss">
$padding: 10px;

.condition {
  --border-color: #dcdfe6;

  &.top {
    padding: 10px;
    margin: 2px 0;
    border: 1px solid var(--border-color);
    border-radius: 4px;
  }

  .border-none {
    border-left: none;
  }

  &:deep(.el-radio-button--mini .el-radio-button__inner) {
    padding: 5px 10px;
  }
}

.condition-group {
  border: 1px solid var(--border-color);

  & + .condition-group {
    border-top: none;
  }

  &__content > .condition > div > section:last-child:first-child {
    border-left: none;
  }
}

.condition-group__header {
  display: flex;
  align-items: center;
}

.has-border {
  position: relative;
  padding: $padding;
  border-left: 1px solid var(--border-color);

  &::before {
    position: absolute;
    display: block;
    width: 1px;
    height: 50%;
    content: '';
    background-color: white;
  }

  &.no-child::after {
    display: none;
  }

  &.condition-group::before {
    display: none;
  }

  &.condition-group::after {
    top: calc(50% + 12px);
  }

  &:last-child::before {
    bottom: 0;
    left: -1px;
  }

  &:first-child::before {
    top: 0;
    left: -1px;
  }

  &::after {
    position: absolute;
    top: 50%;
    left: 0;
    display: block;
    width: $padding;
    height: 1px;
    content: '';
    background-color: var(--border-color);
  }
}
</style>
