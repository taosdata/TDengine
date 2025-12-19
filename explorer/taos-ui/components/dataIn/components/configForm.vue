<template>
  <div class="config-form">
    <template v-for="item in config" :key="item.label">
      <template v-if="item.children">
        <ConnectivityCheck
          v-if="item.type == 'checkConnectivity'"
          :key="item.label"
          ref="checkConnectivityRef"
          :data="localData[item.field]"
          :parent="parent"
        ></ConnectivityCheck>
        <div v-else-if="item.type == 'collapse'" class="block-wrapper">
          <el-collapse :class="`advanced-${lang}`" accordion>
            <el-collapse-item name="one">
              <template #title>
                <div class="mb10">
                  <BlockHeader :title="item.label"> </BlockHeader>
                  <DocsContent v-if="item.description" class="docs-content" :content="item.description" />
                </div>
              </template>
              <FormItem
                v-for="(child, index) in item.children"
                :key="child.label + '-' + index"
                :config="child"
                :data="localData[item.field]"
                :parent-config-list="item.children"
                :parent="parent + item.field + '.'"
              />
            </el-collapse-item>
          </el-collapse>
        </div>
        <section v-else-if="!hide(item)" :id="item.field" class="block-wrapper">
          <div :class="{ mb10: !mb10Type.includes(item.type) && !item.hasValue }">
            <BlockHeader :title="item.label"> </BlockHeader>
            <FormItem v-if="item.hasValue" :config="item" :data="localData[item.field]" :parent="parent" />
            <DocsContent v-else-if="item.description" class="docs-content" :content="item.description" />
          </div>
          <CommonTransformer
            v-if="item.type == 'parser'"
            ref="transformRef"
            :parser-columns="item.fields"
            :editable-sample="item.editableSample !== undefined ? item.editableSample : parser?.editableSample"
          ></CommonTransformer>
          <CsvTransformer
            v-else-if="item.type == 'csvData'"
            ref="csvDataRef"
            v-model="localData[item.field]"
          ></CsvTransformer>
          <template v-else-if="item.type == 'tabs'">
            <el-tabs
              class="form-tabs"
              :model-value="item.valueField ? localData[item.field][item.valueField] : '0'"
              @tab-click="
                ({ paneName }) => (item.valueField ? (localData[item.field][item.valueField] = paneName) : '0')
              "
            >
              <el-tab-pane
                v-for="child in item.children"
                :key="child.label"
                :label="child.label"
                :name="child.name"
                :disabled="tabDisabled(child, item)"
              >
                <p v-if="child.short_description" class="docs-content">{{ child.short_description }}</p>
                <FormItem
                  v-if="tabContentShow(child, item)"
                  :config="child"
                  :data="localData[item.field]"
                  :parent="parent + item.field + '.'"
                />
              </el-tab-pane>
            </el-tabs>
          </template>

          <template v-else-if="item.type == 'grouping'">
            <HostPort
              :config="item.children"
              :data="localData[item.field]"
              :parent="parent + item.field + '.'"
              :parent-config-list="item.children"
            />
          </template>
          <template v-else>
            <FormItem
              v-for="(child, index) in item.children"
              :key="child.label + '-' + index"
              :config="child"
              :data="localData[item.field]"
              :parent-config-list="item.children"
              :parent-config="item"
              :parent="parent + item.field + '.'"
            />
          </template>
        </section>
        <ConfigForm
          v-if="hide(item) && !item.hideall && localData[item.field]"
          :key="item.label"
          :config="item.children"
          :data="localData[item.field]"
          :parent="parent + item.field + '.'"
        ></ConfigForm>
      </template>

      <FormItem v-else :key="item.label" :config="item" :data="data" :parent="parent" />
    </template>
  </div>
</template>

<script setup lang="ts">
import FormItem from './formItem.vue';
import DocsContent from 'components/MdRender.vue';
import BlockHeader from './blockHeader.vue';
import { isEn } from 'config';
import { hasOwnProperty } from 'utils/validate';
import { sourceForm, getNestedValue } from '../model/util';
import ConnectivityCheck from './connectivityCheck.vue';
import HostPort from './hostPort.vue';
import CommonTransformer from './commonTransformer/index.vue';
import CsvTransformer from './csv/csvTransformer.vue';

const props = withDefaults(
  defineProps<{
    config: Record<string, any>[];
    data: Record<string, any>;
    parser?: Record<string, any>;
    parent: string;
    // level: number;
    isEditable?: boolean;
  }>(),
  {
    config: () => [],
    data: () => ({}),
    parser: () => ({})
  }
);

const localData = reactive(props.data);
const mb10Type = ['opcTable', 'parser', 'tabs', 'advanced', 'collapse', 'csvData'];
const emit = defineEmits(['update:data']);
const lang = computed(() => (isEn.value ? 'en' : 'zh'));

watch(localData, newData => {
  emit('update:data', newData);
});

function tabDisabled(child: Recordable) {
  // 后续处理 pi 禁用的问题
  let disabled = false;
  if (child.disabledDependsOn && child.disabledDependsOnValues) {
    child.disabledDependsOn.every((dep: string) => {
      const deps = dep.split('/');
      const nestedValue = getNestedValue(sourceForm.data, dep);
      disabled = child.disabledDependsOnValues?.[deps[deps.length - 1]]?.includes(nestedValue);
    });
    return disabled;
  }
}

function tabContentShow(child: Recordable, parent: Recordable) {
  if (!hasOwnProperty(parent, 'multiple') || parent.multiple) return true;
  return child.name === props.data[parent.field][parent.valueField];
}

function hide(item: Recordable) {
  const allChildrenHidden = checkAllChildrenVisibility(item);
  return allChildrenHidden;
}

function checkAllChildrenVisibility(item: Recordable, isLastLevel = false) {
  if (item.children && item.children.length > 0) {
    return item.children.every((child: Recordable) => {
      if (child.children && child.children.length > 0) {
        if (isLastLevel) {
          // 如果已经是最后一层，则需要检查最后一层的 `hide` 属性
          return checkAllChildrenVisibility(child.children, true);
        }

        return item.hide; // 如果不是最后一层，直接跳过子元素的隐藏检查
      } else {
        // 如果没有子元素（即是最后一层），检查 `hide` 属性
        if (isLastLevel) {
          return child.hide === true;
        }
        return child.hide === true;
      }
    });
  }
  return item.hide;
}
</script>

<style scoped lang="scss">
$color-description: rgb(137 130 130);

.config-form {
  .block-wrapper {
    padding: 15px;
    margin-bottom: 20px;
    border: 1px solid #ececef;
    border-radius: 12px;
  }

  &:deep(.el-tabs__item.is-disabled) {
    cursor: not-allowed;
  }

  .docs-content {
    margin-bottom: 10px;
    font-size: 14px;
    color: $color-description;
    text-align: left;
  }

  &:deep(.el-tabs__item) {
    display: table-cell;
    max-width: 240px;
    line-height: 22px !important;
    word-wrap: break-word;
    white-space: pre-wrap;
    vertical-align: middle;
  }

  .form-tabs {
    margin-top: 1.5rem;
  }

  .mb10 {
    margin-bottom: 10px;
  }

  .advanced-en {
    :deep(.el-collapse-item__header) {
      min-height: 80px;
      border-bottom: 0;
    }

    :deep(.el-collapse-item__content) {
      padding-bottom: 0;
    }

    :deep(.el-collapse-item__wrap) {
      border-bottom: 0;
    }

    border-top: 0;
  }

  .advanced-zh {
    :deep(.el-collapse-item__header) {
      min-height: 60px;
      border-bottom: 0;
    }

    :deep(.el-collapse-item__content) {
      padding-bottom: 0;
    }

    :deep(.el-collapse-item__wrap) {
      border-bottom: 0;
    }

    border-top: 0;
  }
}
</style>
