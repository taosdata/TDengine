<template>
  <div class="config-form" :class="{ 'readonly-mode': isReadonly }">
    <template v-for="item in config" :key="item.label">
      <template v-if="item.children && item.label !== 'Groups-after' && item.label !== 'Groups-before'">
        <ConnectivityCheck
          v-if="item.type == 'checkConnectivity'"
          :key="item.label"
          ref="checkConnectivityRef"
          :data="localData[item.field]"
          :parent="parent"
        ></ConnectivityCheck>
        <div v-else class="block-wrapper">
          <el-collapse v-model="activeNames" :class="`advanced-${lang}`">
            <el-collapse-item :name="item.field || 'one'">
              <template #title>
                <div class="mb10">
                  <BlockHeader :title="item.label"> </BlockHeader>
                </div>
              </template>

              <!-- 添加 description 到内容区 -->
              <DocsContent v-if="item.description" class="docs-content" :content="item.description" />

              <!-- 原 collapse 类型的内容 -->
              <template v-if="item.type == 'collapse'">
                <FormItem
                  v-for="(child, index) in item.children"
                  :key="child.label + '-' + index"
                  :config="child"
                  :data="localData[item.field]"
                  :parent-config-list="item.children"
                  :parent="parent + item.field + '.'"
                />
              </template>

              <!-- 原 section 的内容 -->
              <section v-else-if="!hide(item)" :id="item.field">
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
                      :disabled="tabDisabled(child)"
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
            </el-collapse-item>
          </el-collapse>
        </div>
        <ConfigForm
          v-if="hide(item) && !item.hideall && localData[item.field]"
          :key="item.label"
          :config="item.children"
          :data="localData[item.field]"
          :parent="parent + item.field + '.'"
        ></ConfigForm>
      </template>

      <!-- 特殊处理 Groups-after 和 Groups-before 的子元素 -->
      <template v-if="item.children && (item.label === 'Groups-after' || item.label === 'Groups-before') && item.children.length > 0">
        <div v-for="child in item.children" :key="child.label" class="block-wrapper">
          <el-collapse v-model="activeNames" :class="`advanced-${lang}`">
            <el-collapse-item :name="child.field || child.label">
              <template #title>
                <div class="mb10">
                  <BlockHeader :title="child.label"> </BlockHeader>
                </div>
              </template>

              <!-- 添加 description 到内容区 -->
              <DocsContent v-if="child.description" class="docs-content" :content="child.description" />

              <!-- 渲染子元素的 children -->
              <FormItem
                v-for="(grandChild, index) in child.children"
                :key="grandChild.label + '-' + index"
                :config="grandChild"
                :data="localData[item.field][child.field]"
                :parent-config-list="child.children"
                :parent="parent + item.field + '.' + child.field + '.'"
              />
            </el-collapse-item>
          </el-collapse>
        </div>
      </template>

      <FormItem v-else-if="!item.children" :key="item.label" :config="item" :data="data" :parent="parent" />
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
import { useRoute } from 'vue-router';

const route = useRoute();
const injectedReadonly = inject<Ref<boolean>>('isReadonly', computed(() => route.query.readonly === 'true'));
const isReadonly = computed(() => injectedReadonly.value);

provide('isReadonly', isReadonly);

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
// const mb10Type = ['opcTable', 'parser', 'tabs', 'advanced', 'collapse', 'csvData'];
const emit = defineEmits(['update:data']);
const lang = computed(() => (isEn.value ? 'en' : 'zh'));
const activeNames = ref<string[]>([]); // 添加默认展开项数组

// 初始化所有折叠项为展开状态
onMounted(() => {
  props.config.forEach(item => {
    if (item.children && item.label !== 'Groups-after' && item.label !== 'Groups-before') {
      if (!['advanced_options', 'write_config'].includes(item.field)) {
        activeNames.value.push(item.field || 'one');
      }
    }
    if (item.children && (item.label === 'Groups-after' || item.label === 'Groups-before')) {
      item.children.forEach((child: any) => {
        activeNames.value.push(child.field || child.label);
      });
    }
  });
});

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
    padding: 0px 15px 0px 15px;
    margin-bottom: 10px;
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
    margin-bottom: 0px;
  }

  .advanced-en {
    :deep(.el-collapse-item__header) {
      display: flex;
      align-items: center;
      min-height: 40px;
      font-weight: 300;
      line-height: 1.2;
      border-bottom: 0;

      .mb10 {
        margin-bottom: 0;
        font-weight: 400;

        * {
          font-weight: 400 !important;
        }
      }
    }

    :deep(.el-collapse-item__content) {
      padding-bottom: 0;
    }

    :deep(.el-collapse-item__wrap) {
      border-bottom: 0;
    }

    :deep(.el-collapse-item__arrow) {
      transform: rotate(90deg);
    }

    :deep(.el-collapse-item.is-active .el-collapse-item__arrow) {
      transform: rotate(-90deg);
    }

    border-top: 0;
  }

  .advanced-zh {
    :deep(.el-collapse-item__header) {
      display: flex;
      align-items: center;
      min-height: 40px;
      font-weight: 300;
      line-height: 1.2;
      border-bottom: 0;

      .mb10 {
        margin-bottom: 0;
        font-weight: 400;

        * {
          font-weight: 400 !important;
        }
      }
    }

    :deep(.el-collapse-item__content) {
      padding-bottom: 0;
    }

    :deep(.el-collapse-item__wrap) {
      border-bottom: 0;
    }

    :deep(.el-collapse-item__arrow) {
      transform: rotate(90deg);
    }

    :deep(.el-collapse-item.is-active .el-collapse-item__arrow) {
      transform: rotate(-90deg);
    }

    border-top: 0;
  }
}

.config-form.readonly-mode {
  :deep(.el-input__inner),
  :deep(.el-textarea__inner),
  :deep(.el-input-number),
  :deep(.el-select),
  :deep(.el-switch),
  :deep(.el-checkbox),
  :deep(.el-radio),
  :deep(.el-upload),
  :deep(.el-button:not(.el-collapse-item__header *)) {
    pointer-events: none !important;
    opacity: 0.7;
    cursor: not-allowed !important;
  }

  :deep(.el-input.is-disabled .el-input__wrapper),
  :deep(.el-input__wrapper) {
    background-color: var(--el-disabled-bg-color, #f5f7fa) !important;
  }

  :deep(.el-input__inner) {
    color: var(--el-disabled-text-color, #a8abb2) !important;
    -webkit-text-fill-color: var(--el-disabled-text-color, #a8abb2) !important;
  }

  :deep(.el-select .el-input .el-select__caret) {
    display: none;
  }

  /* 强制覆盖：禁用输入的文字颜色为常规文本色 */
  :deep(.el-input.is-disabled .el-input__inner),
  :deep(.el-input__inner.is-disabled),
  :deep(.el-input__inner[disabled]) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: var(--el-text-color-regular) !important;
    opacity: 1 !important;
  }

  /* 强制覆盖：禁用下拉已选项文字颜色为常规文本色 */
  :deep(.el-select__wrapper.is-disabled .el-select__selected-item),
  :deep(.el-select.is-disabled .el-input__inner),
  :deep(.el-select__wrapper.is-disabled .el-select__single),
  :deep(.el-select__wrapper.is-disabled .el-select__tags),
  :deep(.el-select__wrapper.is-disabled .el-select__tags-text),
  :deep(.el-select__tags .el-select__tags-text),
  :deep(.el-select__selected-tag__text) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: #282b31 !important;
    opacity: 1 !important;
  }

  /* 保留已有的 disabled 文本色变量同步 webkit 文本填充色 */
  :deep(.el-input__inner) {
    color: var(--el-disabled-text-color, #a8abb2) !important;
    -webkit-text-fill-color: var(--el-disabled-text-color, #a8abb2) !important;
  }
}

.el-select__wrapper.is-disabled .el-select__selected-item {
  color: var(--el-text-color-regular);
}

.el-input.is-disabled .el-input__inner {
  color: var(--el-text-color-regular);
}
</style>
