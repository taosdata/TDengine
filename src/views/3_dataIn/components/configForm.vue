<template>
  <div class="config-form">
    <template v-for="item in config">
      <template v-if="item.children">
        <section
          class="block-wrapper"
          v-if="!item.hide"
          :class="{ 'block-wrapper': level }"
          :key="item.label"
        >
          <div :class="{ mb10: !mb10Type.includes(item.type) && !item.hasValue }">
            <BlockHeader :title="item.label"> </BlockHeader>
            <FormItem
              v-if="item.hasValue"
              :config="item"
              :data="data[item.field]"
              :parent="parent"
            />
            <DocsContent
              v-else-if="item.description"
              class="docs-content"
              :content="item.description"
            />
          </div>
          <!-- <OpcTable
            v-if="item.type == 'opcTable'"
            :data="data[item.field]"
            :parent="parent"
            v-bind="item"
          /> -->
          <ParserComp
            v-if="item.type == 'parser'"
            :data="data[item.field]"
            :parent="parent"
            v-bind="item"
          />
          <ConnectivityCheck
            v-if="item.type == 'collapse'"
            :data="data[item.field]"
            :parent="parent"
            v-bind="item"
          ></ConnectivityCheck>
          <template v-if="item.type == 'tabs'">
            <el-tabs
              class="form-tabs"
              :value="item.valueField ? data[item.field][item.valueField] : '0'"
              @tab-click="({ name }) => (item.valueField ? (data[item.field][item.valueField] = name) : '0')"
            >
              <el-tab-pane
                v-for="child in item.children"
                :key="child.label"
                :label="child.label"
                :name="child.name"
                :disabled="tabDisabled(child, item)"
              >
                <FormItem
                  v-if="tabContentShow(child, item)"
                  :config="child"
                  :data="data[item.field]"
                  :parent="parent + item.field + '.'"
                />
              </el-tab-pane>
            </el-tabs>
          </template>
          <template v-else>
            <FormItem
              v-for="(child, index) in item.children"
              :key="child.label + '-' + index"
              :config="child"
              :data="data[item.field]"
              :parentConfigList="item.children"
              :parent="parent + item.field + '.'"
            />
          </template>
        </section>
        <ConfigForm
          v-if="item.hide && !item.hideall"
          :key="item.label"
          :config="item.children"
          :data="data[item.field]"
          :parent="parent + item.field + '.'"
        ></ConfigForm>
      </template>

      <FormItem
        v-else
        :key="item.label"
        :config="item"
        :data="data"
        :parent="parent"
      />
    </template>
  </div>
</template>

<script>
import FormItem from './formItem.vue';
import DocsContent from '@/views/support/components/editorContentDisplay.vue';
import ParserComp from '../components/parserComp.vue';
// import OpcTable from './opcTable.vue';
import BlockHeader from './blockHeader.vue';
import ConnectivityCheck from '../components/connectivityCheck.vue'

import { hasOwn } from '@/utils/util';

export default {
  props: {
    config: {
      type: Array,
      default: () => []
    },
    data: {
      type: Object,
      default: () => {}
    },
    parent: {
      type: String,
      default: ''
    },
    level: {
      type: Number,
      default: 0
    }
  },
  name: 'ConfigForm',
  inject: ['sourceParent'],
  components: { FormItem, DocsContent, BlockHeader, ConnectivityCheck, ParserComp },
  data() {
    this.mb10Type = ['opcTable', 'parser', 'tabs'];
    return {};
  },
  computed: {},
  watch: {},
  created() {},
  mounted() {},
  methods: {
    tabDisabled(child, parent) {
      if (!hasOwn(child, 'disabled')) return false;
      const isFn = typeof child.disabled === 'function';
      return isFn ? child.disabled(this.data[parent.field], this.sourceParent.sourceForm.data) : child.disabled;
    },
    tabContentShow(child, parent) {
      if (!hasOwn(parent, 'multiple') || parent.multiple) return true;
      return child.name === this.data[parent.field][parent.valueField];
    }
  }
  // errorCaptured(err, vm, info) {
  //   console.log(info);
  //   return false;
  // }
};
</script>

<style scoped lang="scss">
.config-form {
  .block-wrapper {
    border: 1px solid #ececef;
    margin-bottom: 20px;
    border-radius: 12px;
    padding: 15px;
  }
  &:deep(.el-tabs__item.is-disabled) {
    cursor: not-allowed;
  }
  .docs-content {
    color: #acaab2;
    font-size: 14px;
  }
  &:deep(.el-tabs__item) {
    max-width: 240px;
    line-height: 22px !important;
    display: table-cell;
    vertical-align: middle;
    white-space: pre-wrap;
    word-wrap: break-word;
  }
  .form-tabs {
    margin-top: 1.5rem;
  }
  .mb10 {
    margin-bottom: 10px;
  }
}
</style>
