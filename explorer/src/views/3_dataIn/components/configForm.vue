<template>
  <div class="config-form">
    <template v-for="item in config">
      <template v-if="item.children">

        <ConnectivityCheck
            v-if="item.type == 'checkConnectivity'"
            :data="data[item.field]"
            :parent="parent"
            v-bind="item"
            :key="item.label"
            ref="checkConnectivity"
          ></ConnectivityCheck>

        <section
          class="block-wrapper"
          :id="item.field"
          v-else-if="!item.hide"
          :key="item.label"
        >
          <div 
            :class="{ mb10: !mb10Type.includes(item.type) && !item.hasValue }" 
            v-if="item.type !='advanced'">
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
          
          <CommonTransformer 
            v-if="item.type == 'parser' && constmqttCols.length > 0" 
            ref='transform' 
            :parserColumns="constmqttCols"
          ></CommonTransformer>
          <CsvData
            v-else-if="item.type == 'csvData'"
            ref="csvdata"
            :isEditable="isEditable"
          ></CsvData>
          <template v-else-if="item.type == 'tabs'">
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
                <p class="docs-content" v-if="child.short_description">{{ child.short_description }}</p>
                <FormItem
                  v-if="tabContentShow(child, item)"
                  :config="child"
                  :data="data[item.field]"
                  :parent="parent + item.field + '.'"
                />
              </el-tab-pane>
            </el-tabs>
          </template>
          <template v-else-if="item.type == 'advanced'">
            <el-collapse 
              :class='`advanced-${lang}`'
              v-model="activeName" 
              accordion>
              <el-collapse-item name='one'>
                <template slot="title">
                  <div class="mb10">
                    <BlockHeader :title="item.label"> </BlockHeader>
                    <DocsContent
                      v-if="item.description"
                      class="docs-content"
                      :content="item.description"
                    />
                  </div>
                </template>
                <FormItem
                  v-for="(child, index) in item.children"
                  :key="child.label + '-' + index"
                  :config="child"
                  :data="data[item.field]"
                  :parentConfigList="item.children"
                  :parent="parent + item.field + '.'"
                />
              </el-collapse-item>
            </el-collapse>
          </template>
          <template v-else-if="item.type == 'grouping'">
            <HostPort 
              :config="item.children"
              :data="data[item.field]"
              :parent="parent + item.field + '.'"
              :parentConfigList="item.children"
            />
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
          v-if="item.hide && !item.hideall && data[item.field]"
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
import { getOptionsValue } from '../utils.js';
import { getBrowserLang } from '@/utils';
import { hasOwn } from '@/utils/util';
import CommonTransformer from './commonTransformer.vue'
import CsvData from "./csvData.vue";
import HostPort from "./hostPort.vue";

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
    parser:{
      type:Object,
      default:()=>{}
    },
    parent: {
      type: String,
      default: ''
    },
    level: {
      type: Number,
      default: 0
    },
    isEditable: {
      type: Boolean
    }
  },
  name: 'ConfigForm',
  inject: ['sourceParent'],
  components: { FormItem, DocsContent, BlockHeader, ConnectivityCheck, ParserComp, CommonTransformer, CsvData, HostPort },
  data() {
    this.mb10Type = ['opcTable', 'parser', 'tabs', 'advanced', 'collapse', 'csvData'];
    return {
      constmqttCols:[]
    };
  },
  computed: {
    lang() {
      return getBrowserLang() == 'zh' ? 'zh': 'en'
    },
    activeName() {
      return this.isEditable ? 'one' : ''
    }
  },
  watch: {
    parser:{
      deep:true,
      handler(val){
        if(val){
          this.$set(this, "constmqttCols", val.fields);
        }
      }
    }
  },
  created() {},
  mounted() {
    if(this.parser){
      this.$set(this, "constmqttCols", this.parser.fields);
    }
  },
  methods: {
    tabDisabled(child, parent) {
      if (!hasOwn(child, 'disabled')) return false;

      if (child.category === 'multi-column' && getOptionsValue(this.sourceParent.sourceForm.data)['system_configuration'].indexOf('AF') < 0) {
        return true;
      }

      const isFn = typeof child.disabled === 'function';
      return isFn ? child.disabled(this.data[parent.field], this.sourceParent.sourceForm.data) : child.disabled;
    },
    tabContentShow(child, parent) {
      if (!hasOwn(parent, 'multiple') || parent.multiple) return true;
      return child.name === this.data[parent.field][parent.valueField];
    }
  }
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
    color: $color-description;
    font-size: 14px;
    margin-bottom: 10px;
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
  .advanced-en {
    :deep(.el-collapse-item__header) {
      min-height: 80px;
      border-bottom: 0;
    }
    :deep(.el-collapse-item__content) {
      padding-bottom: 0,
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
      padding-bottom: 0,
    }
    :deep(.el-collapse-item__wrap) {
      border-bottom: 0;
    } 
    border-top: 0;
  }
}
</style>
