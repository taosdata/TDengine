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
          ></ConnectivityCheck>

        <section
          class="block-wrapper"
          :id="item.field"
          v-else-if="!item.hide"
          :key="item.label"
        >
          <div
            :class="{ mb10: !mb10Type.includes(item.type) }"
          >
            <BlockHeader v-if="item.type != 'tabs'" :title="item.label"> </BlockHeader>

            <DescItem
              v-if="item.hasValue"
              :config="item"
              :data="data[item.field]"
              :parent="parent"
            />
          </div>

          <CommonTransformer
            v-if="item.type == 'parser' && constmqttCols.length > 0"
            ref="transform"
            :parserColumns="constmqttCols"
          ></CommonTransformer>
          <CsvData
            v-else-if="item.type == 'csvData'"
            ref="csvdata"
            :isEditable="isEditable"
            :isViewable="isViewable"
          ></CsvData>
          <template v-else-if="item.type == 'tabs'">
            <template v-for="child in item.children">
              <div class="mb10" :key="child.label + 'tab'">
                <BlockHeader :title="tabTitle(child, item)"></BlockHeader>
              </div>
              <div
                v-if="tabContentShow(child, item)"
                :key="child.label"
                :label="child.label"
                :name="child.name"
              >
                <DescItem
                  v-if="tabContentShow(child, item)"
                  :config="child"
                  :data="data[item.field]"
                  :parent="parent + item.field + '.'"
                />
              </div>
            </template>
          </template>
          <template v-else-if="item.type == 'grouping'">
            <HostPort 
              :config="item.children"
              :data="data[item.field]"
              :isEditable="isEditable"
              :isViewable="isViewable"
            />
          </template>
          <template v-else>
            <div class="descriptions">
              <DescItem
                v-for="(child, index) in item.children"
                :key="child.label + '-' + index"
                :config="child"
                :data="data[item.field]"
                :parentConfigList="item.children"
                :parent="parent + item.field + '.'"
              />
            </div>
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

      <DescItem
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
import DescItem from "./descItem.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
import BlockHeader from "./blockHeader.vue";
import ConnectivityCheck from "../components/connectivityCheck.vue";
import { getBrowserLang } from "@/utils";
import { hasOwn } from "@/utils/util";
import CommonTransformer from "./transformerInfo.vue";
import CsvData from "./csvDataInfo.vue";
import HostPort from "./hostPort.vue";


export default {
  props: {
    config: {
      type: Array,
      default: () => [],
    },
    data: {
      type: Object,
      default: () => {},
    },
    parser: {
      type: Object,
      default: () => {},
    },
    parent: {
      type: String,
      default: "",
    },
    level: {
      type: Number,
      default: 0,
    },
    isEditable: {
      type: Boolean,
    },
    isViewable: {
      type: Boolean,
    },
  },
  name: "ConfigForm",
  inject: ["sourceParent"],
  components: {
    DescItem,
    DocsContent,
    BlockHeader,
    ConnectivityCheck,
    CommonTransformer,
    CsvData,
    HostPort
  },
  data() {
    this.mb10Type = [
      "opcTable",
      "parser",
      "tabs",
      "collapse",
      "csvData",
    ];
    return {
      constmqttCols: [],
    };
  },
  computed: {
    lang() {
      return getBrowserLang() == "zh" ? "zh" : "en";
    },
    activeName() {
      return this.isEditable ? "one" : "";
    },
  },
  watch: {
    parser: {
      deep: true,
      handler(val) {
        if (val) {
          this.$set(this, "constmqttCols", val.fields);
        }
      },
    },
  },
  created() {},
  mounted() {
    if (this.parser) {
      this.$set(this, "constmqttCols", this.parser.fields);
    }
  },
  methods: {
    tabContentShow(child, parent) {
      if (!hasOwn(parent, "multiple") || parent.multiple) return true;
      return child.name === this.data[parent.field][parent.valueField];
    },
    tabTitle(child,parent) {
      return this.data[parent.field][parent.valueField] === child.field ? parent.label + '-' +child.label : '';
    }
  },
};
</script>

<style scoped lang="scss">
.config-form {
  .block-wrapper {
    // border: 1px solid #ececef;
    // margin-bottom: 20px;
    border-radius: 12px;
    padding: 15px;
  }
  &:deep(.el-tabs__item.is-disabled) {
    cursor: not-allowed;
  }
  .docs-content {
    color: $color-description;
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
.descriptions {
  font-size: 16px;
  display: grid;
  grid-template-columns: 1fr 1fr;
}
</style>
