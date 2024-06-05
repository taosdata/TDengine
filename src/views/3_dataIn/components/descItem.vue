<template>
  <div v-if="config && data && display" :class="[(config.children && !config.hasValue) ? 'descriptions' : '', {'grid-item-span-two': config.grid_two}]">
    <template v-if="config.children && !config.hasValue">
      <DescItem
        v-for="item in config.children"
        :key="item.label"
        :config="item"
        :data="data[field]"
        :parent="parent + field + '.'"
      />
    </template>
    <!-- 排除tmq中 dsn 自己带的参数，这类 param 特点就是 type=input,没有label  -->
    <div
      v-else-if="display && (labelText || config.type !== 'input')"
      :class="[classMark, 'descItem', {'grid-item-span-two': config.grid_two}]"
      :prop="parent + field"
    >
      <!-- <template slot="label">
        <el-tooltip placement="top" effect="light" :open-delay="0" v-if="doscShow && !dataSetDocsShow">
          <template slot="content">
            <DocsContent
              v-if="doscShow && !dataSetDocsShow"
              :style="docsStyle"
              :class="config.templateUrl ? 'noboder' : ''"
              :content="config.description"
            />
          </template>
          <span>
            <span>{{ labelText }}</span>
            <span v-if="doscShow && !dataSetDocsShow" style="margin-left: 1px">
              <Icon name="label_info" class="info_icon_custom"></Icon>
            </span>
          </span>
        </el-tooltip>
      </template> -->
      <template>
        <span style="padding-right: 10px">{{ labelText }}:</span>
        <span v-if="config.type == 'select'">{{ getOptions(data[field]) }}</span>
        <a v-if="config.type == 'file'" @click="handleDownloadFile(data[field])">{{ getFile(data[field]) }}</a>
        <span v-else>{{ data[field] }}</span>
      </template>

      <!-- <el-input
        v-if="inputType.includes(config.type)"
        :id="parent + field"
        v-model="data[field]"
        :disabled="disabled()"
        :type="config.type"
        :show-password="config.type == 'password'"
        :placeholder="config.placeholder"
      ></el-input>
      <el-input-number
        v-if="config.type == 'number'"
        :id="parent + field"
        v-model="data[field]"
        :disabled="disabled()"
        :max="config.max"
        :min="config.min"
        :placeholder="config.placeholder"
      ></el-input-number>
      <el-select
        v-if="config.type == 'select'"
        :id="parent + field"
        v-model="data[field]"
        v-bind="meta"
        class="ds-select"
        clearable
        :disabled="disabled()"
        :placeholder="config.placeholder"
        :multiple="config.multiple"
      >
        <el-option
          v-for="item in getOptions()"
          :key="item.value"
          v-bind="item"
          :title="item.description"
          :disabled="item.disabled"
        ></el-option>
      </el-select>
      <el-switch
        v-if="config.type == 'switch'"
        :id="parent + field"
        v-model="data[field]"
        :disabled="disabled()"
        :placeholder="config.placeholder"
        @change="changeSwith"
      ></el-switch>
      <TimezoneDatePicker
        v-if="config.type == 'time'"
        :id="parent + field"
        v-model="data[field]"
        :disabled="disabled()"
        :placeholder="config.placeholder"
        :type="config.dateType"
        style="width: 100%"
      ></TimezoneDatePicker>
      <UploadCsv
        v-if="config.type == 'file'"
        v-model="data[field]"
        :config="config"
      >
      </UploadCsv>
      <Dataset
        v-if="config.type == 'dataset'"
        :config="config"
        :data="data"
        v-model="data[field]"
      />
      <PibackfillTime
        v-if="config.type == 'pibackfillTime'"
        :config="config"
        :data="data"
      />
      <Bucket
        v-if="config.type == 'bucket'"
        ref="bucket"
        :config="config"
        :data="data"
        :parentConfigList="parentConfigList"
      />
      <Mode
        v-if="config.type == 'mode'"
        ref="mode"
        :config="config"
        :data="data"
        :parentConfigList="parentConfigList"
      />
      <PatternComp
        v-if="config.type == 'pattern'"
        ref="pattern"
        :config="config"
        :data="data"
        :parentConfigList="parentConfigList"
      /> -->
      <div v-if="config.info" slot="label">
        {{ config.label }}
        <el-tooltip class="item" effect="light" placement="top" :open-delay="0">
          <div
            v-dompurify-html="parseMarked(config.description)"
            slot="content"
          ></div>
          <el-icon
            style="margin-left: 5px; cursor: pointer"
            class="el-icon-info"
          ></el-icon>
        </el-tooltip>
      </div>
      <!-- <DocsContent
        v-if="dataSetDocsShow"
        :style="docsStyle"
        :class="config.templateUrl ? 'noboder' : ''"
        :content="config.description"
      /> -->
    </div>
    <template v-else-if="nolabel">
      <DocsContent
        v-if="doscShow"
        :style="docsStyle"
        :content="config.description"
      />
      <!-- <TabFormItem
        v-if="config.type == 'tab'"
        :config="config"
        :disabled="disabled()"
        :data="data"
      /> -->
      <!-- <OpcTable
        v-if="config.type == 'opcTable'"
        :data="data[field]"
        :parent="parent + field + '.'"
        v-bind="config"
      /> -->
    </template>
  </div>
</template>

<script>
import { hasOwn } from "@/utils/util";
import { marked } from "marked";
import { parsinginZone } from "@/utils/index";
import { TimeFormats, getGroupsObj, getFieldClassMarkName, handleDownload } from "../utils";

export default {
  props: {
    config: {
      type: Object,
      default: () => {},
    },
    data: {
      type: Object,
      default: () => {},
    },
    parent: {
      type: String,
      default: "",
    },
    parentConfigList: {
      type: Array,
      default: () => [],
    },
  },
  name: "DescItem",
  inject: ["sourceParent"],
  components: {
    DocsContent: () => import("@/views/support/components/editorContentDisplay.vue"),
    UploadCsv: () => import("./uploadCsv.vue"),
    Dataset: () => import("./dataset.vue"),
    TimezoneDatePicker: () => import("@/components/date-picker"),
    PibackfillTime: () => import("./pibackfillTime.vue"),
    Bucket: () => import("./bucket.vue"),
    Mode: () => import("./mode.vue"),
    PatternComp: () => import("./pattern.vue")
  },
  data() {
    this.inputType = ["input", "textarea", "password"];
    return {
      noLabelType: ["tab", "opcTable"],
      files: [],
      selectOptions: [],
      date1: 0,
      date2: 0,
    };
  },
  computed: {
    field() {
      return this.config.valueField || this.config.field;
    },
    labelWidth() {
      return this.config.labelWidth || "";
    },
    labelText() {
      return this.config.labelShow !== false ? this.config.label : "";
    },
    nolabel() {
      return !this.config.type || this.noLabelType.includes(this.config.type);
    },
    display() {
      if (this.nolabel) return false;
      if (this.config.type == 'switch' && this.config.hasValue) return false;
      if (!hasOwn(this.config, "if")) return true;
      if (typeof this.config.if === "function") {
        return this.config.if(this.data, this.sourceParent.sourceForm.data);
      }
      return this.config.if;
    },
    doscShow() {
      return this.config.description && !this.config.info;
    },
    dataSetDocsShow() {
      return this.config.info2;
    },
    docsStyle() {
      const isTab = this.config.type == "tab";
      const marginKey = isTab ? "marginBottom" : "marginTop";
      return {
        [marginKey]: "5px",
      };
    },
    meta() {
      return this.config.meta || {};
    },

    isEdit() {
      return this.sourceParent.isEditable;
    },
    isCopyable() {
      return this.sourceParent.isCopyable;
    },
    timeFormats() {
      return TimeFormats;
    },
    classMark() {
      return getFieldClassMarkName(this.parent + this.field);
    },
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    parseMarked(desc) {
      return marked.parse(desc);
    },
    changeSwith() {
      if (this.config.field === "use_csv_config") {
        this.$emit("csv-enable", this.data[this.field]);
      }
    },
    getOptions(val, multiple) {
      let result = []
      console.log('thiuss',this.config.options);
      if (typeof this.config.options === "function") {
        result = this.config.options(this).filter(item => {
          if (val.includes(item.value)) {
            result.push(item.label)
          }
        })
        return result.join();
      }
      result = this.config.options.filter(item => {
        if (val.includes(item.value)) {
          result.push(item.label)
        }
      })
      return result.join();
    },
    getFile(val) {
      return val?.substr(val.lastIndexOf("/") + 1)
    },
    handleDownloadFile(val) {
      if (val) {
        let name = this.getFile(val)
        let path = val?.split('@')[1]
        handleDownload(path, name)
      }
    }
  },
};
</script>

<style scoped lang="scss">
:deep(.markdown-body) {
  p {
    font-size: 14px;
  }
  color: $color-description;
}
.ds-select {
  width: 100%;
}

.noboder {
  ::v-deep p {
    margin-bottom: 5px;
  }
  ::v-deep table {
    border: none;
    font-size: 12px;
    th,
    tr,
    td {
      padding: 0px;
      border: none;
      background-color: unset;
    }
  }
}
.descItem {
  padding: 0 5px 10px 0;
}
.grid-item-span-two {
  grid-column: 1 / 3; /* 使该元素跨越两列 */
}
</style>
