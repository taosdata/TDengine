<template>
  <div v-if="config && data">
    <template v-if="config.children && !config.hasValue">
      <FormItem
        v-for="item in config.children"
        :key="item.label"
        :config="item"
        :data="data[field]"
        :parent="parent + field + '.'"
      />
    </template>
    <!-- 排除tmq中 dsn 自己带的参数，这类 param 特点就是 type=input,没有label  -->
    <el-form-item
      v-else-if="display && (labelText || config.type !== 'input')"
      :label="labelText"
      :label-width="labelWidth"
      :required="required(config)"
      :class="[classMark, {'hidden-required': !required(config)}]"
      :rules="timeFormats.includes(field) ? [...timeRules, ...rules] : rules"
      :prop="parent + field"
    >
      <template slot="label">
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
      </template>
      <el-input
        v-if="inputType.includes(config.type)"
        :id="parent + field"
        v-model="data[field]"
        :disabled="disabled()"
        :type="config.type"
        :show-password="config.type == 'password'"
        :placeholder="config.placeholder"
        @blur="trimInput"
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

      <div v-if="config.type == 'compose'">
        <el-input  :placeholder="config.placeholder" v-model="data[field]" class="input-with-select" @blur="trimInput">
          <el-select style="width: 120px;" v-model="data[field + '_type']" slot="prepend">
            <el-option v-for="item in getOptions()"
              :key="item.value"
              v-bind="item"
              :title="item.description"
              :disabled="item.disabled"
            ></el-option>
          </el-select>
        </el-input>
        <el-button v-if="config.action"
          v-loading.fullscreen.lock="fullscreenLoading"
          plain
          style="margin-top: 10px;"
          type="primary" 
          @click="submitAction"
          :icon="`el-icon-${config.action}`">
          {{ config.action_text }}
        </el-button>
      </div>
      <div v-if="config.type == 'composeAppend'" class="input-number-with-select">
        <el-input-number 
          style="width: 80%" 
          :placeholder="config.placeholder" 
          v-model="data[field]"
          :max="config.max"
          :min="config.min"
          :controls="false">
        </el-input-number>
        <el-select style="width: 20%;" v-model="data[field + '_type']" :placeholder="$t('timeUnit')">
          <el-option v-for="item in getOptions()"
            :key="item.value"
            v-bind="item"
            :title="item.label"
          ></el-option>
        </el-select>
      </div>

      <UploadCsv
        v-if="config.type == 'file'"
        v-model="data[field]"
        :config="config"
        :btnText="config.btnText"
        :disabled="disabled()"
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
      />
      <CustomId
        v-if="config.type == 'customId'"
        ref="pattern"
        :config="config"
        :data="data"
        :parentConfigList="parentConfigList"
      />
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
      <DocsContent
        v-if="dataSetDocsShow"
        :style="docsStyle"
        :class="config.templateUrl ? 'noboder' : ''"
        :content="config.description"
      />
    </el-form-item>
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
import { TimeFormats, getGroupsObj, getFieldClassMarkName, getDsnData, getActiveTabValueObject, getActiveTabKey, handleDownload, checkJson } from "../utils";
import {
  generatePIDefaultConfigFile,
} from "@/api/explorer/datain";
const fnMap = {
  "checkJson": checkJson
}
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
  name: "FormItem",
  inject: ["sourceParent"],
  components: {
    DocsContent: () => import("@/views/support/components/editorContentDisplay.vue"),
    UploadCsv: () => import("./uploadCsv.vue"),
    Dataset: () => import("./dataset.vue"),
    TimezoneDatePicker: () => import("@/components/date-picker"),
    PibackfillTime: () => import("./pibackfillTime.vue"),
    Bucket: () => import("./bucket.vue"),
    Mode: () => import("./mode.vue"),
    PatternComp: () => import("./pattern.vue"),
    CustomId: () => import("./customId.vue")
  },
  data() {
    this.inputType = ["input", "textarea", "password"];
    return {
      noLabelType: ["tab", "opcTable"],
      files: [],
      selectOptions: [],
      date1: 0,
      date2: 0,
      fullscreenLoading: false,
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
      if (!hasOwn(this.config, "if")) return true;
      if (typeof this.config.if === "function") {
        return this.config.if(this.data, this.sourceParent.sourceForm.data);
      }
      if (typeof this.config.if === "string") {
        if (this.config.if.startsWith("!")) {
          return !this.data[this.config.if.substring(1)];
        } else {
          return this.data[this.config.if];
        }
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
    rules() {
     const requireRule = [
        {
          required: true,
          message: this.$t("required", [
            this.config.label ?? this.config.field,
          ]),
        },
      ]

      const patternRule = [
        {
          pattern: this.config.pattern,
          message: this.config.patternMsg,
          trigger: 'blur',
        }
      ]
      const validatorRule = [
        { 
          validator: fnMap[this.config.validator], 
          trigger: "blur" 
        }
      ]
      let rules = []
      if (this.config.pattern) {
        rules = rules.concat(patternRule)
      }

      if (this.config.validator) {
        rules = rules.concat(validatorRule)
      }

      if (typeof this.config.required === "function") {
        return this.config.required(
          this.data,
          this.sourceParent.sourceForm.data,
          this.sourceParent.currentDefinition,
          this.isEdit && !this.isCopyable
        )
          ? rules.concat(requireRule)
          : rules;
      }
      return this.config.required
        ? rules.concat(requireRule)
        : rules;
    },
    timeRules() {
      return [{ validator: this.compareTime, trigger: "blur" }];
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
    disabled() {
      if (!hasOwn(this.config, "disabled")) return false;
      if (typeof this.config.disabled === "function") {
        return this.config.disabled(
          this.data,
          this.sourceParent.sourceForm.data,
          this.sourceParent.currentDefinition,
          this.isEdit && !this.isCopyable
        );
      }
      if (typeof this.config.disabled === "string") {
        if (this.config.disabled.startsWith("!")) {
          return !this.data[this.config.disabled.substring(1)];
        } else {
          return this.data[this.config.disabled];
        }
      } 
      return this.config.disabled;
    },
    parseMarked(desc) {
      return marked.parse(desc);
    },
    required() {
      if (typeof this.config.required === "function") {
        return this.config.required(
          this.data,
          this.sourceParent.sourceForm.data,
          this.sourceParent.currentDefinition,
          this.isEdit && !this.isCopyable
        );
      }
      return this.config.required;
    },
    changeSwith() {
      if (this.config.field === "use_csv_config") {
        this.$emit("csv-enable", this.data[this.field]);
      }
    },
    getOptions() {
      if (typeof this.config.options === "function") {
        return this.config.options(this);
      }
      return this.config.options;
    },
    compareTime(info, value, callback) {
      const type = this.sourceParent.sourceForm.type;
      let groupsData = getGroupsObj(this.sourceParent.sourceForm.data);
      switch (type) {
        case "taos":
        case "postgres":
        case "mysql":
        case "oracle":
        case "mssql":
        case "mongodb":
          this.date1 = groupsData?.start ? new Date(groupsData?.start) : 0;
          this.date2 = groupsData?.end ? new Date(groupsData?.end) : 0;
          break;
        case "avevaHistorian":
          this.date1 = groupsData?.beginDateTime ? new Date(groupsData?.beginDateTime) : 0;
          this.date2 = groupsData?.endDateTime ? new Date(groupsData?.endDateTime) : 0;
          break;
        case "influxdb":
        case "opentsdb":
          this.date1 = groupsData?.beginTime ? new Date(groupsData?.beginTime) : 0;
          this.date2 = groupsData?.endTime ? new Date(groupsData?.endTime) : 0;
          break;
        case "pibackfill": 
          this.date1 = groupsData?.BackfillStartTime ? new Date(groupsData?.BackfillStartTime) : 0;
          this.date2 = groupsData?.BackfillEndTime ? new Date(groupsData?.BackfillEndTime) : 0;
          break;
        default:
          break;
      }
      if (this.date1 && this.date2 && this.date1 > this.date2) {
        return callback(new Error(this.$t("dataIn.timeTip")));
      } else {
        callback();
      }
    },

    async downloadPIDefaultConfigFile() {
      const sourceForm = this.sourceParent.sourceForm;
      let type = sourceForm.type
      let via = sourceForm.agent
      let filter_params = getActiveTabValueObject(sourceForm.data)
      if (!filter_params.filter_value_type) {
        filter_params.filter_value_type = 'template'
      }
      delete filter_params['filter_point'];
      delete filter_params['filter_element'];
      delete filter_params['filter_template'];
      filter_params[`filter_${filter_params.filter_value_type}`] = filter_params.filter_value
      filter_params['model'] = getActiveTabKey(sourceForm.data)

      const url = type + getDsnData(sourceForm.data, this.sourceParent.currentDefinition);
      
      try {
        this.fullscreenLoading = true;
        let defaultFileInfo = await generatePIDefaultConfigFile(url, null, via);
        console.log("defaultFileInfo", defaultFileInfo)
        if (typeof defaultFileInfo !== 'string') {
          if (defaultFileInfo && defaultFileInfo.message) {
            this.$error(defaultFileInfo.message);
          } else {
            this.$error('Failed to generate default config file');
          }
          return;
        }

        handleDownload(defaultFileInfo);
        this.fullscreenLoading = false;

        if (filter_params.transform_config_file) {
          // 已经有配置文件情况下, 则询问是否覆盖
          this.$confirm(this.$t('datasource.pi.confirmOverwriteConfigFile'), this.$t('tips'), {
            confirmButtonText: this.$t('yes'),
            cancelButtonText: this.$t('no'),
            type: 'warning'
          }).then(() => {
            this.$eventBus.$emit("updatePIDefaultConfigFile", defaultFileInfo);
          });
        } else {
          // 如果当前使用默认配置，但是没有配置文件，则直接更新
          this.$eventBus.$emit("updatePIDefaultConfigFile", defaultFileInfo);
        }
      } finally {
        this.fullscreenLoading = false;
      }
    },

    submitAction() {
      const sourceForm = this.sourceParent.sourceForm;
      let type = sourceForm.type

      // 目前只有 pi 数据源的 config.action=download 的按钮用到这个方法，不同的数据源需要不同的处理
      if (['pi', 'pibackfill'].indexOf(type) >= 0 && this.config.action === 'download') {
        this.downloadPIDefaultConfigFile();
      } else {
        console.log('not support, please add your own logic here.')
      }

    },
    trimInput() {
      // 在失去焦点时去除输入框值的前后空格
      this.data[this.field] = this.data[this.field].toString().trim();
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
.input-with-select {
  ::v-deep .el-input-group__prepend {
    background-color: #fff;
    border-color: #bebcbc;
    .el-input__inner {
      box-shadow: none;
    }
  }
  ::v-deep .el-input-group__append {
    background-color: #fff;
    border-color: #bebcbc;
    .el-input__inner {
      box-shadow: none;
    }
  }
}
.input-number-with-select ::v-deep .el-input-number .el-input__inner {
  text-align: left;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  box-shadow: none;
  box-shadow: inset 1px 0 0 0 #bebcbc, /* 左边 */
              inset 0 -1px 0 0 #bebcbc, /* 上边 */
              inset 0 1px 0 0 #bebcbc; /* 下边 */

}
.input-number-with-select ::v-deep .el-select .el-input__inner {
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-left: none;
}
</style>
