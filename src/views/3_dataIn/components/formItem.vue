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
    <el-form-item
      v-else-if="display"
      :label="labelText"
      :label-width="labelWidth"
      :required="required()"
      :rules="timeFormats.includes(field) ? [...timeRules,...rules] : rules"
      :prop="parent + field"
    >
      <el-input
        v-if="inputType.includes(config.type)"
        v-model="data[field]"
        :disabled="disabled()"
        :type="config.type"
        :show-password="config.type == 'password'"
        :placeholder="config.placeholder"
      ></el-input>
      <el-input-number
        v-if="config.type == 'number'"
        v-model="data[field]"
        :disabled="disabled()"
        :max="config.max"
        :min="config.min"
        :placeholder="config.placeholder"
      ></el-input-number>
      <el-select
        v-if="config.type == 'select'"
        v-model="data[field]"
        v-bind="meta"
        class="ds-select"
        clearable
        :disabled="disabled()"
        :placeholder="config.placeholder"
      >
        <el-option
          v-for="item in getOptions()"
          :key="item.value"
          v-bind="item"
          :title="item.description"
        ></el-option>
      </el-select>
      <el-switch
        v-if="config.type == 'switch'"
        v-model="data[field]"
        :disabled="disabled()"
        :placeholder="config.placeholder"
        @change="changeSwith"
      ></el-switch>
      <TimezoneDatePicker
        v-if="config.type == 'time'"
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
      <!-- <Dataset
        v-if="config.type == 'dataset'"
        :config="config"
        :data="data"
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
      /> -->
      <div
        v-if="config.info"
        slot="label"
        >{{ config.label }}
        <el-tooltip
          class="item"
          effect="light"
          placement="top"
        >
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
        v-if="doscShow"
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
      <TabFormItem
        v-if="config.type == 'tab'"
        :config="config"
        :disabled="disabled()"
        :data="data"
      />
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
import { hasOwn, marked } from '@/utils/util';
import { parsinginZone } from "@/utils/index";
import { TimeFormats, getGroupsObj } from '../utils'

export default {
  props: {
    config: {
      type: Object,
      default: () => {}
    },
    data: {
      type: Object,
      default: () => {}
    },
    parent: {
      type: String,
      default: ''
    },
    parentConfigList: {
      type: Array,
      default: () => []
    },
    isSubmit: {
      type: Boolean,
      default: true
    }
  },
  name: 'FormItem',
  inject: ['sourceParent'],
  components: {
    DocsContent: () => import('@/views/support/components/editorContentDisplay.vue'),
    // TabFormItem: () => import('../components/tabFormItem.vue'),
    // OpcTable: () => import('./opcTable.vue'),
    // UploadCsv: () => import('./uploadCsv.vue'),
    // Dataset: () => import('./dataset.vue'),
    TimezoneDatePicker: () => import('@/components/date-picker'),
    // PibackfillTime: () => import('./pibackfillTime.vue'),
    // Bucket: () => import('./bucket.vue')
  },
  data() {
    this.inputType = ['input', 'textarea', 'password'];
    return {
      noLabelType: ['tab', 'opcTable'],
      files: [],
      selectOptions: [],
      date1: 0,
      date2: 0
    };
  },
  computed: {
    field() {
      return this.config.valueField || this.config.field;
    },
    labelWidth() {
      return this.config.labelWidth || '';
    },
    labelText() {
      return this.config.labelShow !== false ? this.config.label : '';
    },
    nolabel() {
      return !this.config.type || this.noLabelType.includes(this.config.type);
    },
    display() {
      if (this.nolabel) return false;
      if (!hasOwn(this.config, 'if')) return true;
      if (typeof this.config.if === 'function') return this.config.if(this.data, this.sourceParent.sourceForm.data);
      return this.config.if;
    },
    doscShow() {
      return this.config.description && !this.config.info;
    },
    docsStyle() {
      const isTab = this.config.type == 'tab';
      const marginKey = isTab ? 'marginBottom' : 'marginTop';
      return {
        [marginKey]: '5px'
      };
    },
    rules() {
      if (typeof this.config.required === 'function') return this.config.required(this.isSubmit) ? [{ required: true, message: this.$t('required', [this.config.label ?? this.config.field]) }] : [];
      return this.config.required ? [{ required: true, message: this.$t('required', [this.config.label ?? this.config.field]) }] : [];
    },
    timeRules() {
      return [{ validator: this.compareTime, trigger: "blur", }]
    },
    meta() {
      return this.config.meta || {};
    },

    isEdit() {
      return this.sourceParent.isEdit;
    },
    timeFormats() {
      return TimeFormats
    }
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    disabled() {
      if (!hasOwn(this.config, 'disabled')) return false;
      if (typeof this.config.disabled === 'function') return this.config.disabled(this.data, this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition);
      return this.config.disabled;
    },
    parseMarked(desc) {
      return marked.parse(desc);
    },
    required() {
      if (typeof this.config.required === 'function') return this.config.required(this.isSubmit);
      return this.config.required;
    },
    changeSwith() {
      if (this.config.field === 'use_csv_config') {
        this.$emit('csv-enable', this.data[this.field]);
      }
    },
    getOptions() {
      if (typeof this.config.options === 'function') return this.config.options(this);
      return this.config.options;
    },
    compareTime(info, value, callback) {
      let groupsData = getGroupsObj(this.sourceParent.sourceForm.data)
      this.date1 = new Date(groupsData?.beginDateTime) ?? 0
      this.date2 = new Date(groupsData?.endDateTime) ?? 0
      if (this.date1 && this.date2 && this.date1 > this.date2) {
        return callback(new Error(this.$t('dataOut.startTime') + ' > ' + this.$t('dataOut.endTime')));
      } else {
        callback()
      }
      },
  }
};
</script>

<style scoped lang="scss">
:deep(.markdown-body) {
  p {
    font-size: 14px;
  }
  color: #a6adbc;
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
</style>
