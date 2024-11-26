<template>
  <div v-if="config && data && displayIf" :class="[(config.children && !config.hasValue) ? 'descriptions' : '', {'grid-item-span-two': config.grid_two}]">
    <template v-if="config.children && !config.hasValue">
      <DescItem
        v-for="item in config.children"
        :key="item.label"
        :config="item"
        :data="data[field]"
        :parent="parent + field + '.'"
      />
    </template>
    <div
      v-else-if="display && (labelText || config.type !== 'input')"
      :class="[classMark, 'descItem', {'grid-item-span-two': config.grid_two}]"
      :prop="parent + field"
    >
      <template>
        <span v-if="config.type !== 'dataset'" style="padding-right: 10px">{{ labelText }}:</span>
        <span v-if="!inputType.includes(config.type)">{{ field == 'group' ? (data['group'] || this.taskId) : data[field] }}</span>
        <span v-if="config.type == 'select'">
          <span v-if="Array.isArray(getOptions())" class="flexWrap">
            <span v-for="(option,index) in getOptions()" :key="option">{{ option }}
              <span v-if="getOptions().length > 1 && index != getOptions().length -1">,</span>
            </span>
          </span>
        </span>
        <a v-if="config.type == 'file' || config.type == 'dataset'" @click="handleDownloadFile(data[field])">{{ getFile(data[field]) }}</a>
        <span v-if="config.type == 'composeAppend'">{{ data[field] ? data[field] + data[field + '_type'] : ''}}</span>
        <span v-if="config.type == 'password'">{{ data[field]? '****': ''}}</span>
      </template>
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
    </div>
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
  },
  data() {
    this.inputType = ["select", "file", "dataset", "composeAppend", "password"];
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
    displayIf() {
      if (!hasOwn(this.config, "if")) return true;
      if (typeof this.config.if === "function") {
        return this.config.if(this.data, this.sourceParent.sourceForm.data);
      }
      return this.config.if;
    },
    classMark() {
      return getFieldClassMarkName(this.parent + this.field);
    },
    taskId() {
      return this.sourceParent.editId
    }
  },

  methods: {
    parseMarked(desc) {
      return marked.parse(desc);
    },
    changeSwith() {
      if (this.config.field === "use_csv_config") {
        this.$emit("csv-enable", this.data[this.field]);
      }
    },
    getOptions() {
      let result = []
      const val = [].concat(this.data[this.field])
      const options = typeof this.config.options === "function" 
        ? this.config.options(this) 
        : this.config.options;

      if (options.length > 0) {
        const labels = val.map(id => {
          const matchedObject = options.find(obj => obj.value === id);
          if (matchedObject) {
            result.push(matchedObject.label)
          }
        });
      } else {
        result = val;
      }
      return result;
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
  > span {
    display: inline-block;
  }
}
.grid-item-span-two {
  grid-column: 1 / 3; /* 使该元素跨越两列 */
}
.flexWrap {
  display: flex;
  flex-wrap: wrap;
}
</style>
