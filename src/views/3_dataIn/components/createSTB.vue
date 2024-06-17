<template>
  <div class="create-stb">
    <el-form :model="stable_form" :rules="rules"  ref="form" label-position="left" label-width="150px">
      <el-form-item prop="name" class="name_input">
        <template slot="label">
          <span>{{ $t("name") }}</span>
          <el-tooltip
            class="item"
            effect="light"
            :content="$t('data.tableNameTip')"
            placement="top-start"
          >
            <el-icon style="margin-left: 10px" class="el-icon-info"></el-icon>
          </el-tooltip>
        </template>
        <el-input
          size="small"
          :maxlength="192"
          :title="stable_form.name"
          v-model="stable_form.name"
        >
        </el-input>
      </el-form-item>
    </el-form>
    <el-collapse v-model="activeNames" @change="handleChange">
      <el-collapse-item name="1" :title="$t('data.columns')">
        <!-- <el-input
          :placeholder="$t('data.columnNameTip')"
          v-model="stable_form.ts_field_name"
          size="small"
        >
        <div slot="prepend">TIMESTAMP</div>
        </el-input> -->
        <div
          class="flexCenter input_row"
          v-for="(column, index) in stable_form.columns"
          :key="'column' + index"
        >
          <el-select
            v-model="column.type"
            size="small"
            default-first-option
            :placeholder="$t('Data') + $t('type')"
            :disabled="index == 0"
            class="columnPrependBtn"
            @change="() => handleTypeChange(column, index)"
          >
            <el-option
              v-for="item in handleTypeList(column.type, 'dataType')"
              :key="item.value"
              v-bind="item"
            ></el-option>
          </el-select>
          <el-input-number
            size="small"
            v-if="VariableTableColumnType.includes(column.type)"
            :value="column.length
            "
            @change="
              (newVal, oldVal) =>
                handleChange(newVal, oldVal, column.type, index)
            "
            :min="1"
            :max="column.type == 'NCHAR' ? 4093 : 65517"
            label="Length"
            controls-position="right"
            class="custom-length"
          ></el-input-number>
          <el-input
              size="small"
              v-model="column.field"
              :maxlength="64"
              :placeholder="$t('data.columnNameTip')"
              style="min-width: 60px"
            >
          </el-input>
          <el-tag effect="plain" type="info" v-if="index==1 && version_gt_3300">
              <el-checkbox 
                v-model="column.primaryKey"  
                :disabled="parmaryKeyType.findIndex((item) => column.type.startsWith(item.value)) == -1"
                >PRIMARY KEY</el-checkbox>
            </el-tag>
            <el-tooltip
              placement="top" effect="light" :open-delay="100"
              :content="$t('console.encode')" v-if="version_gt_3300">
              <el-select
                size="small"
                default-first-option
                defaultValue="simple8b"
                v-model="column.encode"
                placeholder="ENCODE"
                class="columnWidth120"
                clearable
              >
                <el-option
                  v-for="item in handleEncodeList(column.type)['encodeList']"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-tooltip>
            <el-tooltip
              placement="top" effect="light" :open-delay="100"
              :content="$t('console.compress')" v-if="version_gt_3300">
              <el-select
                size="small"
                default-first-option
                defaultValue="lz4"
                v-model="column.compress"
                placeholder="COMPRESS"
                class="columnWidth120"
                clearable
              >
                <el-option
                  v-for="item in handleEncodeList(column.type)['compressList']"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-tooltip>
            <el-tooltip
              placement="top" effect="light" :open-delay="100"
              :content="$t('console.level')" v-if="version_gt_3300">
              <el-select
                size="small"
                default-first-option
                v-model="column.level"
                placeholder="LEVEL"
                class="columnWidth120"
                clearable
              >
                <el-option
                  v-for="item in levelList"
                  :key="item.value"
                  v-bind="item"
                ></el-option> 
              </el-select>
            </el-tooltip>
        <span class="action-btn">
          <el-button
            icon="el-icon-minus"
            size="small"
            :disabled="!index"
            @click="minusColumn(index)"
          ></el-button>
          <el-button @click="addColumn" icon="el-icon-plus" size="small"></el-button>
          <el-tooltip
            :content="$t('data.clickColumnTip')"
          >
          <el-button @click="removeToTag(index)" size="small" :disabled="!index">
            <Icon
              :name="'tag'"
              class="console-tree-icon"
              style="width: 18px; height: 18px"
            ></Icon>
          </el-button>
          </el-tooltip>
        </span>
        </div>
      </el-collapse-item>
      <el-collapse-item name="2" :title="$t('tags')">
        <div
          class="flexCenter input_row"
          v-for="(column, index) in stable_form.tags"
          :key="'column' + index"
        >
          <el-select
            v-model="column.type"
            size="small"
            default-first-option
            :placeholder="$t('Data') + $t('type')"
            class="columnPrependBtn"
          >
            <el-option
              v-for="item in tagType"
              :key="item.value"
              v-bind="item"
            ></el-option>
          </el-select>
          <el-input-number
            size="small"
            v-if="VariableTableColumnType.includes(column.type)"
            :value="column.length"
            @change="
              (newVal, oldVal) =>
                tagLengthChange(newVal, oldVal, column.type, index)
            "
            :min="1"
            :max="column.type == 'NCHAR' ? 4093 : 16382"
            label="Length"
            controls-position="right"
            class="custom-length"
          ></el-input-number>
          <el-input
            size="small"
            v-model="column.field"
            :maxlength="64"
            :placeholder="$t('data.tagNameTip')"
          >
            <template slot="append">
              <el-button
                icon="el-icon-minus"
                @click="minusTags(index)"
              ></el-button>
              <el-button @click="addTags" icon="el-icon-plus"></el-button>
            </template>
          </el-input>
        </div>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>
<script>
import { deepClone } from "@/utils";

import {
  dataType,
  tagType,
  parmaryKeyType, storageCompression, levelList, groupOne, groupTwo, groupThree, groupFour, groupFive
} from "../../2_explorer/views/components/utils/index";
import { VariableTableColumnType } from "@/const"
import VersionMixin from "@/mixins/version";
export default {
  name: "CreateSTB",
  data() {
    this.parmaryKeyType = parmaryKeyType;
    this.storageCompression = storageCompression;
    this.levelList = levelList;
    return {
      dataType,
      tagType,
      column_item: {
        type: "INT",
        field: "",
        value: "",
        length: 8,
        encode: "simple8b", 
        compress: "lz4", 
        level: "medium",
      },
      column_item_ts:{ 
        type: "TIMESTAMP", 
        field: "", 
        value: "",
        length:8, 
        encode: "delta-i", 
        compress: "lz4", 
        level: "medium", 
        primaryKey: false 
      },

      stable_form: {
        name: "",
        ts_field_name: "",
        rollup: "",
        columns: [],
        tags: [],
      },
      rules: {
        name: [
          {
            required: true,
            message: this.$t("data.nameTip").replace(
              "/name/",
              this.$t("dashboard.stables")
            ),
            trigger: "blur",
          },
          {
            validator: (_, value, callback) => {
              callback(
                value.indexOf(".") != -1
                  ? new Error(this.$t("formatWrong"))
                  : undefined
              );
            },
            trigger: "blur",
          },
        ],
      },
      activeNames: ["1", "2"],
      VariableTableColumnType
    };
  },
  props: {
    columnsArr: {
      type: Array,
      default: () => [],
    }
  },
  mixins: [VersionMixin],
  watch: {
    "$store.state.app.stbDefaultColumns": {
      handler(columnsArr_new) {
        if (columnsArr_new.length > 0) {
          let arr = columnsArr_new;
          arr = arr.map(item => {
            let type = item.localType.toUpperCase()
            type = type.startsWith('TIMESTAMP') ? type.split('(')[0] : type
            return {
              field: item.name,
              type: type,
              encode: this.handleEncodeList(type)['defaultEncode'],
              compress: this.handleEncodeList(type)['defaultCompress'],
              level: 'medium'
            }
          })
          arr.unshift(deepClone(this.column_item_ts))
          this.stable_form.columns = arr;
          this.$set(this.stable_form.tags, 0, deepClone(this.column_item));
        } else {
          this.$set(this.stable_form.columns, 0, deepClone(this.column_item_ts));
          this.$set(this.stable_form.columns, 1, deepClone(this.column_item));
          this.$set(this.stable_form.tags, 0, deepClone(this.column_item));
        }
      },
      immediate: true,
      deep: true,
    }
  },
  mounted() {
  },
  methods: {
    handleChange(newVal, oldVal, type, index) {
      this.$set(this.stable_form.columns[index], "length", newVal);
    },
    tagLengthChange(newVal, oldVal, type, index) {
      this.$set(this.stable_form.tags[index], "length", newVal);
    },
    minusColumn(index) {
      if (this.stable_form.columns.length > 1) {
        this.stable_form.columns.splice(index, 1);
      }
      // 是主键列
      if (index == 1) {
        this.handPrimarykeyCol(index)
      }
    },
    minusTags(index) {
      if (this.stable_form.tags.length > 1) {
        this.stable_form.tags.splice(index, 1);
      }
    },
    typeChange() {},
    addTags() {
      this.stable_form.tags.push(deepClone(this.column_item));
    },
    addColumn() {
      this.stable_form.columns.push(deepClone(this.column_item));
    },
    removeToTag(index) {
      if (this.stable_form.columns.length > 1) {
        let column = this.stable_form.columns.splice(index, 1)[0];
        this.stable_form.tags.push(deepClone(column));
      }
      // 是主键列
      if (index == 1) {
        this.handPrimarykeyCol(index)
      }
    },
    handPrimarykeyCol(index) {
      this.$set(this.stable_form.columns[index], "primaryKey", false);
    },
    handleEncodeList(type) {
      if (!type) return this.storageCompression.empty
      if (groupOne.includes(type)) {
        return this.storageCompression.groupOne
      } else if (groupTwo.includes(type)) {
        return this.storageCompression.groupTwo
      } else if (groupThree.includes(type)) {
        return this.storageCompression.groupThree
      } else if (groupFour.findIndex((item) => type.startsWith(item)) !== -1) {
        return this.storageCompression.groupFour
      } else if (groupFive.includes(type)) {
        return this.storageCompression.groupFive
      } else {
        return this.storageCompression.groupSix
      }
    },
    handleTypeChange(column, index) {
      const data = this.handleEncodeList(column.type)
      const { defaultEncode, defaultCompress } = data
      this.$set(this.stable_form.columns[index], "encode", defaultEncode);
      this.$set(this.stable_form.columns[index], "compress", defaultCompress);
      this.$set(this.stable_form.columns[index], "level", 'medium');
      // 如果不支持 primary key 
      if (index == 1 && 
        column.primaryKey && 
        this.parmaryKeyType.findIndex((item) => column.type.startsWith(item.value)) == -1) 
      {
        this.$set(this.stable_form.columns[index], "primaryKey", false);
      }
    },
    handleTypeList(currentType, name) {
      return this[name];
    },
  },
};
</script>
<style lang="scss" scoped>
.input_row {
  margin-top: 18px;
}
.create-stb ::v-deep {
  .el-collapse {
    border-top: 0;
  }
  .el-form-item__content {
    display: flex;
  }
  .el-collapse-item__header {
    font-size:18px;
    border-bottom: none !important;
  }
  .el-collapse-item__wrap {
    border-bottom: none !important;
  }
}
.columnPrependBtn {
  width: 150px;
  flex-shrink: 0;
}
.custom-length {
  width: 110px;
  flex-shrink: 0;
  ::v-deep {
    .el-input-number__decrease {
      height: 16px;
    }
    .el-input-number__increase {
      height: 16px;
    }
    .el-input {
      .el-input__inner {
        height: 32px !important;
      }
    }
  }
}
.create-stb ::v-deep .el-input.is-disabled .el-input__inner,
.create-stb ::v-deep .el-input-group__append,
.create-stb ::v-deep .el-input-group__prepend {
  background-color: unset;
  color: #606266;
  .el-button.is-disabled,
  .el-button.is-disabled:hover,
  .el-button.is-disabled:focus {
    background-color: transparent;
    border-color: transparent;
  }
}
.create-stb ::v-deep .el-input-group__prepend {
    width: 150px;
    padding-left: 15px;
  }
.create-stb ::v-deep .flexCenter .el-select .el-input__inner {
  border-color: #dcdfe6;
  border-left: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
.create-stb ::v-deep .flexCenter .el-input .el-input__inner {
  border-color: #dcdfe6;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
}

.create-stb ::v-deep .flexCenter .el-select:first-of-type .el-input__inner  {
  border-left: 1px solid #dcdfe6;
  border-right: none;
}

.columnWidth120 {
  width: 110px;
  flex-shrink: 0;
}

.create-stb ::v-deep .flexCenter .action-btn {
  display: flex;
  margin-left: 10px;
  .el-button + .el-button {
    margin-left: 0px;
    border-left-style: none;
  }
}

.create-stb ::v-deep .el-tag {
  border-left: none;
}
</style>
