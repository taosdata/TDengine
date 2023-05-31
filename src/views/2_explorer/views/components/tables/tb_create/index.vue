<template>
  <div class="tbCreate">
    <div class="form_title">{{ formTitle }}</div>
    <div class="formWrapper">
      <el-form
        ref="table_form"
        label-position="left"
        :rules="rules"
        :label-width="table_form.stbTmpl ? '170px' : '130px'"
        :model="table_form"
      >
        <!-- Name -->
        <el-form-item prop="name" class="name_input">
          <template slot="label">
            <span>{{ $t("name") }}</span>
            <el-tooltip
              v-if="!isEdit"
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
            maxlength="192"
            :title="table_form.name"
            :disabled="isEdit"
            v-model="table_form.name"
          />
        </el-form-item>
        <template v-if="!table_form.stbTmpl">
          <!-- Column Section -->
          <div class="section_title">
            <span class="sectionTitle_text">{{ $t("data.columns") }}</span>
            <div class="foldIcon" @click="foldColumns">
              <i class="el-icon-arrow-right" v-if="isColumnsFold"></i>
              <i class="el-icon-arrow-down" v-else></i>
            </div>
          </div>
          <div v-if="!isColumnsFold">
            <!-- <el-input v-if="!isEdit" size="small" v-model="table_form.ts_field_name" :placeholder="$t('data.columnNameTip')" class="input_row">
              <div slot="prepend">TIMESTAMP</div>
            </el-input> -->

            <div
              v-for="(column, index) in table_form.columns"
              class="flexCenter input_row"
              :key="'column' + index"
            >
              <el-select
                v-model="column.type"
                size="small"
                slot="prepend"
                :placeholder="$t('Data') + $t('type')"
                class="columnPrependBtn"
                :disabled="typeHasSpe(column.type) || index == 0"
              >
                <el-option
                  v-for="item in column.typeList"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
              <el-input-number
                v-if="column.type == 'VARCHAR' || column.type == 'NCHAR'"
                :value="
                  column.type == 'VARCHAR'
                    ? column.varcharLength
                    : column.ncharLength
                "
                @change="
                  (newVal, oldVal) =>
                    handleChange(newVal,  column.type, index)
                "
                :min="1"
                :max="column.type == 'VARCHAR' ? 16374 : 4093"
                label="Length"
                controls-position="right"
                class="custom-length"
              ></el-input-number>
              <el-input
                size="small"
                v-model="column.field"
                :placeholder="$t('data.columnNameTip')"
              >
                <template slot="append">
                  <el-button
                    icon="el-icon-minus"
                    @click="minusColumn(index, column)"
                  ></el-button>
                  <el-button
                    icon="el-icon-plus"
                    v-if="!isEdit"
                    @click="addColumn(index)"
                  ></el-button>
                  <el-button
                    v-if="isEdit"
                    icon="el-icon-check"
                    @click="columnTypeChange(column)"
                  ></el-button>
                </template>
              </el-input>
            </div>
            <!-- 添加用的column -->
            <div class="flexCenter input_row" v-if="columnEdit && isEdit">
              <el-select
                v-model="currentData.type"
                size="small"
                default-first-option
                :placeholder="$t('Data') + $t('type')"
                class="columnPrependBtn"
              >
                <el-option
                  v-for="item in dataType"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
              <el-input-number
                v-if="currentData.type == 'VARCHAR' || currentData.type == 'NCHAR'"
                :value="
                  currentData.type == 'VARCHAR'
                    ? currentData.varcharLength
                    : currentData.ncharLength
                "
                @change="
                  (newVal, oldVal) =>
                    handleEditChange(newVal, currentData.type)
                "
                :min="1"
                :max="currentData.type == 'VARCHAR' ? 16374 : 4093"
                label="Length"
                controls-position="right"
                class="custom-length"
              ></el-input-number>
              <el-input
                size="small"
                v-model="currentData.field"
                :placeholder="$t('data.columnNameTip')"
              >
                <template slot="append">
                  <el-button
                    icon="el-icon-close"
                    @click="
                      columnEdit = false;
                      currentData = {};
                    "
                  ></el-button>
                  <el-button
                    :disabled="loading"
                    @click="add"
                    icon="el-icon-check"
                  ></el-button>
                </template>
              </el-input>
            </div>
            <el-button
              v-if="isEdit"
              class="add-btn"
              size="small"
              plain
              icon="el-icon-plus"
              @click="addColumn()"
            ></el-button>
          </div>
        </template>
        <template v-else>
          <!-- Tag Section -->
          <div
            class="section_title"
            v-if="table_form.stbTmpl && table_form.stbTmpl != 'nfile'"
          >
            <span class="sectionTitle_text">{{ $t("tags") }}</span>
            <div class="foldIcon" @click="foldTags">
              <i class="el-icon-arrow-right" v-if="isTagsFold"></i>
              <i class="el-icon-arrow-down" v-else></i>
            </div>
          </div>
          <div v-if="!isTagsFold">
            <div v-for="(tag, index) in table_form.tags" :key="'tag' + index">
              <el-input
                size="small"
                v-model="tag.value"
                placeholder="Value"
                class="input_row"
                :title="tag.value"
              >
                <p
                  slot="prepend"
                  class="columnPrependBtn nowrap"
                  :title="tag.type"
                >
                  {{ `${tag.field}:${tag.type}` }}
                </p>
                <el-button
                  v-if="isEdit"
                  slot="append"
                  icon="el-icon-check"
                  @click="tagValueChange(tag, index)"
                ></el-button>
              </el-input>
            </div>
          </div>
        </template>

        <!-- Comfirm Btn -->
        <el-button
          v-if="!isEdit"
          class="submitBtn"
          size="small"
          :loading="loading"
          :disabled="loading"
          type="primary"
          @click="handleCreateTable"
          >{{ $t("create") }}</el-button
        >
        <el-button
          :disabled="loading"
          class="submitBtn"
          size="small"
          @click="cancel"
          >{{ $t("cancel") }}</el-button
        >
      </el-form>
    </div>
  </div>
</template>

<script>
/**
 * 当这个表不属于任何超级表的时候只允许修改列的类型（binary、nchar）和列的添加与删除
 * 当属于某个超级表时只能修改tag的value值，修改列需要去修改超级表的结构
 */
import { mapState } from "vuex";
// import { sendSQLReq } from '@/api/sql'
import { dataType, tagType } from "../../utils";
import { changeTableStruct, getTagValue } from "@/api/gateway/data/tables";
import { VariableTableColumnType } from "@/const";
import { validDatabaseName } from "@/utils/validate";
Array.prototype.insert = function (index, item) {
  this.splice(index, 0, item);
};
Array.prototype.remove = function (index) {
  if (index > -1 && this.length > 1) {
    this.splice(index, 1);
  }
};
export default {
  data() {
    this.tagType = tagType;
    this.dataType = dataType;
    return {
      isColumnsFold: false,
      isTagsFold: false,
      columnEdit: false,
      currentData: {},
      loading: false,
    };
  },
  computed: {
    ...mapState({
      selected_db: (state) => state.dbs.selected_db,
      table_form: (state) => state.tables.table_form,
    }),
    isEdit() {
      return this.$store.state.tables.formStatus == "update";
    },
    formTitle() {
      if (!this.isEdit) {
        if (this.$store.state.tables.formStatus == "createByStb") {
          return this.$t("data.createTableUse").replace(
            /\{\}/,
            this.table_form.stbTmpl
          );
        } else {
          return this.$t("data.createTable");
        }
      } else {
        return (
          this.$t("edit") + ` ${this.table_form.name} ` + this.$t("data.table")
        );
      }
    },
    rules() {
      return {
        name: [
          {
            required: true,
            message: this.$t("data.nameTip").replace('/name/',this.$t('dashboard.tables')),
            trigger: "blur",
          },
          {
            validator: (_, value, callback) => {
              callback(
                validDatabaseName(value)
                  ? undefined
                  : new Error(this.$t("data.nameTip").replace('/name/',this.$t('dashboard.tables')))
              );
            },
            trigger: "blur",
          },
        ],
      };
    },
  },
  watch: {
    table_form: {
      handler() {
        this.$nextTick(() => {
          this.$refs.table_form.clearValidate();
        });
      },
    },
  },
  methods: {
    handleChange(newVal, type, index) {
      if (type === "VARCHAR") {
        this.$set(this.table_form.columns[index], "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.table_form.columns[index], "ncharLength", newVal);
      }
    },
    handleEditChange(newVal, type) {
      if (type === "VARCHAR") {
        this.$set(this.currentData, "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.currentData, "ncharLength", newVal);
      }
    },
    // 判断类型是不是可以修改的类型
    typeHasSpe(currentType) {
      if (!this.isEdit) return false;
      return !VariableTableColumnType.some((item) =>
        currentType.startsWith(item)
      );
    },
    foldColumns() {
      this.isColumnsFold = !this.isColumnsFold;
    },
    addColumn(index) {
      if (!this.isEdit) {
        return this.table_form.columns.insert(index + 1, {
          type: "INT",
          field: "",
          varcharLength:8,
          ncharLength:8,
          typeList: dataType
        });
      }
      this.columnEdit = true;
      this.currentData = { field: "", type: "INT",varcharLength:8,
          ncharLength:8 };
    },
    columnTypeChange(column) { 
      let params = null
      let rename_params = null
      if(!this.typeHasSpe(column.type) && column.type_old !== column.type) {
        params = {
          operation: "modify column",
          first_field: column.field_old,
          second_field: column.type,
        };
      } 
      if(column.field_old !== column.field) {
        rename_params = {
          operation: "rename column",
          first_field: column.field_old, // old_col_name
          second_field: column.field, // new_col_name
        };
      }
      this.updateTypeField(params, rename_params);  
    },
    minusColumn(index, data) {
      if (!this.isEdit) return this.table_form.columns.remove(index);
      this.$confirm(this.$t('isDel').replace('{isDelName}', ''), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      })
        .then(() => {
          let params = {
            operation: "drop column",
            first_field: data.field,
          };
          this.updateData(params);
        })
        .catch(() => {});
    },
    async updateData(params, tag) {
      if (this.loading) return;
      this.loading = true;
      await changeTableStruct(
        params,
        "`" + this.selected_db + "`" + "." + "`" + this.table_form.name + "`"
      )
        .then(() => {
          this.$message.success(this.$t("operateSucc"));
        })
        .catch((err) => this.$message.error(err?.desc));
      this.loading = false;
      // 修改tag的value时只获取当前value的值
      if (tag) {
        tag.value = (
          await getTagValue(
            [tag],
            this.selected_db,
            this.table_form.stbTmpl,
            this.table_form.name
          )
        )[0][tag.field];
      } else {
        this.$store
          .dispatch("tables/getTableStruct", {
            tableName: this.table_form.name,
            stableName: this.table_form.stbTmpl,
          })
          .catch(() => false);
      }
    },
    async updateTypeField(params, rename_params) {
      if (this.loading) return;
      this.loading = true;
      let second_params = "`" + this.selected_db + "`" + "." + "`" + this.table_form.name + "`"
      if(params) {
        await changeTableStruct(params, second_params)
          .then(async () => {
            this.$message.success(this.$t('data.modifyColumn') + this.$t("operateSucc"));
          })
          .catch((err) => this.$message.error(err?.desc));
      }
      if(rename_params) {
        await changeTableStruct(rename_params,second_params)
          .then(async () => {
            this.$message.success(this.$t('data.renameColumn') + this.$t("operateSucc"));
          })
          .catch((err) => this.$message.error(err?.desc));
      }
      this.loading = false;
      this.$store
        .dispatch("tables/getTableStruct", {
          tableName: this.table_form.name,
          stableName: this.table_form.stbTmpl,
        })
        .catch(() => false);
    },
    foldTags() {
      this.isTagsFold = !this.isTagsFold;
    },
    addTag(index) {
      this.table_form.tags.insert(index + 1, {
        type: "",
        field: "",
        value: "",
      });
    },
    minusTag(index) {
      this.table_form.tags.remove(index);
    },
    handleCreateTable() {
      this.$refs.table_form.validate((valid) => {
        if (valid) {
          this.handleData();
          this.$store.dispatch("tables/submitTableForm").then(() => {
            this.$message.success(this.$t("createSucc"));
          })
          .catch((err) => {
            console.log('sssserr',err);
            this.$message({
              type: "error",
              message: err?.desc
            })
          });
        }
      });
    },
    handleData() {
      this.table_form.columns = this.table_form.columns.filter(
        (item) => item.field
      );
      if (this.table_form.tags) {
        this.table_form.tags = this.table_form.tags.filter(
          (item) => item.value
        );
      }
    },
    handleEditTable() {
      this.handleCreateTable();
    },
    // 当修改表结构的tag时，tag的value发生变化的
    tagValueChange(tag) {
      let isString = VariableTableColumnType.some((item) =>
        tag.type.startsWith(item)
      );
      let value = isString ? `'${tag.value}'` : tag.value;
      let params = {
        operation: "set tag",
        first_field: `\`${tag.field}\`` + "=" + value,
      };
      this.updateData(params, tag);
    },
    add() {
      if (!this.currentData.field || !this.currentData.type) {
        return this.$message.error(this.$t("data.checkFail"));
      }
      let params = {
        operation: "add column",
        first_field: this.currentData.field,
        second_field: this.currentData.type === 'VARCHAR' 
        ? this.currentData.type + `(${this.currentData.varcharLength})`
        : this.currentData.type === 'NCHAR' 
        ? this.currentData.type + `(${this.currentData.ncharLength})` 
        : this.currentData.type,
      };
      this.columnEdit = false;
      this.updateData(params);
    },
    cancel() {
      this.$store.commit("console/CANCEL_DETAIL");
    },
  },
};
</script>

<style lang="scss" scoped>
.form_title {
  font-size: 24px;
  font-weight: 400;
}

.formWrapper {
  padding-right: 18px;
  max-width: 680px;
}

.name_input {
  margin-top: 20px;
}

.columnPrepend {
  width: 130px;
  cursor: auto;
}

.columnPrependBtn {
  width: 130px;
  flex-shrink: 0;
}

.section_title {
  font-size: 18px;
  font-weight: 400;
  margin-top: 30px;
  display: flex;
  flex-direction: row;
  justify-content: space-between;

  .foldIcon {
    font-size: 14px;
    cursor: pointer;
  }
}

.input_row {
  margin-top: 18px;
}
.add-btn {
  margin-top: 20px;
  width: 100%;
}
.submitBtn {
  margin-top: 40px;
  font-size: 14px;
}
.custom-length {
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
</style>
