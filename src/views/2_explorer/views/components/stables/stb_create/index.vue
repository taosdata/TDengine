<template>
  <div class="stbCreate">
    <div class="form_title">{{ formTitle }}</div>
    <div class="formWrapper">
      <el-form
        ref="stable_form"
        label-position="left"
        label-width="150px"
        :rules="rules"
        :model="stable_form"
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
            :disabled="isEdit"
            :maxlength="192"
            :title="stable_form.name"
            v-model="stable_form.name"
          >
          </el-input>
        </el-form-item>
        <!-- rollup -->
        <el-form-item
          v-if="db_data.retentions"
          prop="rollup"
          class="name_input"
        >
          <template slot="label">
            <span>rollup</span>
          </template>
          <el-select
            class="w100"
            size="small"
            placeholder=""
            :disabled="isEdit"
            v-model="stable_form.rollup"
          >
            <el-option
              v-for="item in rollupList"
              :key="item"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <!-- Column Section -->
        <div class="section_title">
          <span class="sectionTitle_text">{{ $t("data.columns") }}</span>
          <div class="foldIcon" @click="foldColumns">
            <i class="el-icon-arrow-right" v-if="isColumnsFold"></i>
            <i class="el-icon-arrow-down" v-else></i>
          </div>
        </div>
        <div v-if="!isColumnsFold">
          <el-input
            size="small"
            v-model="stable_form.ts_field_name"
            :placeholder="$t('data.columnNameTip')"
            :disabled="isEdit"
            class="input_row"
          >
            <div slot="prepend">TIMESTAMP</div>
          </el-input>

          <div
            class="flexCenter input_row"
            v-for="(column, index) in stable_form.columns"
            :key="'column' + index"
          >
            <el-select
              v-model="column.type"
              size="small"
              :disabled="typeHasSpe(column.type)"
              default-first-option
              :placeholder="$t('Data') + $t('type')"
              class="columnPrependBtn"
            >
              <el-option
                v-for="item in handleTypeList(column.type, 'dataType')"
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
                  handleChange(newVal, oldVal, column.type, index)
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
              :maxlength="64"
              :disabled="isEdit"
              :placeholder="$t('data.columnNameTip')"
            >
              <template slot="append">
                <el-button
                  icon="el-icon-minus"
                  @click="minusColumn(index)"
                ></el-button>
                <el-button
                  v-if="!isEdit"
                  @click="addColumn"
                  icon="el-icon-plus"
                ></el-button>
                <el-button
                  v-else
                  :disabled="typeHasSpe(column.type)"
                  icon="el-icon-check"
                  @click="typeChange(column, 'column')"
                ></el-button>
              </template>
            </el-input>
          </div>
          <!-- 编辑用的column -->
          <div class="flexCenter input_row" v-if="currentEdit == 'column'">
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
                  handleEdit(newVal, currentData.type)
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
              :maxlength="64"
              :placeholder="$t('data.columnNameTip')"
            >
              <template slot="append">
                <el-button
                  icon="el-icon-close"
                  @click="
                    currentEdit = '';
                    currentData = {};
                  "
                ></el-button>
                <el-button
                  @click="add"
                  :disabled="loading"
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
            :disabled="currentEdit == 'column'"
            icon="el-icon-plus"
            @click="addColumn()"
          ></el-button>
        </div>

        <!-- Tag Section -->
        <div class="section_title">
          <span class="sectionTitle_text">{{ $t("tags") }}</span>
          <div class="foldIcon" @click="foldTags">
            <i class="el-icon-arrow-right" v-if="isTagsFold"></i>
            <i class="el-icon-arrow-down" v-else></i>
          </div>
        </div>
        <div v-if="!isTagsFold">
          <div
            class="flexCenter input_row"
            v-for="(tag, index) in stable_form.tags"
            :key="'tag' + index"
          >
            <el-select
              v-model="tag.type"
              size="small"
              :disabled="typeHasSpe(tag.type)"
              default-first-option
              :placeholder="$t('Data') + $t('type')"
              class="columnPrependBtn"
            >
              <el-option
                v-for="item in handleTypeList(tag.type, 'tagType')"
                :key="item.value"
                v-bind="item"
              ></el-option>
            </el-select>

            <el-input-number
              v-if="tag.type == 'VARCHAR' || tag.type == 'NCHAR'"
              :value="tag.type == 'VARCHAR'
                  ? tag.varcharLength
                  : tag.ncharLength"
              @change="(newVal,oldVal)=>tagLengthChange(newVal,oldVal,tag.type,index)"
              :min="1"
              :max="tag.type == 'VARCHAR' ? 16374 : 4093"
              label="Length"
              controls-position="right"
              class="custom-length"
            ></el-input-number>
            <el-input
              size="small"
              v-model="tag.field"
              :maxlength="64"
              @focus="saveOldTag(tag.field)"
              :placeholder="$t('data.tagNameTip')"
            >
              <template slot="append">
                <el-button
                  icon="el-icon-minus"
                  @click="minusTag(index)"
                ></el-button>
                <el-button
                  v-if="!isEdit"
                  :disabled="addTagDisabled"
                  @click="addTag"
                  icon="el-icon-plus"
                ></el-button>
                <el-button
                  v-else
                  icon="el-icon-check"
                  @click="typeChange(tag, 'tag', index)"
                ></el-button>
              </template>
            </el-input>
          </div>
          <!-- 编辑用的tag -->
          <div
            class="flexCenter input_row"
            v-if="currentEdit == 'tag' && isEdit && !addTagDisabled"
          >
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
                  handleTagEdit(newVal, currentData.type)
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
              :maxlength="64"
              :placeholder="$t('data.tagNameTip')"
            >
              <template slot="append">
                <el-button
                  icon="el-icon-close"
                  @click="currentEdit = ''"
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
            :disabled="currentEdit == 'tag'"
            icon="el-icon-plus"
            @click="addTag()"
          ></el-button>
        </div>

        <!-- Comfirm Btn -->
        <el-button
          v-if="!isEdit"
          class="submitBtn"
          size="small"
          :disabled="loading"
          :loading="loading"
          type="primary"
          @click="handleCreateStable"
          >{{ $t("create") }}</el-button
        >
        <el-button
          class="submitBtn"
          :disabled="loading"
          size="small"
          @click="cancel"
          >{{ $t("cancel") }}</el-button
        >
      </el-form>
    </div>
  </div>
</template>

<script>
import { mapState } from "vuex";
import { dataType, tagType } from "../../utils";
import { changeStableStruct } from "@/api/gateway/data/stables";
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
    this.dataType = dataType;
    this.tagType = tagType;
    this.rollupList = ["avg", " sum", "min", "max", "last", "first"];
    return {
      isColumnsFold: false,
      isTagsFold: false,
      currentEdit: "",
      currentData: {},
      currentOriData: {},
      currentField: "",
      loading: false,
      customeLength: 8,
      tagLength: 8,
    };
  },
  computed: {
    ...mapState({
      selected_db: (state) => state.dbs.selected_db,
      db_data: (state) => state.dbs.db_form,
      stable_form: (state) => state.stables.stable_form,
      formStatus: (state) => state.stables.formStatus,
    }),
    duplicate() {
      return this.$store.state.stables.tagDuplicate;
    },
    isEdit() {
      return this.formStatus == "update";
    },
    formTitle() {
      if (!this.isEdit) {
        return this.$t("data.createStable");
      } else {
        return this.$t("data.editStable");
      }
    },
    rules() {
      return {
        name: [
          {
            required: true,
            message: this.$t("data.nameTip").replace('/name/',this.$t('dashboard.stables')),
            trigger: "blur",
          },
          {
            validator: (_, value, callback) => {
              callback(
                validDatabaseName(value)
                  ? undefined
                  : new Error(this.$t("data.nameTip").replace('/name/',this.$t('dashboard.stables')))
              );
            },
            trigger: "blur",
          },
        ],
        rollup: [
          {
            required: true,
            trigger: "blur",
          },
        ],
      };
    },
    addTagDisabled() {
      return this.stable_form.tags.length > 128;
    },
  },
  watch: {
    stable_form: {
      handler() {
        this.$nextTick(() => {
          this.$refs.stable_form.clearValidate();
        });
      },
    },
  },
  methods: {
    handleTagEdit(newVal,type){
      if (type === "VARCHAR") {
        this.$set(this.currentData, "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.currentData, "ncharLength", newVal);
      }
    },
    //编辑列用
    handleEdit(newVal, type){
      if (type === "VARCHAR") {
        this.$set(this.currentData, "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.currentData, "ncharLength", newVal);
      }
    },
    //columns的自定义varchar/nchar长度
    handleChange(newVal, oldVal, type, index) {
      if (type === "VARCHAR") {
        this.$set(this.stable_form.columns[index], "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.stable_form.columns[index], "ncharLength", newVal);
      }
    },
    //tag自定义长度
    tagLengthChange(newVal, oldVal, type, index) {
      if (type === "VARCHAR") {
        this.$set(this.stable_form.tags[index], "varcharLength", newVal);
      }
      if (type === "NCHAR") {
        this.$set(this.stable_form.tags[index], "ncharLength", newVal);
      }
    },
    // 当修改时，如果字段的类型为binary和nchar则需要对可修改的进行过滤，只保留比其大的
    handleTypeList(currentType, name) {
      if (!this.isEdit) return this[name];
      // 当数据类型为BINARY和NCHAR才会进行过滤并且是修改状态下的时候
      let index = VariableTableColumnType.findIndex((item) =>
        currentType.startsWith(item)
      );
      if (index == -1) return this[name];
      return this[name].filter((item) => {
        let cur = item.value.match(/\d+/);
        return (
          item.value.startsWith(VariableTableColumnType[index]) &&
          cur &&
          +cur[0] > +currentType.match(/\d+/)?.[0]
        );
      });
    },
    // 判断类型是不是可以修改的类型
    typeHasSpe(currentType) {
      if (!this.isEdit) return false;
      return !VariableTableColumnType.some((item) =>
        currentType.startsWith(item)
      );
    },
    typeChange(data, type, index) {
      // 不是修改状态就不处理
      if (!this.isEdit) return;
      let params = {
        operation: "modify " + type,
        first_field: data.field,
        second_field: data.type,
      };
      if (type == "tag") {
        //这里区分tag修改的是啥
        if (this.duplicate[index].type == data.type) {
          params = {
            operation: "rename " + type,
            first_field: this.duplicate[index].field,
            second_field: `\`${data.field}\``,
          };
        }
      }
      this.updateData(params);
    },
    // 当修改时更新数据的接口，与新增无关
    async updateData(params) {
      this.loading = true;
      await changeStableStruct(
        params,
        `\`${this.selected_db }\`. \`${this.stable_form.name}\``
      )
        .then(() => {
          this.$message.success(this.$t("operateSucc"));
        })
        .catch(() => false);
      // 无论修改成功或失败都应该刷新数据
      await this.$store
        .dispatch("stables/getStatleStruct", this.stable_form.name)
        .catch(() => false);
      this.loading = false;
    },
    foldColumns() {
      this.isColumnsFold = !this.isColumnsFold;
    },
    addColumn() {
      let index = this.stable_form.columns.length;
      if (!this.isEdit) {
        return this.stable_form.columns.insert(index, {
          type: "INT",
          field: "",
          varcharLength: 8,
          ncharLength: 8,
        });
      }
      this.currentEdit = "column";
      this.currentData = {
        field: "",
        type: "INT",
        varcharLength: 8,
        ncharLength: 8,
      };
    },
    minusColumn(index) {
      if (!this.isEdit) return this.stable_form.columns.remove(index);
      this.$confirm(this.$t('isDel').replace('{isDelName}', ''), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      })
        .then(() => {
          let data = this.stable_form.columns[index];
          let params = {
            operation: "drop column",
            first_field: data.field,
          };
          this.updateData(params);
        })
        .catch(() => {});
    },
    foldTags() {
      this.isTagsFold = !this.isTagsFold;
    },
    // 修改时：保存旧的tagfield
    saveOldTag(field) {
      if (!this.isEdit) return;
      this.currentField = field;
    },
    // 修改时：修改
    tagFieldChange(val) {
      if (!this.isEdit) return;
      let params = {
        operation: "change tag",
        first_field: this.currentField,
        second_field: val,
      };
      this.updateData(params);
    },
    addTag() {
      let index = this.stable_form.tags.length;
      if (!this.isEdit) {
        return this.stable_form.tags.insert(index, {
          type: "INT",
          field: "",
          varcharLength: 8,
          ncharLength: 8,
        });
      }
      this.currentEdit = "tag";
      this.currentData = {
        tag: "",
        type: "INT",
        varcharLength: 8,
        ncharLength: 8,
      };
    },
    minusTag(index) {
      if (!this.isEdit) return this.stable_form.tags.remove(index);
      this.$confirm(this.$t('isDel').replace('{isDelName}', ''), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      }).then(() => {
        let data = this.stable_form.tags[index];
        let params = {
          operation: "drop tag",
          first_field: data.field,
        };
        this.updateData(params);
      });
    },
    handleCreateStable() {
      this.$refs.stable_form.validate((valid) => {
        if (valid) {
          this.handleData();
          this.$store
            .dispatch("stables/submitStableForm", this.selected_db)
            .then(() => {
              this.$message.success(this.$t("createSucc"));
            })
            .catch((err) => {
              this.$message({
                type: "error",
                message: err?.desc
              })
            });
        }
      });
    },
    handleData() {
      this.stable_form.columns = this.stable_form.columns.filter((item) =>item.field);
      this.stable_form.tags = this.stable_form.tags.filter(
        (item) => item.field
      );
    },
    // 修改状态时，确定后发送请求添加数据
    add() {
      let params = {
        operation: "add " + this.currentEdit,
        first_field: this.currentData.field,
        second_field: this.currentData.type=='VARCHAR'?`VARCHAR(${this.currentData.varcharLength})`:this.currentData.type=='NCHAR'?
        `NCHAR(${this.currentData.ncharLength})`:this.currentData.type,
      };
      this.currentData = {};
      this.currentEdit = "";
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
  width: 150px;
  cursor: auto;
}

.columnPrependBtn {
  width: 150px;
  flex-shrink: 0;
}
.add-btn {
  margin-top: 20px;
  width: 100%;
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
