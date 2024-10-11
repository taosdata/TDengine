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
          <!-- <el-input
            size="small"
            v-model="stable_form.ts_field_name"
            :placeholder="$t('data.columnNameTip')"
            :disabled="isEdit"
            class="input_row"
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
              :disabled="typeHasSpe(column.type)  || index == 0"
              default-first-option
              :placeholder="$t('Data') + $t('type')"
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
              v-if="VariableTableColumnType.includes(column.type)"
              :value="column.length"
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
              :disabled="isEdit"
              :placeholder="$t('data.columnNameTip')"
            >
            </el-input>
            <el-tag effect="plain" type="info" v-if="index==1 && version_gt_3300">
              <el-checkbox 
                :disabled="isEdit || parmaryKeyType.findIndex((item) => item.value.includes(column.type)) == -1" 
                v-model="column.primaryKey" 
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
              size="small"
              icon="el-icon-minus"
              @click="minusColumn(index)"
              :disabled="!index || (isEdit && column.primaryKey)"
            ></el-button>
            <el-button
              v-if="!isEdit"
              size="small"
              @click="addColumn"
              icon="el-icon-plus"
            ></el-button>
            <el-button
              v-else
              size="small"
              :disabled="!isEdit"
              icon="el-icon-check"
              @click="typeChange(column, 'column')"
            ></el-button>
          </span>
          </div>
          <!-- 编辑用的column -->
          <div class="flexCenter input_row" v-if="currentEdit == 'column'">
            <el-select
              v-model="currentData.type"
              size="small"
              default-first-option
              :placeholder="$t('Data') + $t('type')"
              class="columnPrependBtn"
              @change="handleEditTypeChange(currentData)"
            >
              <el-option
                v-for="item in dataType"
                :key="item.value"
                v-bind="item"
              ></el-option>
            </el-select>
            <el-input-number
              v-if="VariableTableColumnType.includes(currentData.type)"
              :value="currentData.length"
              @change="
                (newVal, oldVal) =>
                  handleEdit(newVal, currentData.type)
              "
              :min="1"
              :max="currentData.type == 'NCHAR' ? 4093 : 65517"
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
            </el-input>
            <el-tooltip
                placement="top" effect="light" :open-delay="100"
                :content="$t('console.encode')" v-if="version_gt_3300">
                <el-select
                  size="small"
                  default-first-option
                  v-model="currentData.encode"
                  placeholder="ENCODE"
                  class="columnWidth120"
                  clearable>
                  <el-option
                    v-for="item in handleEncodeList(currentData.type)['encodeList']"
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
                  v-model="currentData.compress"
                  placeholder="COMPRESS"
                  class="columnWidth120"
                  clearable
                >
                  <el-option
                    v-for="item in handleEncodeList(currentData.type)['compressList']"
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
                  v-model="currentData.level"
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
              icon="el-icon-close"
              size="small"
              @click="
                currentEdit = '';
                currentData = {};
              "
            ></el-button>
            <el-button
              size="small"
              @click="add"
              :disabled="loading"
              icon="el-icon-check"
            ></el-button>
          </span>
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
              v-if="VariableTableColumnType.includes(tag.type)"
              :value="tag.length"
              @change="(newVal,oldVal)=>tagLengthChange(newVal,oldVal,tag.type,index)"
              :min="1"
              :max="tag.type == 'NCHAR' ? 4093 : 16382"
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
              v-if="VariableTableColumnType.includes(currentData.type)"
              :value="currentData.length"
              @change="
                (newVal, oldVal) =>
                  handleTagEdit(newVal, currentData.type)
              "
              :min="1"
              :max="currentData.type == 'NCHAR' ? 4093 : 16382"
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
import { dataType, tagType, parmaryKeyType, storageCompression, levelList, groupOne, groupTwo, groupThree, groupFour, groupFive } from "../../utils";
import { changeStableStruct, changeStableStructOther } from "@/api/gateway/data/stables";
import { VariableTableColumnType } from "@/const";
import { Message } from "element-ui";
import VersionMixin from "@/mixins/version";
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
    this.parmaryKeyType = parmaryKeyType;
    this.storageCompression = storageCompression;
    this.levelList = levelList;
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
      VariableTableColumnType: VariableTableColumnType
    };
  },
  mixins: [VersionMixin],
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
                value.indexOf('.') != -1 ? new Error(this.$t("formatWrong")) : undefined
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
      this.$set(this.currentData, "length", newVal);
    },
    //编辑列用
    handleEdit(newVal, type){
      this.$set(this.currentData, "length", newVal);
    },
    //columns的自定义varchar/nchar长度
    handleChange(newVal, oldVal, type, index) {
      this.$set(this.stable_form.columns[index], "length", newVal);
    },
    //tag自定义长度
    tagLengthChange(newVal, oldVal, type, index) {
      this.$set(this.stable_form.tags[index], "length", newVal);
    },
    // columns 修改时encode/compress 变更
    handleTypeChange(column, index) {
      if (this.isEdit) return;
      const data = this.handleEncodeList(column.type)
      const { defaultEncode, defaultCompress } = data
      this.$set(this.stable_form.columns[index], "encode", defaultEncode);
      this.$set(this.stable_form.columns[index], "compress", defaultCompress);
      this.$set(this.stable_form.columns[index], "level", 'medium');
      // 如果不支持 primary key 
      if (index == 1 && 
        column.primaryKey && 
        this.parmaryKeyType.findIndex((item) => item.value.includes(column.type)) == -1) 
      {
        this.$set(this.stable_form.columns[index], "primaryKey", false);
      }
    },
    handleEditTypeChange(column, index) {
      const data = this.handleEncodeList(column.type)
      const { defaultEncode, defaultCompress } = data
      this.$set(this.currentData, "encode", defaultEncode);
      this.$set(this.currentData, "compress", defaultCompress);
      this.$set(this.currentData, "level", 'medium');
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
          item.value.startsWith(VariableTableColumnType[index]) 
          // &&
          // cur &&
          // +cur[0] > +currentType.match(/\d+/)?.[0]
        );
      });
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
    // 判断类型是不是可以修改的类型
    typeHasSpe(currentType) {
      if (!this.isEdit) return false;
      return !VariableTableColumnType.some((item) =>
        currentType.startsWith(item)
      );
    },
    async typeChange(data, type, index) {
      // 不是修改状态就不处理
      if (!this.isEdit) return;
      let isVariable =  VariableTableColumnType.some((item) =>
        data.type.startsWith(item)
      );
      let params = null;
      if (isVariable && data.length_old !== data.length) {
        params = {
          isVariable,
          operation: "modify " + type,
          first_field: data.field,
          second_field: VariableTableColumnType.includes(data.type)
            ? `${data.type}(${data.length})`
            : data.type,
        };
      }
      if (type == "tag" && data.field_old !== data.field) {
        //这里区分tag修改的是啥
        if (this.duplicate[index].type == data.type) {
          params = {
            operation: "rename " + type,
            first_field: this.duplicate[index].field,
            second_field: `${data.field}`,
          };
        }
      }
      await this.updateData(params);

      if (this.version_gt_3300 && (data.encode_old !== data.encode || data.compress_old !== data.compress || data.level_old !== data.level)) {
        params = {
          isVariable,
          operation: "modify " + type,
          first_field: data.field,
          second_field: VariableTableColumnType.includes(data.type)
            ? `${data.type}(${data.length})`
            : data.type,
          encode: data.encode,
          compress: data.compress,
          level: data.level
        };
        await this.updateDataOther(params)
      }

    },
    // 当修改时更新数据的接口，与新增无关
    async updateData(params) {
      this.loading = true;
      if (params) {
        await changeStableStruct(
          params,
          `\`${this.selected_db }\`.\`${this.stable_form.name}\``
        )
        .then(() => {
          this.$message.success(this.$t("operateSucc"));
        })
        .catch(err => this.$error(err.desc));
        await this.$store
          .dispatch("stables/getStatleStruct", { stableName: this.stable_form.name, type: 'create_stb'})
          .catch(() => false);
        await this.$store.commit("console/CHANGE_TREE_KEY", null, { root: true })
      }
      // 无论修改成功或失败都应该刷新数据
      this.loading = false;
    },
    async updateDataOther(params) {
      this.loading = true;
      // 改压缩方法
      if (!params.operation.startsWith('rename')) {
        await changeStableStructOther(
          params,
          `\`${this.selected_db }\`.\`${this.stable_form.name}\``
        )
          .then(() => {
            this.$message.success(this.$t("operateSucc"));
          })
          .catch(err => this.$error(err.desc));
      }
      // 无论修改成功或失败都应该刷新数据
      await this.$store
        .dispatch("stables/getStatleStruct", { stableName: this.stable_form.name, type: 'create_stb'})
        .catch(() => false);
      await this.$store.commit("console/CHANGE_TREE_KEY", null, { root: true })
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
          length: 8,
          encode: "simple8b", 
          compress: "lz4", 
          level: "medium",
        });
      }
      this.currentEdit = "column";
      this.currentData = {
        field: "",
        type: "INT",
        length: 8,
        encode: "simple8b", 
        compress: "lz4", 
        level: "medium",
      };
    },
    minusColumn(index) {
      if (!this.isEdit) {
        this.stable_form.columns.remove(index, 'column');
        if (index == 1) {
          this.$set(this.stable_form.columns[index], "primaryKey", false);
        }
        return 
      }
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
          length: 8,
        });
      }
      this.currentEdit = "tag";
      this.currentData = {
        tag: "",
        type: "INT",
        length: 8,
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
          for (let i = 0; i < this.stable_form.columns.length; i++) {
            const element = this.stable_form.columns[i];
            if (!element.field) {
              return Message.warning(
                this.$t("dataIn.enterTip") + " " + this.$t("data.columnNameTip")
              );
            }
          }
          for (let i = 0; i < this.stable_form.tags.length; i++) {
            const element = this.stable_form.tags[i];
            if (!element.field) {
              return Message.warning(
                this.$t("dataIn.enterTip") + " " + this.$t("data.tagNameTip")
              );
            }
          }
          this.handleData();
          this.$store
            .dispatch("stables/submitStableForm", this.selected_db)
            .then(() => {
              this.$message.success(this.$t("createSucc"));
              this.$store.commit("console/CANCEL_DETAIL");
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
      if (!this.version_gt_3300) {
        this.stable_form.columns = this.stable_form.columns.map((item) => {
          return {
            ...item,
            encode: '',
            compress: '',
            level: ''
          }
        });
      }
    },
    // 修改状态时，确定后发送请求添加数据
    add() {
      let params = {
        operation: "add " + this.currentEdit,
        first_field: this.currentData.field,
        second_field: VariableTableColumnType.includes(this.currentData.type)
        ? `${this.currentData.type}(${this.currentData.length})`
        : this.currentData.type,
        encode: this.version_gt_3300 ? this.currentData.encode : '',
        compress: this.version_gt_3300 ? this.currentData.compress : '',
        level: this.version_gt_3300 ? this.currentData.level : ''
      };
      this.currentData = {};
      this.currentEdit = "";
      this.updateDataOther(params);
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
  // max-width: 920px;
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
.columnWidth120 {
  width: 110px;
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
// .action {
//   ::v-deep .el-input-group__append {
//     padding: 0 8px;
//   }
// }
</style>
