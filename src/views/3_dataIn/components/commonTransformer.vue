<template>
  <div class="common-transformer">
    <template>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.msgbody") }}</span>
        </div>
        <el-form
          @submit.native.prevent
          :model="msgForm"
          :rules="msgRules"
          ref="msgForm"
        >
          <el-form-item prop="msgbody">
            <el-input
              v-model="msgForm.msgbody"
              size="small"
              type="textarea"
            ></el-input>
          </el-form-item>
        </el-form>
      </section>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.identified") }}</span>
        </div>
        <ul class="col-list">
          <li v-for="(item, index) in indentifiedColumns" :key="index">
            <span>{{ item.name }}</span>
          </li>
        </ul>
      </section>
      <section class="extract">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.extract") }}</span>
        </div>
        <template v-for="(item, index) in extractArr">
          <ExtractSplit
            ref="extract"
            :key="index"
            :itemData="item"
            :index="index"
            :payload="msgForm.msgbody"
            :extractColumns="item.columns"
            :indentifiedColumns="indentifiedColumns"
            @deleteExtract="deleteExtract"
            @selectColumn="changeColumnStatus"
            @changeExtractExpr="changeExtractExpr"
          ></ExtractSplit>
        </template>

        <el-button type="primary" size="small" @click="addNewExtract">
          {{ $t("add") }}
        </el-button>
      </section>
      <section class="filter">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.filter") }}</span>
        </div>
        <template v-for="(item, index) in filterArr">
          <FilterExpression
            :key="index"
            :index="index"
            :itemData="item"
            :payload="msgForm.msgbody"
            :inputparamsColumns="columnsArr"
            :indentifiedColumns="indentifiedColumns"
            @deleteFilter="deleteFilter"
            @changeFilter="changeFilter"
            ref="filter"
          ></FilterExpression>
        </template>
        <el-button
          type="primary"
          size="small"
          @click="addNewFilter"
          :disabled="filterArr.length >= 1"
        >
          {{ $t("add") }}
        </el-button>
      </section>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.superconfig") }}</span>
        </div>
        <div class="table-content">
          <div class="table-title">
            <div class="title">
              <span style="color: #4259ce">
                {{ $t("datasource.transformer.targetSt") }}
              </span>
              <el-form :model="sruleForm">
                <el-form-item prop="s_name">
                  <el-select
                    v-model="sruleForm.s_name"
                    filterable
                    allow-create
                    default-first-option
                    size="small"
                    @change="getSTbaleList"
                  >
                    <el-option
                      v-for="(item, index) in stableLists"
                      :key="index"
                      :label="item"
                      :value="item"
                    ></el-option>
                  </el-select>
                </el-form-item>
              </el-form>
            </div>
            <el-button type="primary" size="small" @click="createStable">
              {{ $t("datasource.transformer.createstb") }}
            </el-button>
          </div>
          <div class="table-detail" v-if="tableData.length > 0">
            <div class="mapping">
              {{ $t("datasource.transformer.mapping") }}
              <el-button
                type="primary"
                @click="caculateMappingResult"
                size="small"
                >{{ $t("datasource.transformer.caculate") }}</el-button
              >
            </div>
            <el-table
              :data="tableData"
              border
              style="width: 100%"
              :key="tablekey"
            >
              <template v-for="(item, index) in st_columnLists">
                <el-table-column
                  v-if="item === 'Expression'"
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                  width="320px"
                >
                  <template slot-scope="scope">
                    <el-cascader
                      size="small"
                      style="width: 100px; margin-right: 10px"
                      :show-all-levels="false"
                      v-model="scope.row.maptype[1]"
                      v-if="scope.row['Type'] != 'Tablename'"
                      @change="changeMapColumn(scope)"
                      :options="options"
                    ></el-cascader>
                    <!-- <el-popover
                      trigger="click"
                      placement="right-end"
                      :content="$t('datasource.transformer.searchSResult')"
                    >-->
                    <el-input
                      slot="reference"
                      style="width: 180px"
                      v-model="scope.row.Expression"
                      size="small"
                      :disabled="
                        scope.row['Type'] == 'TIMESTAMP' && !enable
                          ? true
                          : false
                      "
                    ></el-input>

                    <!-- @keyup.enter.native="submitSuper(scope.row)" -->
                    <!-- </el-popover> -->
                  </template>
                </el-table-column>

                <el-table-column
                  v-else
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                ></el-table-column>
              </template>
            </el-table>
          </div>
        </div>
      </section>
      <el-dialog
        :title="$t('datasource.transformer.create_st')"
        :visible.sync="showCreateDIalog"
        width="40%"
        center
        destroy-on-close
        :append-to-body="true"
        @close="closeDialog"
      >
        <CreateSTB ref="createstb"></CreateSTB>
        <div class="buttons">
          <el-button type="primary" size="small" @click="createST">
            {{ $t("create") }}
          </el-button>
          <el-button size="small" @click="closeDialog">
            {{ $t("cancel") }}
          </el-button>
        </div>
      </el-dialog>
    </template>
  </div>
</template>
<script>
import ExtractSplit from "./extractSplit.vue";
import FilterExpression from "./filterExpression.vue";
import { getParser } from "@/api/explorer/datain";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { getRFC3339Time } from "@/utils/index";
import CreateSTB from "./createSTB.vue";
import { createStableReq } from "@/api/gateway/data/stables";
export default {
  name: "CommonTransformer",
  components: { ExtractSplit, FilterExpression, CreateSTB },
  props: {
    parserColumns: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      mapExpressionList: [
        "value",
        "generator",
        "join",
        "format",
        "sum",
        "expr",
      ],
      showFilterSect: false,
      enable: true, //只针对ts的expression的input
      timestampExpr: "",
      options: [],
      tablekey: 1,
      msgForm: {
        msgbody: "",
      },
      msgRules: {
        msgbody: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.msgbodytip"),
          },
        ],
      },
      params_columns: [],
      params_tags: [],
      mapType: "value",
      extractAddStatus: false,
      mappingTypes: ["value", "generator", "join", "format", "sum", "expr"],
      st_columnLists: [
        "Name",
        "Type",
        "Expression",
        "Sample Output1",
        "Sample Output2",
      ],
      dialogForm: {
        st_name: "",
      },
      dialogRules: {
        st_name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      },

      showCreateDIalog: false,
      stableLists: [],
      sruleForm: {
        s_name: "",
      },
      srules: {
        s_name: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      },
      uploadData: {
        req_id: new Date().getTime(),
      },
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      payloads: ["json", "csv"],
      fileList: [],
      ruleForm: {
        payload: "csv",
        file: "",
      },
      rules: [
        {
          payload: [
            {
              required: true,
              trigger: "blur",
            },
          ],
        },
      ],
      indentifiedColumns: [],
      columnsArr: [],
      tableData: [],
      extractArr: [],
      filterArr: [
        {
          expression: "",
          key: Math.random(),
        },
      ],
      currentCol: "",
      mappingParser: {},
    };
  },
  mounted() {
    this.initColumnLists();
    if (this.$parent.isEditable) {
      this.echoParser(this.$store.state.app.transformerParserData);
    }

    this.getInitStables();
  },
  methods: {
    //编辑回显数据
    async echoParser(value) {
      this.msgForm.msgbody =
        this.$store.state.app.currentDBType == "mqtt"
          ? value.input.map((item) => item.payload).join(";")
          : value.input.map((item) => item.value).join(";");
      Object.entries(value.parser.parse).map((item) => {
        let ind = this.columnsArr.findIndex((col) => col.name == item[0]);
          if (ind > -1) {
            this.$set(this.columnsArr[ind], "show", false);
          }
          let obj = {
            columnname: item[0],
            expression:Object.values(item[1]).flat(1).join(';') ,
            type: Object.keys(item[1]).toString(),
            columns: this.columnsArr,
          };
          this.extractArr.push(obj);
      });
      this.$store.commit("app/SET_EXTRACT_PARSE_DATA", value.parser.parse);
      let echoMapData = [];
      value.parser.mutate.forEach((item) => {
        if (Object.keys(item).toString() == "filter") {
          let obj = {
            expression: item.filter,
            key: Math.random(),
          };
          this.filterArr.splice(0, this.filterArr.length, obj);
        }
        if (Object.keys(item).toString() == "map") {
          echoMapData = Object.entries(item["map"]).map((val) => {
            return {
              columnname: val[0],
              type: Object.keys(val[1]).toString(),
              expression: Object.values(val[1]).toString(),
            };
          });
        }
      });
      this.$store.commit("app/SET_ECHO_MAP_DATA", {
        model: value.parser.model,
        tableData: echoMapData,
      });
      this.$nextTick(() => {
        this.$refs.extract.map(comp=>{
            comp.submitExtract()
        })
        // this.$refs.extract[this.$refs.extract.length - 1].submitExtract();
        this.$refs.filter[0].submitFilter();
      });

      this.sruleForm.s_name = value.parser.model.using;
      await this.getSTbaleList();
      await this.echoFetchMap();
    },
    //初始化列下拉框数据，适用于新增和编辑，拷贝
    initColumnLists() {
      this.$set(
        this,
        "indentifiedColumns",
        this.parserColumns.map((item) => {
          return {
            ...item,
            show: true,
          };
        })
      );
      this.$set(
        this,
        "columnsArr",
        this.parserColumns
          .filter((val) => ["varchar", "nchar"].includes(val.type))
          .map((item) => {
            return {
              ...item,
              show: true,
            };
          })
      );
    },
    //messagebody非空验证触发
    validateMsgBody() {
      this.$refs.msgForm.validate((valid) => {
        if (valid) {
          return true;
        } else {
          return false;
        }
      });
    },
    //计算mapping的结果
    caculateMappingResult() {
      if (!this.msgForm.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        Message.error(this.$t("datasource.transformer.tablenametip"));
        return;
      }
      let tags = [];
      let columns = [];
      let mutates = [];
      let mutateMap = {};
      let primarykey = "";
      this.tableData.forEach((item) => {
        if (item["Expression"]) {
          if (
            this.params_columns.includes(item["Name"]) &&
            item["Type"] != "TIMESTAMP"
          ) {
            columns.push(item["Name"]);
          }
          if (item["Type"] == "TIMESTAMP") {
            primarykey = item["Name"];
          }
          if (this.params_tags.includes(item["Name"])) {
            tags.push(item["Name"]);
          }
          let key = Array.isArray(item.maptype[1])
            ? item.maptype[1][0] == "mapping"
              ? "cast"
              : item.maptype[1][1]
            : !this.mapExpressionList.includes(item.maptype[1])
            ? "cast"
            : item.maptype[1]; //此处处理了编辑回显
          if (item.maptype[1] != "string") {
            //排除第一行的tablename
            mutates.push({
              [`${item["Name"]}`]: {
                [`${key}`]: item["Expression"],
              },
            });
          }
        }
      });
      mutates.forEach((item) => {
        Object.assign(mutateMap, item);
      });
      columns.unshift(primarykey);
      let parserData = {
        parser: {
          parse: this.$store.state.app.transformExtractParseData,
          model: {
            name: this.tableData[0]["Expression"],
            using: this.sruleForm.s_name,
            tags: tags,
            columns: columns,
          },
          mutate:this.$store.state.app.transformerFilterParseData?[].concat({
            map: mutateMap,
          }).concat({
            filter:Object.values(this.$store.state.app.transformerFilterParseData).toString()
          }) : [].concat({
            map: mutateMap,
          }),
        },
        input: [].concat(this.generateInput()),
      };
      this.mappingParser = parserData;
      console.log('查询mapping的参数',parserData,this.$store.state.app.transformerFilterParseData)
      this.getParserData(parserData);
    },
    changeMapColumn(scope) {
      if (scope.row.maptype[1][0] == "mapping") {
        this.enable = false;
        this.$set(
          this.tableData[scope.$index],
          "Expression",
          scope.row.maptype[1][1]
        );
      } else {
        this.enable = true;
      }
    },
    //给filter赋值
    changeFilter(key, value) {
      let index = this.filterArr.findIndex((val) => val.key == key);
      this.$set(this.filterArr[index], "expression", value);
    },
    //extract的expression赋值
    changeExtractExpr(colname, value) {
      let index = this.extractArr.findIndex(
        (item) => item.columnname == colname
      );
      this.$set(this.extractArr[index], "expression", value);
    },
    //获取transformer的所有参数
    getTransformerParams() {
      //  不删除，不确定mapping表格是否计算过才能创建新任务

      // let caculateRows = this.tableData.filter(item => item["Expression"]);
      // let mutate = this.filterArr
      //   .map(item => {
      //     return {
      //       filter: item.expression
      //     };
      //   })
      //   .concat(
      //     caculateRows.map(val => {
      //       return { [`${val["maptype"]}`]: val["Expression"] };
      //     })
      //   );

      if (!this.mappingParser.parser) {
        Message.warning(this.$t("datasource.transform.mapcaculate"));
        return;
      }
      let extractObj = {};
      this.extractArr.forEach((item) => {
        extractObj[item.columnname] = {
          [`${item.type}`]:
            item.type == "regex" ? item.expression : item.expression.split(";"),
        };
      });
      let parserData = {
        parser: {
          parse: extractObj,
          model: this.mappingParser.parser.model,
          mutate: this.mappingParser.parser.mutate.some(key=>Object.keys(key).toString()=='filter')?this.mappingParser.parser.mutate:
          this.filterArr
            .map((item) => {
              return {
                filter: item.expression,
              };
            })
            .concat(this.mappingParser.parser.mutate),
        },

        input: [].concat(this.generateInput()),
      };
      this.$emit("getTransformerParams", parserData);
    },
    changeColumnStatus(index, name) {
      //选中的列不能再选中
      let ind = this.columnsArr.findIndex((item) => item.name == name);
      this.$set(this.columnsArr[ind], "show", false);
      this.extractAddStatus = this.columnsArr.every((item) => !item.show);
      this.$set(this.extractArr[index], "columnname", name);
    },
    async getParserData(data) {
      try {
        let result = await getParser(data);
        if (result.message) {
          Message.error(result.message);
          return;
        }
        let outputColumns = result[0].fields.map((item) => item.name);
        let outputTBData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, data[index]];
            })
          );
        });
        let overlapColumns = [];
        this.tableData
          .map((val) => val["Name"])
          .forEach((item) => {
            if (outputColumns.includes(item)) {
              overlapColumns.push(item);
            }
          });
        this.tableData.map((item) => {
          if (overlapColumns.includes(item["Name"])) {
            outputTBData.map((val, index) => {
              item[`Sample Output` + (index + 1)] = val[item["Name"]];
            });
          }
        });
        this.tablekey = Math.random();
      } catch (error) {
        console.log(error);
      }
    },
    //输出input结果
    generateInput() {
      let inputList = [];
      inputList = this.msgForm.msgbody.split(";").map((msg) => {
        let inputobj = {};
        this.indentifiedColumns.forEach((item) => {
          if (this.$store.state.app.currentDBType == "mqtt") {
            if (item.name == "payload") {
              inputobj["payload"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp" ? getRFC3339Time() : item.name;
            }
          } else if (this.$store.state.app.currentDBType == "kafka") {
            if (item.name == "value") {
              inputobj["value"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp" ? getRFC3339Time() : item.name;
            }
          }
        });
        return inputobj;
      });
      return inputList;
    },
    submitSuper(data) {
      if (!this.msgForm.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        Message.error(this.$t("datasource.transformer.tablenametip"));
        return;
      }
      this.currentCol = data["Name"];

      let parserData = {
        parser: {
          parse: this.$store.state.app.transformExtractParseData,
          model: {
            name: this.tableData[0]["Expression"],
            using: this.sruleForm.s_name,
            tags: this.params_tags.includes(data["Name"])
              ? [].concat(data["Name"])
              : [],
            columns: this.params_columns.includes(data["Name"])
              ? [].concat(data["Name"])
              : [],
          },
          mutate: [
            {
              map: {
                [`${data["Name"]}`]: {
                  [`${data.maptype}`]: data["Expression"].split(";").toString(),
                },
              },
            },
          ],
        },
        input: [].concat(this.generateInput()),
      };

      this.getParserData(parserData);
    },
    closeDialog() {
      this.dialogForm.st_name = "";
      this.showCreateDIalog = false;
    },
    //创建或者查询
    async createST() {
      try {
        let payload = {
          selected_db: this.$store.state.app.currentDBName,
          stable_form: this.$refs.createstb.stable_form,
        };
        let result = await createStableReq(payload);
        if (result?.desc) {
          Message.error(this.$t(result.desc));
          return;
        }
        Message.success(this.$t("operateSucc"));
        this.sruleForm.s_name = this.$refs.createstb.stable_form.ts_field_name
        this.getInitStables();
        this.closeDialog();
      } catch (error) {
        error.desc ? Message.error(error.desc) : "";
        console.log(error);
      }
    },
    //获取初始化的stables
    async getInitStables() {
      try {
        if (!this.$store.state.app.currentDBName) return;
        let result = await sendSQLReq(
          `show  \`${this.$store.state.app.currentDBName}\`.stables `
        );
        this.$set(this, "stableLists", Array.from(result.data).flat(1));
      } catch (error) {
        console.log(error);
      }
    },
    createStable() {
      if (!this.$store.state.app.currentDBName) {
       return Message.warning(this.$t('pleaseSelect') + " " + this.$t('stream.targetDB'))
      }
      this.showCreateDIalog = true;
    },
    //回显数据调用mapping接口
    echoFetchMap() {
      let echoData = this.$store.state.app.transformEchoMapData;
      if (echoData) {
        //编辑回显
        this.tableData.map((item) => {
          if (
            echoData.tableData.map((v) => v.columnname).includes(item["Name"])
          ) {
            let idx = echoData.tableData.findIndex(
              (val) => val.columnname == item["Name"]
            );
            item.maptype = []
              .concat(item.maptype[0])
              .concat(
                echoData.tableData[idx].type == "cast"
                  ? echoData.tableData[idx].expression
                  : echoData.tableData[idx].type
              );
            item["Expression"] = echoData.tableData[idx].expression;
          }
          return item;
        });
        this.$set(this.tableData[0], "Expression", echoData.model.name);
        this.caculateMappingResult();
      }
    },
    async getSTbaleList() {
      try {
        if (!this.$store.state.app.currentDBName) {
          Message.error(this.$t("datasource.selecttargetdb"));
        }
        let res = await sendSQLReq(
          `desc \`${this.$store.state.app.currentDBName}\`.\`${this.sruleForm.s_name}\``
        );
        if (res.desc) {
          Message.error(res.desc);
          return;
        }
        this.params_columns.splice(0, this.params_columns.length - 1);
        this.params_tags.splice(0, this.params_tags.length - 1);
        this.tableData = res.data.map((val, index) => {
          if (!val[3] && index > 0) {
            this.params_columns.push(val[0]); //存储非逐渐列
          }
          if (val.includes("TAG")) {
            this.params_tags.push(val[0]);
          }
          return {
            Name: val[0],
            Type: val[1],
            maptype: ["expression", "value"],
            Expression: "",
            "Sample Output1": "",
            "Sample Output2": "",
          };
        });
        this.tableData.unshift({
          Name: this.sruleForm.s_name,
          Type: "Tablename",
          maptype: ["expression", "string"],
          Expression: "",
          "Sample Output1": "",
          "Sample Output2": "",
        });
        this.params_columns.unshift(res.data[0][0]);
      } catch (error) {
        console.log(error);
      }
    },

    //新增extract
    addNewExtract() {
      this.extractArr.push({
        columns: this.columnsArr,
        columnname: "",
        expression: "",
        type: "",
      });
    },
    //新增filter
    addNewFilter() {
      this.filterArr.push({
        expression: "",
        key: Math.random(),
      });
    },
    //删除filter
    deleteFilter(key) {
      let ind = this.filterArr.findIndex((val) => val.key == key);
      this.filterArr.splice(ind, 1);
    },
    deleteExtract(index, name) {
      if (name) {
        let ind = this.extractArr.findIndex((item) => item.columnname == name);
        this.extractArr.splice(ind, 1);
        let restoreIndex = this.columnsArr.findIndex(
          (item) => item.name == name
        );
        this.$set(this.columnsArr[restoreIndex], "show", true);
      } else {
        this.extractArr.splice(index, 1);
      }
    },
  },
  watch: {
    "$store.state.app.transformerParserData": {
      deep: true,
      handler(val) {
        console.log(val, "监听transform数据回显");
      },
    },
    "$store.state.app.transformerMapCloumns": {
      deep: true,
      handler(val) {
        this.$set(this, "options", val);
      },
    },
    
    "$store.state.app.currentDBName": {
      deep: true,
      handler(val) {
        this.getInitStables();
      },
    },
    parserColumns: {
      deep: true,
      handler(val) {
        this.initColumnLists();
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.mapping {
  font-size: 16px;
  font-weight: 600;
  color: #4259ce;
  margin-bottom: 15px;
}
.block-title {
  margin-top: 25px;
  margin-bottom: 15px !important;
}
.extract {
  .el-button {
    width: 100%;
    margin-top: 20px;
  }
}
.col-list {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  column-gap: 15px;
  row-gap: 20px;
  max-height: 200px;
  overflow-y: auto;
  li {
    color: #4259ce;
    background: #ecf2fe;
    border-radius: 14px;
    border: 1px solid #f6f8fa;
    text-align: center;
  }
}
.filter {
  .el-button {
    width: 100%;
  }
  ::v-deep .el-input {
    margin-left: 0px !important;
  }
}
.table-title {
  display: flex;
  white-space: nowrap;
  align-items: center;
  .title {
    display: flex;
    align-items: center;
    flex: 1;
    .el-form-item {
      margin-bottom: 0px;
    }
    .el-select {
      width: 100%;
    }
    .el-form {
      flex: 1;
      margin-right: 15px;
      margin-left: 15px;
    }
  }
}
.table-detail {
  margin-top: 20px;
  .mapping {
    display: flex;
    justify-content: space-between;
  }
  .el-table {
    thead tr th:first-child {
      div {
        visibility: hidden;
      }
    }
  }
}
.payload-upload {
  .el-select {
    width: 100%;
  }
}
.upload-demo {
  display: flex;
  align-items: baseline;
}
.buttons {
  display: flex;
  justify-content: center;
  align-items: center;
  .el-button {
    width: 60px;
  }
}
</style>
