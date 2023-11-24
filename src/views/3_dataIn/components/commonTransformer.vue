<template>
  <div class="common-transformer">
    <template>
      <el-button type="danger" @click="getTransformerParams">点击</el-button>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.msgbody") }}</span>
        </div>
        <el-input v-model="msgbody" size="small"></el-input>
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
            :key="index"
            :itemData="item"
            :index="index"
            :payload="msgbody"
            :extractColumns="item.columns"
            :indentifiedColumns="indentifiedColumns"
            @deleteExtract="deleteExtract"
            @selectColumn="changeColumnStatus"
            @changeExtractExpr="changeExtractExpr"
          ></ExtractSplit>
        </template>

        <el-button type="primary" size="small" @click="addNewExtract">{{
          $t("add")
        }}</el-button>
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
            :payload="msgbody"
            :inputparamsColumns="columnsArr"
            :indentifiedColumns="indentifiedColumns"
            @deleteFilter="deleteFilter"
            @changeFilter="changeFilter"
          ></FilterExpression>
        </template>
        <el-button type="primary" size="small" @click="addNewFilter">{{
          $t("add")
        }}</el-button>
      </section>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.superconfig") }}</span>
        </div>
        <div class="table-content">
          <div class="table-title">
            <div class="title">
              <span style="color: #4259ce">{{
                $t("datasource.transformer.targetSt")
              }}</span>
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
            <el-button type="primary" size="small" @click="createStable">{{
              $t("datasource.transformer.createstb")
            }}</el-button>
          </div>
          <div class="table-detail" v-if="tableData.length > 0">
            <div class="mapping">
              {{ $t("datasource.transformer.mapping") }}
              <el-button type="primary" @click="caculateMappingResult"
                >计算结果</el-button
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
                    <!-- <el-select
                      v-model="scope.row.maptype"
                      size="small"
                      style="width: 100px; margin-right: 10px"
                      v-if="
                        scope.row['Type'] != 'Tablename' &&
                        scope.row['Type'] != 'TIMESTAMP'
                      "
                    >
                      <el-option
                        v-for="val in mappingTypes"
                        :key="val"
                        :label="val"
                        :value="val"
                      ></el-option>
                    </el-select> -->
                    <el-cascader
                      size="small"
                      style="width: 100px; margin-right: 10px"
                      :show-all-levels="false"
                      v-model="scope.row.maptype[1]"
                      v-if="scope.row['Type'] != 'Tablename'"
                      @change="changeMapColumn(scope)"
                      :options="options"
                    ></el-cascader>
                    <el-popover
                      trigger="click"
                      placement="right-end"
                      :content="$t('datasource.transformer.searchSResult')"
                    >
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
                    </el-popover>
                  </template>
                </el-table-column>

                <el-table-column
                  v-else
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                >
                </el-table-column>
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
        @close="closeDialog"
      >
        <el-form :model="dialogForm" :rules="dialogRules">
          <el-form-item prop="st_name">
            <el-input v-model="dialogForm.st_name" size="small"></el-input>
          </el-form-item>
        </el-form>
        <div class="buttons">
          <el-button size="small" @click="closeDialog">{{
            $t("cancel")
          }}</el-button>
          <el-button type="primary" size="small" @click="createST">{{
            $t("ok")
          }}</el-button>
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
import {getRFC3339Time} from '@/utils/index'
export default {
  name: "CommonTransformer",
  components: { ExtractSplit, FilterExpression },
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
      enable: true, //只针对ts的expression的input
      timestampExpr: "",
      options: [],
      tablekey: 1,
      msgbody: "",
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
      mappingParser:{}
    };
  },
  mounted() {
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
    this.$set(this.extractArr, 0, {
      columns: this.columnsArr,
      type: "",
      expression: "",
      columnname: "",
    });
    console.log(
      this.parserColumns,
      this.$store.state.app.currentDBName,
      this.columnsArr,
      this.extractArr,
      "parserparserparser"
    );
    this.getInitStables();
  },
  methods: {
    //计算mapping的结果
    caculateMappingResult() {
      if (!this.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        Message.error(this.$t("datasource.transformer.tablenametip"));
        return;
      }
      let tags=[]
      let columns=[]
      let mutates=[]
      let mutateMap={}
      let primarykey=''
      this.tableData.forEach(item=>{
        if(item['Expression']){
            if(this.params_columns.includes(item['Name'])&&item['Type']!='TIMESTAMP'){
                columns.push(item['Name'])
            }
            if(item['Type']=='TIMESTAMP'){
                primarykey=item['Name']
            }
            if(this.params_tags.includes(item["Name"])){
                tags.push(item['Name'])
            }
            let key=Array.isArray(item.maptype[1])?(item.maptype[1][0]=='mapping'?'cast': item.maptype[1][1]):item.maptype[1]
            if(item.maptype[1]!='string'){//排除第一行的tablename
                console.log(item.maptype[1],'item.maptype[1]')
                mutates.push({[`${item["Name"]}`]:{
               [`${key}`]:item['Expression']
            }})
            }
            
        }
      })
      mutates.forEach(item=>{
        Object.assign(mutateMap,item)
      })
      console.log(this.tableData,'this.tableData',primarykey,mutates,mutateMap)
      columns.unshift(primarykey)
      let parser = {
        parser: {
          parse: {},
          model: {
            name: this.tableData[0]["Expression"],
            using: this.sruleForm.s_name,
            tags: tags,
            columns: columns,
          },
          mutate: [].concat({
            map:mutateMap
          }),
        },
        input: [].concat(this.generateInput()),
      };
      this.mappingParser=parser
      this.getParserData(parser);
      console.log(parser,'计算结果')
      
    },
    changeMapColumn(scope) {
        console.log(scope,scope.row.maptype,this.tableData,'kkkk=======----切换类型')
      if (scope.row.maptype[1][0] == "mapping") {
        this.enable = false;
        this.$set(this.tableData[scope.$index],'Expression',scope.row.maptype[1][1])
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
      let extractObj = {};
      let caculateRows = this.tableData.filter((item) => item["Expression"]);
      let mutate = this.filterArr
        .map((item) => {
          return {
            filter: item.expression.split(";").toString(),
          };
        })
        .concat(
          caculateRows.map((val) => {
            return { [`${val["maptype"]}`]: val["Expression"] };
          })
        );

      console.log(caculateRows, mutate, "参与计算的行");
      let parser = {
        parser: {
          parse: Object.assign(
            {},
            this.extractArr.map((item) => {
              return ([item.columnname] = {
                [`${item.type}`]: item.expression.split(";"),
              });
            })
          ),
          mutate: this.filterArr.map(item=>{
            return {
                filter:item.expression.split(';')
            }
          }).concat(this.mappingParser.parser.mutate),
        },
        input: [].concat(this.generateInput()),
      };
      console.log(
        parser,
        this.extractArr,
        this.filterArr,
        this.tableData,
        "所有的参数",
        extractObj
      );
    },
    changeColumnStatus(index, name) {
      //选中的列不能再选中
      let ind = this.columnsArr.findIndex((item) => item.name == name);
      this.$set(this.columnsArr[ind], "show", false);
      this.extractAddStatus = this.columnsArr.every((item) => !item.show);
      this.$set(this.extractArr[index], "columnname", name);
      console.log(this.extractArr, "修改后的", this.extractAddStatus);
    },
    async getParserData(data) {
      console.log("接口调用");
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
        let overlapColumns=[]
        this.tableData.map(val=>val['Name']).forEach(item=>{
            if(outputColumns.includes(item)){
                overlapColumns.push(item)
            }
        })
        this.tableData.map(item=>{
            if(overlapColumns.includes(item['Name'])){
                outputTBData.map((val,index)=>{
                    item[`Sample Output`+(index+1)]=val[item['Name']]
                })
            }
        })
        // let currentindex = this.tableData.findIndex(
        //   (item) => item["Name"] == this.currentCol
        // );
        // outputTBData.splice(0, 2).map((item, index) => {
        //   this.$set(
        //     this.tableData[currentindex],
        //     "Sample Output" + `${index + 1}`,
        //     item[this.currentCol]
        //   );
        // });
        this.tablekey = Math.random();
        // this.$set(this,'tablekey',Math.random())
        console.log(
            overlapColumns,
          outputColumns,
          outputTBData,
          this.tableData,
          "mappingde 结果"
        );
      } catch (error) {
        console.log(error);
      }
    },
    //输出input结果
    generateInput() {
      let inputobj = {};
      this.indentifiedColumns.forEach((item) => {
        if (this.$store.state.app.currentDBType == "mqtt") {
          if (item.name == "payload") {
            inputobj["payload"] = this.msgbody;
          } else {
            inputobj[item.name] = item.type == "timestamp" ? getRFC3339Time() : item.name;
          }
        } else if (this.$store.state.app.currentDBType == "kafka") {
          if (item.name == "value") {
            inputobj["value"] = this.msgbody;
          } else {
            inputobj[item.name] = item.type == "timestamp" ? getRFC3339Time() : item.name;
          }
        }
      });
      return inputobj;
    },
    submitSuper(data) {
      console.log(this.tableData[0], "判断s-----name");
      if (!this.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        Message.error(this.$t("datasource.transformer.tablenametip"));
        return;
      }
      this.currentCol = data["Name"];

      let parser = {
        parser: {
          parse: {},
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
      this.getParserData(parser);
    },
    closeDialog() {
      this.dialogForm.st_name = "";
      this.showCreateDIalog = false;
    },
    //创建或者查询
    async createST() {
      try {
        let result = await sendSQLReq(
          `CREATE STABLE if not exists \`${this.$store.state.app.currentDBName}\`.\`${this.dialogForm.st_name}\` (\`ts\` TIMESTAMP, \`i\` INT) TAGS (\`j\` INT)`
        );
        if (result.desc) {
          Message.error(result.desc);
          return;
        }
        Message.success(this.$t("operateSucc"));
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
      this.showCreateDIalog = true;
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
            maptype: ['expression','value'],
            Expression: "",
            "Sample Output1": "",
            "Sample Output2": "",
          };
        });
        this.tableData.unshift({
          Name: this.sruleForm.s_name,
          Type: "Tablename",
          maptype: ['expression','string'],
          Expression: "",
          "Sample Output1": "",
          "Sample Output2": "",
        });
        console.log(this.tableData, "this.tableData**********");
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
      console.log(this.filterArr, "增加");
    },
    //删除filter
    deleteFilter(key) {
      let ind = this.filterArr.findIndex((val) => val.key == key);
      this.filterArr.splice(ind, 1);
      console.log(this.filterArr, "删除");
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
          val
            .filter((val) => ["varchar", "nchar"].includes(val.type))
            .map((item) => {
              return {
                show: true,
                ...item,
              };
            })
        );

        this.$set(this.extractArr, 0, {
          columns: this.columnsArr,
          type: "",
          expression: "",
          columnname: "",
        });
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
