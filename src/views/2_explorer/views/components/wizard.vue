<template>
  <div id="wizard">
    <!-- <querybuilder></querybuilder> -->
    <div class="wrap">
      <Genneral
        :general="general"
        @getFromVal="getFromVal"
      ></Genneral>
      <div class="label">WHERE</div>
      <rule-list
        :rules="rules"
        :fields="fields"
        :defaultFields="defaultFields"
        :valueVisible="valueVisible"
        @handleAddRule="handleAddRule"
        @handleIdChange="handleIdChange"
        @handleOperatorChange="handleOperatorChange"
        @handleAddGroup="handleAddGroup"
        @handleDelete="handleDelete"
      ></rule-list>
      <OtherRule
        :otherRule="otherRule"
      ></OtherRule>
      <!-- <el-col class="flexEnd">
        <el-button :disabled="previewBtn" @click="generateSql" size="small"
          >{{ $t('sqlPreview') }}
        </el-button>
        <el-button @click="() => handleSendSQL(rules[0])" size="small" type="primary">运行</el-button>
      </el-col> -->
      <el-dialog
        custom-class="show-topic-sql"
        width="500px"
        append-to-body
        :visible.sync="dialog"
        title="SQL"
      >
        <pre :key="previewSql" v-highlight>
            <code class="language-sql">{{previewSql}}</code>
          </pre>
        <section class="flexEnd">
          <el-button type="primary" size="mini" @click="dialog = false">{{
            $t("confirm")
          }}</el-button>
        </section>
      </el-dialog>
    </div>
  </div>
</template>

<script>
  import Genneral from './querybuilder/components/general.vue'
  import RuleList from './querybuilder/components/ruleList.vue'
  import OtherRule from './querybuilder/components/otherRule.vue'
  import uuid from './querybuilder/utils/uuid'
  import random from './querybuilder/utils/random'
  import formatQuery from './querybuilder/utils/formatQuery'
  import { proprocess_sql } from "../../utils/preProcessSQL";
  import {
  TDengineStringType,
  TDengineNumberType,
  CompareOperator,
  JsonOperator,
  GeneralOperator,
  RegularOperator
} from "@/const";

const getGeneralFn = (type) => {
  return GeneralOperator.filter(
    (item) => !type.includes(item.label ) 
  ).map((item) => item.label);
};
const conditionMap = {
  TIMESTAMP: CompareOperator.concat(getGeneralFn(["TIMESTAMP"])),
  NUMBER: CompareOperator.concat(getGeneralFn(["NUMBER"])),
  STRING: RegularOperator.concat(getGeneralFn(["STRING"])),
  JSON: JsonOperator,
  BOOL: CompareOperator.concat(getGeneralFn(["NOT BETWEEN AND", "BETWEEN AND" ])),
};
  export default {
    name: 'Wizard',
    data () {
    return {
      dialog: false,
      previewSql: "",
      count: 0,
      valueVisible: {},
      general: {
        dbname: '',
        tbName: [],
        fields: ''
      },
      fromVal: '',
      otherRule: {
        limit: 1000,
        offset: 0,
      },
      rules: [{
        combinator: 'AND',
        id: 0,
        key: uuid(),
        rules: [
          {
            field: 'start time',
            key: uuid(),
            operator: '>=',
            value: '',
            operators: []
          },
          {
            field: 'end time',
            key: uuid(),
            operator: '<',
            value: '',
            operators: []
          }
        ]
      }],
      defaultFields: [
        {
          name: 'start time',
          field: 'start time'
        },
        {
          name: 'end time',
          field: 'end time'
        }
      ]
    }
  },
  computed: {
    fields() {
      let fields = this.$store.state.console.fields
      return fields
    }
  },
  mounted () {
    this.init()
  },
  watch: {
    rules: {
      handler (newV, oldV) {
        if (this.params.handleChange && this.count > 0) {
          const rules = JSON.parse(JSON.stringify(this.rules));
          this.deleteKeys(rules)
          this.params.handleChange(rules)
        }
        this.count++
      },
      deep: true
    },
    general: {
      handler(newObj) {
        if (
          newObj.dbname &&
          newObj.tbName.length > 0 && 
          newObj.fields
      ) {
        this.$store.commit('console/SET_PREVIEW_BTN',false)
      } else {
        this.$store.commit('console/SET_PREVIEW_BTN',true)
      }
      },
      deep: true
    }
  },
  methods: {
    deleteKeys (rules) {
      rules.forEach(item => {
        delete item.key;
        if (item.rules) this.deleteKeys(item.rules)
      })
    },
    init () {
      const { fields, rules } = this.params

      if (fields) {
        this.fields = fields
      }
      if (rules) {
        this.rules = rules
        this.generateKey(this.rules)
      }
    },
    generateKey (rules) {
      rules.forEach(item => {
        item.key = uuid()
        if (item.rules) this.generateKey(item.rules)
      })
    },
    deleteRulesById (rules, key) {
      rules.forEach((item, itemIndex) => {
        if (item.key === key) {
          rules.splice(itemIndex, 1)
        }
        if (item.rules) this.deleteRulesById(item.rules, key)
      })
    },
    handleDelete (val) {
      this.deleteRulesById(this.rules, val)
    },
    addRulesById (rules, id) {
      rules.forEach(item => {
        if (item.id === id) {
          item.rules.push({
            field: '',
            operator: '',
            value: '',
            key: uuid()
          })
        }
        if (item.rules) this.addRulesById(item.rules, id)
      })
    },
    addGroupById (rules, id) {
      rules.forEach(item => {
        if (item.id === id) {
          item.rules.push({
            combinator: 'OR',
            id: random(),
            key: uuid(),
            rules: [{
              field: '',
              key: uuid(),
              operator: '',
              value: ''
            }]
          })
        }
        if (item.rules) this.addGroupById(item.rules, id)
      })
    },
    handleAddGroup (id) {
      this.addGroupById(this.rules, id)
    },
    idChange (rules, key, type) {
      rules.forEach(item => {
        if (item.key === key) {
          if (['RangePicker'].includes(type)) {
            item.value = [undefined, undefined]
          } else {
            item.value = ''
          }
          item.operateType = type,
          item.operators = conditionMap[this.getType(type)]
        }
        if (item.rules) this.idChange(item.rules, key, type)
      })
    },
    handleIdChange (id, key) {
      const { type } = this.fields.find(item => item.name === id)
      this.idChange(this.rules, key, type)
    },
    operatorChange (rules, key, operatorId) {
      // rules.forEach(item => {
      //   if (item.key === key) {
      //     if ([11, 12].includes(operatorId)) {
      //        item.operateType = 'Between'
      //     }
      //   }
      //   if (item.rules) this.operatorChange(item.rules, key, operatorId)
      // })
    },
    handleOperatorChange (id, key) {
      // 暂时不处理
      // const rst = this.operators.find(item => item === id)
      // this.operatorChange(this.rules, key, rst.id)
    },
    handleAddRule (id) {
      this.addRulesById(this.rules, id)
    },
    getType(type, fnType) {
      if (this.avgFn && fnType == "result") return "AVGFN";
      if (!type) return "";
      type = type.replace(/\(\d+\)/, "");
      if (TDengineStringType.includes(type)) return "STRING";
      if (TDengineNumberType.includes(type)) return "NUMBER";
      return type;
    },
    getFromVal(val) {
      this.fromVal = val
    },
    validateRules(rules) {
      for (let index = 0; index < rules.length; index++) {
        const { value, value1, value2, field, operator } = rules[index];
        const isField = Boolean(field)
        const isValue = Boolean(value)
        const isValue1 = Boolean(value1)
        const isValue2 = Boolean(value2)

        if (['BETWEEN', 'NOT BETWEEN'].includes(operator)) {
          if (isField && (!isValue1 || !isValue2)) {
            this.$message.error(this.$t('console.enterTip').replace('{value}',field));
            return false
          }
        }
    
        if ((isField && !isValue) && !['IS NULL', 'IS NOT NULL'].includes(operator)) {
          // 选择字段没有值
          this.$message.error(this.$t('console.enterTip').replace('{value}',field));
          return false
        } 
        // else if (!isField) {
        //   this.$message.error('请输入规则字段')
        //   return false
        // }
        if (rules[index].rules) this.validateRules(rules[index].rules)
      }
    },
    generateSql() {
      const query = this.rules[0]
      let sql = ''
      sql = `SELECT ${this.general.fields} FROM ${this.fromVal}`
      let condition = formatQuery(query)

      if (condition) {
        sql += ` WHERE ${condition}`
      }
      if (this.otherRule) {
        for (const key in this.otherRule) {
          if (Object.hasOwnProperty.call(this.otherRule, key)) {
            const value = this.otherRule[key];
            if (key == 'limit' && !value) {
              return this.$message.error(this.$t('console.enterTip').replace('{value}',key))
            }
            if (value) {
              sql += ` ${key} ${value}`
            } 
          }
        }
      }
      return sql
    },
    getPreviewSql() {
      this.dialog = true
      this.previewSql = this.generateSql()
    },
    async handleSendSQL() {
      // const query = {
      //     rules: [
      //       {
      //         "field": "firstName",
      //         "value": "Stev",
      //         "operator": "beginsWith"
      //       },
      //       {
      //         "field": "lastName",
      //         "value": "Vai, Vaughan",
      //         "operator": "in"
      //       },
      //       {
      //         "field": "age",
      //         "value": "28",
      //         "operator": ">"
      //       },
      //       {
      //         "rules": [
      //           {
      //             "field": "isMusician",
      //             "value": true,
      //             "operator": "="
      //           },
      //           {
      //             "field": "instrument",
      //             "value": "Guitar",
      //             "operator": "="
      //           }
      //         ],
      //         "combinator": "and"
      //       },
      //       {
      //         "field": "groupedField1",
      //         "value": "groupedField4",
      //         "operator": "=",
      //         "valueSource": "field"
      //       },
      //       {
      //         "field": "birthdate",
      //         "value": "1954-10-03,1960-06-06",
      //         "operator": "between"
      //       }
      //     ],
      //     combinator: "or",
      //     not: false
      //   }
      const query = this.rules[0]
      let sql = ''
      if (this.general) {
        for (const key in this.general) {
          if (Object.hasOwnProperty.call(this.general, key)) {
            const value = this.general[key];
            if (!value) {
              return this.$message.error(this.$t('console.enterTip').replace('{value}',key))
            }
          }
        }
      }
      sql = `SELECT ${this.general.fields} FROM ${this.fromVal}`

      // 校验 _c0 为必填项 
      if (!this.validateRules(query.rules)) {
        return 
      }
      
      let condition = formatQuery(query)
      console.log('result',condition);
      if (condition) {
        sql += ` WHERE ${condition}`
      }
      if (this.otherRule) {
        for (const key in this.otherRule) {
          if (Object.hasOwnProperty.call(this.otherRule, key)) {
            const value = this.otherRule[key];
            if (key == 'limit' && !value) {
              return this.$message.error(this.$t('console.enterTip').replace('{value}',key))
            }
            if (value) {
              sql += ` ${key} ${value}`
            } 
          }
        }
      }
      console.log('sql',sql);

      if (this.requestIng) return;
      this.requestIng = true;
      let sqlStr = sql
      let { isSendSQL, updated_sqlStr } = await proprocess_sql(sqlStr); // 预处理要执行的sql语句
      
      if (isSendSQL) {
        await this.$store.dispatch("console/sendConsoleSQL", updated_sqlStr);
      }
      this.requestIng = false;
    }
  },
  props: {
    params: {
      type: Object,
      default: () => {
        return {}
      }
    }
  },
  components: {
    RuleList,
    Genneral,
    OtherRule
  }
  }
</script>

<style lang="scss">
    #wizard {
      width: 100%;
      height: 45vh;
      /* height: 24vh; */
      flex-shrink: 0;
      display: flex;
      flex-direction: column;
      position: relative;
      padding: 0 15px 20px;
      margin-top: 8px;
      overflow: auto;
    }
    .wrap {
      padding: 15px;
      background: #fff;
    }
    .label {
      font-size: 14px;
      color: #4259ce;
      align-items: center;
      width: 120px;
      display: block;
      line-height: 32px;
    }

    .el-cascader-menu:first-child {
      .el-radio {
        display: none;
      }
    }
    .language-sql {
      white-space: normal;
      word-break: break-all;
      word-wrap: break-word;
    }
</style>