<template>
  <div id="wizard">
    <!-- <querybuilder></querybuilder> -->
    <div class="wrap">
      <Genneral
        :general="general"
        @getFromVal="getFromVal"
      ></Genneral>
      <div class="label">
        WHERE
      </div>
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
        :columnList="fields"
        :general="general"
        :isInterp="isInterp"
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
        :close-on-click-modal="false"
      >
        <pre :key="sql" v-highlight>
            <code class="language-sql">{{sql}}</code>
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
  import { parsinginZone } from "@/utils/index";
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
      sql: "",
      count: 0,
      valueVisible: {},
      isGroupByCondition: false,
      general: {
        dbname: '',
        tbName: '',
        fields: ''
      },
      fromVal: '',
      otherRule: {
        limit: 1000,
        offset: 0,
        orderby: '',
        partitionby: '',
        groupby: '',
        having: '',
        slimit: '',
        soffset: '',
        window_type: '',
        tol_val: "",
        tol_unit: "m",
        interval_val: "",
        interval_offset: "",
        column: "",
        interval_unit: "m",
        offset_unit: "m",
        sliding_val: "",
        sliding_unit: "s",
        range1: '',
        range2: '',
        every_val: '',
        every_unit: 'a',
        fill: 'NONE',
        fill_val: '',
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
            operators: [],
            placeholder: 'console.startTime'
          },
          {
            field: 'end time',
            key: uuid(),
            operator: '<',
            value: '',
            operators: [],
            placeholder: 'console.endTime'
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
    },
    isInterp() {
      // 检测 select 是否包含 interp 函数
      return /interp/i.test(this.general.fields)
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
            this.$error(this.$t('console.enterTip').replace('{value}',field));
            return false
          }
        } else if ((isField && !isValue) && !['IS NULL', 'IS NOT NULL'].includes(operator)) {
          // 选择字段没有值
          this.$error(this.$t('console.enterTip').replace('{value}',field));
          return false
        }  else {
          return true
        }
        // else if (!isField) {
        //   this.$error('请输入规则字段')
        //   return false
        // }
        if (rules[index].rules) this.validateRules(rules[index].rules)
      }
    },
    compareTime(time1,time2) {
      let date1 = new Date(time1)
      let date2 = new Date(time2)
      return date1 <= date2
    },
    generateSql() {
      const query = this.rules[0]
      
      this.sql = `SELECT ${this.general.fields} FROM ${this.fromVal}`
      let condition = formatQuery(query)

      if (condition) {
        this.sql += ` WHERE ${condition}`
      }

      if (this.otherRule.orderby) {
        this.sql += ` ORDER BY ${this.otherRule.orderby}`
      }
      
      if (this.otherRule.partitionby) {
        this.sql += ` PARTITION BY ${this.otherRule.partitionby}`
      }
      
      if (this.otherRule.groupby) {
        this.sql += ` GROUP BY ${this.otherRule.groupby}`
        if (this.otherRule.having) {
          this.sql += ` HAVING ${this.otherRule.having}`
        }
      }

      // slimit
      if (this.otherRule.groupby || this.otherRule.partitionby) {
        if (String(this.otherRule.slimit) != 'undefined') {
          this.sql += ` SLIMIT ${this.otherRule.slimit}`
        }
        if (String(this.otherRule.soffset) != 'undefined') {
          this.sql += ` SOFFSET ${this.otherRule.soffset}`
        }
      }

      // window_clause
      if (this.otherRule.window_type) {
        this.sql += " ";
        const ts_col = this.fields.find(
          (item) => item.type === "TIMESTAMP"
        )?.field;
        switch (this.otherRule.window_type) {
          case "SESSION":
            if (ts_col && this.otherRule.tol_val) {
              this.sql += ` SESSION(${ts_col},${this.otherRule.tol_val}${this.otherRule.tol_unit})`;
            }
            break;
          case "STATE":
            this.sql += this.otherRule.state_column ? ` STATE_WINDOW(${this.otherRule.state_column})` : '';
            break;
          case "INTERVAL":
            if (this.otherRule.interval_val) {
              this.sql += `INTERVAL(${this.otherRule.interval_val}${this.otherRule.interval_unit}`;
              if(this.otherRule.interval_offset){
                this.sql += `,${this.otherRule.interval_offset}${this.otherRule.offset_unit}`
              }
              this.sql +=`)`
            }
            if (this.otherRule.sliding_val) {
              this.sql += ` SLIDING(${this.otherRule.sliding_val}${this.otherRule.sliding_unit})`;
            }
            break;
          case "EVENT":
            if (this.otherRule.start_with && this.otherRule.end_with) {
              this.sql += ` EVENT_WINDOW start with ${this.otherRule.start_with} end with ${this.otherRule.end_with}`
            }
            break;
          default:
            break;
        }
      }

      // interp_clause 
      if (this.isInterp) {
        const { range1, range2, every_val, every_unit, fill, fill_val } = this.otherRule
        this.sql += ''
        // 需要进行校验,必填项 6个 框
        // rang1 <= rang2 
        // 处理range 

        if (!range1) {
          this.$error(this.$t('console.enterTip').replace('{value}','RANGE'))
          return
        }
        if (range1 && !range2) {
          this.sql += ` RANGE('${parsinginZone(range1)}')`
          // 只有rang1， every 可以省略
          if (every_val) {
            this.sql += ` EVERY(${every_val}${every_unit})`
          } 
        } else if (range1 && range2) {
          if (!this.compareTime(range1,range2)) {
            this.$error('RANGE 范围必须是 timestamp1 <= timestamp2')
            return 
          }
          if (every_val) {
            this.sql += ` RANGE('${parsinginZone(range1)}','${parsinginZone(range2)}') EVERY(${every_val}${every_unit})`
          } else {
            this.$error(this.$t('console.enterTip').replace('{value}','EVERY'))
            return 
          }
        }

        if (fill) { 
          if (fill === 'VALUE') {
            if (!fill_val) {
              this.$error(this.$t('console.enterTip').replace('{value}','FILL'))
              return
            }
            this.sql += ` FILL(${fill},${fill_val})`
          } else {
            this.sql += ` FILL(${fill})`
          }
        } else {
          this.$error(this.$t('console.enterTip').replace('{value}','FILL'))
          return 
        }
      }

      // limit 放在最后
      if (String(this.otherRule.limit) != 'undefined') {
        this.sql += ` LIMIT ${this.otherRule.limit}`
      } else {
        this.$error(this.$t('console.enterTip').replace('{value}','LIMIT'))
        return 
      }
      if (String(this.otherRule.offset) != 'undefined') {
        this.sql += ` OFFSET ${this.otherRule.offset}`
      }
      return true
    },
    getPreviewSql() {
      this.dialog = true
      this.generateSql()
    },
    resetWizard() {
      Object.assign(this.$data, this.$options.data())
      this.$store.commit('console/RESET_GRID')
    },
    async handleSendSQL() {
      // 校验 _c0 为必填项 
      const query = this.rules[0]
      if (!this.validateRules(query.rules)) {
        return 
      }

      if (!this.generateSql()) {
        return
      }
      
      if (this.requestIng) return;
      this.requestIng = true;
      this.generateSql()
      let sqlStr = this.sql
      console.log('sql',sqlStr);
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