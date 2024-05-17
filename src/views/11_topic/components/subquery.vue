<template>
  <div>
    <el-form-item :label="dbLabel" :prop="dbFiled" required>
      <el-select class="w100" @change="dbChange" filterable placeholder="" v-model="info[dbFiled]">
        <el-option v-for="item in dbList" :key="item.name" :value="item.name"></el-option>
      </el-select>
    </el-form-item>
    <slot name="db-bottom"></slot>
    <template v-if="level">
      <el-form-item v-if="level == 1" :label="stbLabel" :prop="stbField" required>
        <el-select
          class="w100"
          placeholder=""
          v-model="info[stbField]"
          :disabled="!info[dbFiled]"
          :default-first-option="true"
          filterable
          :remote-method="searchStable"
          :loading="requestIng"
          @focus="focus(0)"
          remote
        >
          <el-option v-for="item in stableList" :key="item.stable_name" :value="item.stable_name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="level == 2" :label="$t('data.tableName')" prop="tbName" required>
        <el-select
          class="w100"
          placeholder=""
          v-model="info.tbName"
          :disabled="!info[dbFiled]"
          :default-first-option="true"
          filterable
          :remote-method="searchTable"
          :loading="requestIng"
          @focus="focus(1)"
          remote
        >
          <el-option v-for="item in tableList" :key="item.table_name" :value="item.table_name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="(info[stbField] || info.tbName) && fieldSet" prop="resultSet" :label="$t('topic.fieldSet')">
        <ResultSet ref="resultSet" :avgFn="avgFn" :tags.sync="tags" :columns.sync="columns" :params="params" v-model="info.resultSet" />
      </el-form-item>
      <el-form-item v-if="parttion && level == 1" prop="parttionSet" :label="$t('stream.parttionSet')">
        <el-select
          class="w100"
          placeholder=""
          :disabled="!info[stbField]"
          ref="resultSet"
          :tags.sync="tags"
          :params="params"
          multiple
          v-model="info.parttionSet"
        >
          <el-option v-for="item in partitionList" :key="item.field" :value="item.field"></el-option>
        </el-select>
      </el-form-item>
      <WindowClause v-if="windowClause" :window_clause="info" :column-list="columns" />
    </template>
  </div>
</template>

<script>
  import { getDBListReq } from "@/api/gateway/data/dbs";
  import { searchTable } from "@/api/gateway/data/tables";
  import { searchStable } from "@/api/gateway/data/stables";
  import ResultSet from "./resultSet.vue";
  import { isArray } from "@/utils/validate";
  import WindowClause from "./windowClause.vue";
  import { TDengineFnReverseGroup } from '@/const';
  export default {

    name:'Subquery',
    props: {
      info: {
        type: Object,
        default: () => {
          return {
            db_name: "",
            topic_type: "DATABASE",
            stbName: "",
            tbName: "",
            resultSet: [],
          };
        },
      },
      level: {
        type: Number,
        default: 1,
      },
      fieldSet: {
        type: Boolean,
        default: false,
      },
      dbConfig: {
        type: Object,
        default: () => ({}),
      },
      stbConfig: {
        type: Object,
        default: () => ({}),
      },
      parttion: {
        type: Boolean,
        default: false,
      },
      windowClause: {
        type: Boolean,
        default: false,
      },
      avgFn: {
        type: Boolean,
        default: false,
      },
    },
    components: { ResultSet, WindowClause },
    data() {
      return {
        systemFns:['DATABASE','CLIENT_VERSION','SERVER_VERSION','SERVER_STATUS'],
        stableList: [],
        tableList: [],
        dbList: [],
        requestIng: false,
        tags: [],
        columns: [],
      };
    },
    computed: {
      params() {
        const result = {
          selected_db: this.info[this.dbFiled],
        };
        if (this.level == 1) {
          result.stableName = this.info[this.stbField];
        } else {
          result.selected_tb = this.info.tbName;
        }
        return result;
      },
      dbLabel() {
        return this.dbConfig?.label || this.$t("topic.database");
      },
      dbFiled() {
        return this.dbConfig?.filed || "db_name";
      },
      stbLabel() {
        return this.stbConfig?.label || this.$t("topic.stable");
      },
      stbField() {
        return this.stbConfig?.filed || "stbName";
      },
      partitionList() {
        return this.tags.concat([
          {
            field: "tbname",
          },
        ]);
      },
    },
    watch: {},
    created() {
      this.getDBList();
    },
    mounted() {
    },
    methods: {
      getDBList() {
        getDBListReq().then(data => {
          this.dbList = data;
          this.$emit("update:dbList", data);
        });
      },
      dbChange(val) {
        this.info.stbName = "";
        this.info.tbName = "";
        this.$emit("db-change", val);
      },
      focus(type) {
        if (type == 0) {
          !this.info[this.stbField] && this.searchStable("");
        } else {
          !this.info.tbName && this.searchTable("");
        }
      },
      searchStable(query) {
        if (this.requestIng) return;
        this.requestIng = true;
        searchStable(query, this.info[this.dbFiled])
          .then(data => {
            this.stableList = data;
          })
          .catch(err => {
            this.stableList = [];
            err.desc && this.$error(err.desc);
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      searchTable(query) {
        if (this.requestIng) return;
        this.requestIng = true;
        searchTable(query, this.info[this.dbFiled])
          .then(data => {
            this.tableList = data;
          })
          .catch(err => {
            this.tableList = [];
            err.desc && this.$error(err.desc);
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      getResultSet() {
        let resultSet = [];
        const conditionSet = [];
        let isResultSet = false;
        this.info.resultSet.forEach(item => {
          if (!item.checked) return;
          // 处理result
          const result = item.result;
          const condition = item.condition.filter(ite => {
            if (["IS NULL", "IS NOT NULL"].includes(ite.operator)) {
              return ite
            } else if (['BETWEEN', 'NOT BETWEEN'].includes(ite.operator)) {
              return ite.value && ite.value1
            } else {
              return ite.operator && ite.value
            } 
          });
          if (result.fn) {
            isResultSet = true;
            const fnList = item.fnList.map(item=>item.options).flat(1) || [];
            const currentFn = fnList.find(ite => ite.label == result.fn)?.filters || [];
            let otherParmas = "";
            const isReverse = TDengineFnReverseGroup.includes(result.fn);
            if (currentFn.length) {
              otherParmas = currentFn
                .reduce((pre, { field }) => {
                  const value = result.params[field];
                  if (value) {
                    if (isArray(value)) {
                      isReverse ? value.forEach(v => pre.push(JSON.stringify(v))) : pre.push(...value);
                    } else {
                      isReverse ? pre.push(JSON.stringify(value)) : pre.push(value);
                    }
                    return pre;
                  }
                }, [])
                .join(',');
              if (otherParmas) {
                otherParmas = isReverse ? otherParmas + ',' : ',' + otherParmas;
              }
            }
            if(this.systemFns.includes(result.fn)){
              resultSet.push(`${result.fn}()`);
            }else{
              resultSet.push(`${result.fn}(${isReverse ? otherParmas + JSON.stringify(item.name) : item.field + otherParmas})`);
            }
          } else {
            if (!this.avgFn) {
              resultSet.push(item.field);
            }
          }
          // 处理condition
          if (condition.length) {
            conditionSet.push(
              condition
                .reduce((pre, cur) => {
                  if(cur.operator == 'BETWEEN' || cur.operator == 'NOT BETWEEN') {
                    pre.push(`${item.field} ${cur.operator} ${cur.value} AND ${cur.value1}`)
                  } else if(cur.operator == 'IN' || cur.operator == 'NOT IN') {
                    pre.push(`${item.field} ${cur.operator} (${cur.value})`);
                  }else {
                    pre.push(`${item.field} ${cur.operator} ${cur.value}`);
                  }
                  return pre;
                }, [])
                .join(" AND ")
            );
          }
        });
        const name = this.level == 1 ? this.info.stbName : this.info.tbName;
        let result = "";
        if (!isResultSet && (!resultSet.length || resultSet.length == this.info.resultSet.length)) {
          resultSet = this.avgFn ? ["count(*)"] : ["*"];
        }
        result = `SELECT ${resultSet.join(",")} FROM \`${this.info[this.dbFiled]}\`.\`${name}\``;
        if (conditionSet.length) {
          result += ` WHERE ${conditionSet.join(" AND ")}`;
        }
        return result;
      },
    },
  };
</script>

<style scoped lang="scss"></style>
