<template>
  <span class="extra-cus-wrap">
    <template v-for="(item,index) in rules">
      <div class="rules-wrap" v-if="item.combinator" :key="item.key">
        <div class="rules-header">
          <el-row type="flex">
            <el-col :span="12">
              <el-radio-group
                v-model="item['combinator']"
                button-style="solid"
                @change="() => handleCondition(item['combinator'])"
                :size="size"
              >
                <el-radio-button label="AND">And</el-radio-button>
                <el-radio-button label="OR" :disabled="index==0">Or</el-radio-button>
              </el-radio-group>
            </el-col>
            <el-col :span="12">
              <div class="ctl-wrap">
                <el-button type="primary" @click="() => handleAddRule(item.id)" :size="size" :disabled="index==0" >Add Rule</el-button>
                <el-button type="primary" @click="() => handleAddGroup(item.id)" :size="size">Add Group</el-button>
                <el-button v-if="index !== 0" type="danger" @click="() => handleDelete(item.key)" :size="size">Delete</el-button>
              </div>
            </el-col>
          </el-row>
        </div>
        <div class="rules-body">
          <div class="rules-list">
            <template v-if="item.rules">
              <template
                class="rule-container aaa"
                v-for="(ruleItem, ruleItemIndex) in item.rules"
              >
                <div
                  class="rule-container"
                  v-if="!ruleItem.combinator"
                  :key="ruleItemIndex"
                >
                  <el-row type="flex" :gutter="8">
                    <el-col>
                      <div class="id-wrap" v-if="index==0">
                        <el-select
                          v-if="ruleItem.field === 'start time'"
                          @change="() => handleIdChange(ruleItem.field, ruleItem.key)"
                          v-model="ruleItem.field"
                          class="select"
                          :size="size"
                        >
                          <el-option value="start time" label="start time"></el-option>
                        </el-select>
                        <el-select
                          v-else
                          @change="() => handleIdChange(ruleItem.field, ruleItem.key)"
                          v-model="ruleItem.field"
                          class="select"
                          :size="size"
                        >
                          <el-option value="end time" label="end time"></el-option>
                        </el-select>
                      </div>
                      <div class="id-wrap" v-else>
                        <el-select
                          @change="() => handleIdChange(ruleItem.field, ruleItem.key)"
                          v-model="ruleItem.field"
                          class="select"
                          :size="size"
                        >
                          <el-option v-for="idItem in fields" :key="idItem.name" :value="idItem.name" :label="idItem.name"></el-option>
                        </el-select>
                      </div>
                    </el-col>
                    <el-col>
                      <div class="operator-wrap" v-if="index==0">
                        <el-select
                          v-if="ruleItem.field === 'start time'"
                          @change="() => handleOperatorChange(ruleItem.operator, ruleItem.key)"
                          v-model="ruleItem.operator"
                          class="select"
                          :size="size"
                        >
                          <el-option value=">=" label=">="></el-option>
                        </el-select>
                        <el-select
                          v-else
                          @change="() => handleOperatorChange(ruleItem.operator, ruleItem.key)"
                          v-model="ruleItem.operator"
                          class="select"
                          :size="size"
                        >
                          <el-option value="<" label="<"></el-option>
                        </el-select>
                      </div>
                      <div class="operator-wrap" v-else>
                        <el-select
                          @change="() => handleOperatorChange(ruleItem.operator, ruleItem.key)"
                          v-model="ruleItem.operator"
                          class="select"
                          :size="size"
                        >
                          <el-option v-for="operatorItem in ruleItem.operators" :key="operatorItem" :value="operatorItem" :label="operatorItem"></el-option>
                        </el-select>
                      </div>
                    </el-col>
                    <el-col>
                      <div class="value-wrap">
                        <el-date-picker v-if="ruleItem.operator === 'TIMESTAMP'" v-model="ruleItem.value" :size="size"/>
                        <!-- <el-date-picker v-else-if="ruleItem.operateType === 'MonthPicker'" picker="month" v-model="ruleItem.value" :size="size" />
                        <el-range-picker v-else-if="ruleItem.operateType === 'RangePicker'" v-model="ruleItem.value" :size="size"/>
                        <el-select
                          v-else-if="ruleItem.operateType === 'Category'"
                          v-model="ruleItem.value"
                          class="select"
                          :size="size"
                        >
                          <el-option v-for="categoryItem in ruleItem.categoryList" :key="categoryItem.id" :value="categoryItem.id" :label="categoryItem.name"></el-option>
                        </el-select> -->
                        <span class="between-wrap" v-else-if="['BETWEEN', 'NOT BETWEEN'].includes(ruleItem.operator)" :size="size">
                          <el-input
                            v-model="ruleItem.value1"
                            style="width: 100px; text-align: center"
                            placeholder="Minimum"
                            :size="size"
                          />
                          <span>~</span>
                          <el-input
                            v-model="ruleItem.value2"
                            style="width: 100px; text-align: center; border-left: 0"
                            placeholder="Maximum"
                            :size="size"
                          />
                        </span>
                        <!-- <el-rate v-else-if="ruleItem.operateType === 'Rate'" v-model="ruleItem.value" allow-half  :size="size"/> -->
                        <el-input :placeholder="$t(`${ruleItem.placeholder}`)" v-else-if="!['IS NULL', 'IS NOT NULL'].includes(ruleItem.operator)" v-model="ruleItem.value" :size="size" />
                      </div>
                    </el-col>
                    <el-col>
                      <div class="operator-wrap">
                        <el-button v-if="item.rules.length > 1 &&  index > 0" type="danger" @click="() => handleDelete(ruleItem.key)" :size="size">Delete</el-button>
                      </div>
                    </el-col>
                  </el-row>
                </div>
              </template>
            </template>
            <rule-list
              :rules="item.rules"
              :fields="fields"
              :valueVisible="valueVisible"
              @handleAddRule="handleAddRule"
              @handleIdChange="handleIdChange"
              @handleOperatorChange="handleOperatorChange"
              @handleAddGroup="handleAddGroup"
              @handleDelete="handleDelete"
            >
            </rule-list>
          </div>
        </div>
      </div>
    </template>
  </span>
</template>
<script>
export default {
  name: 'RuleList',
  data () {
    return {}
  },
  mounted () {
  },
  emits: ['handleAddRule', 'handleIdChange', 'handleOperatorChange', 'handleAddGroup', 'handleDelete'],
  methods: {
    handleDelete (val) {
      this.$emit('handleDelete', val)
    },
    handleAddRule (val) {
      this.$emit('handleAddRule', val)
    },
    handleAddGroup (val) {
      this.$emit('handleAddGroup', val)
    },
    handleCondition (val) {
      console.log(val)
    },
    handleIdChange (id, key) {
      this.$emit('handleIdChange', id, key)
    },
    handleOperatorChange (id, key) {
      console.log('operator',id,key);
      this.$emit('handleOperatorChange', id, key)
    }
  },
  props: {
    valueVisible: {
      type: Object,
      default: () => {
        return {}
      }
    },
    rules: {
      type: Array,
      default: () => {
        return []
      }
    },
    fields: {
      type: Array,
      default: () => {
        return []
      }
    },
    operators: {
      type: Array,
      default: () => {
        return []
      }
    },
    size: {
      type: String,
      default: 'mini'
    },
    defaultFields: {
      type: Array,
      default: () => {
        return []
      }
    }
  }
}
</script>
<style lang="scss" scoped>
.rules-wrap {
  position: relative;
  padding: 10px;
  padding-bottom: 6px;
  border: 1px solid #ddd;
  background: #f3f4f5;
  margin: 4px 0;
  border-radius: 5px;
  .rules-list,.extra-cus-wrap  {
    list-style: none;
    padding: 0 0 0 15px;
    margin: 0;
    & > :last-child::before {
      border-radius: 0 0 0 4px;
    }
    & > :first-child::before {
      top: -12px;
      height: calc(50% + 14px);
    }
    & > ::before {
      top: -4px;
      border-width: 0 0 2px 2px;
    }
    & > ::before,& > ::after {
      content: '';
      position: absolute;
      left: -16px;
      width: 15px;
      height: calc(50% + 4px);
      border-color: #d9d9d9;
      // border-color: #4259ce;
      border-style: solid;
    }
    & > ::after {
      top: 50%;
      border-width: 0 0 0 2px;
    }
    & > :last-child::after {
      display: none;
    }
    .rule-container {
      position: relative;
      margin: 4px 0;
      border-radius: 5px;
      padding: 5px;
      border: 1px solid #eee;
      background: rgba(255,255,255,.9);
    }
    
  }
  .rules-list {
    .extra-cus-wrap::before {
      border: 0;
    }
    & > :nth-last-child(2)::after {
      display: none;
    }
    & > .extra-cus-wrap :last-child::before {
      top: -46px;
    }

  }
  .id-wrap {
    width: 200px;
  }
  .operator-wrap {
    width: 170px;
  }
  .value-wrap {
    width: 260px;
  }
  .between-wrap {
    display: flex;
    align-items: center;
    justify-content: center;
    & > div {
      flex: auto;
    }
  }
  .rules-header {
    margin-bottom: 10px;
    overflow: hidden;
  }
  .select  {
    width: 100%;
  }
  .ctl-wrap {
    text-align: right;
    button {
      margin: 0 0 0 5px;
    }
  }
}
</style>
