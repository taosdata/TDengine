<template>
  <ul class="condition-list">
    <li v-for="(item, index) in condition" :key="item.key">
      <el-select v-model="item.operator" slot="prepend" placeholder="Operator" @change="() => changeOperator(item)">
        <el-option v-for="ite in conditionList" :disabled="getSelectableCondition(ite)" :key="ite" :value="ite"></el-option>
      </el-select>
      <span v-if="['BETWEEN', 'NOT BETWEEN'].includes(item.operator)" class="condition-span">
        <el-input placeholder=""  v-model="item.value"></el-input>
        <span>AND</span>
        <el-input placeholder="" v-model="item.value1"></el-input>
      </span>
      <span v-else class="condition-span">
        <el-input placeholder="" v-model="item.value" :disabled="['IS NULL', 'IS NOT NULL'].includes(item.operator)"></el-input>
      </span>
      <el-button @click="del(index)" slot="append" icon="el-icon-minus"></el-button>
    </li>
    <li class="add-btn">
      <el-button class="w100" icon="el-icon-plus" @click="addCondition"></el-button>
    </li>
  </ul>
</template>

<script>
  export default {
    props: {
      condition: {
        type: Array,
        default: () => [],
      },
      conditionList: {
        type: Array,
        default: () => [],
      },
    },
    components: {},
    data() {
      return {};
    },
    computed: {
      currentOperatorList() {
        return this.condition.map(item => item.operator);
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      del(index) {
        this.condition.splice(index, 1);
      },
      addCondition() {
        this.condition.push({
          key: Date.now(),
          operator: "",
          value: "",
          value1: ""
        });
      },
      getSelectableCondition(operator) {
        switch (operator) {
          case ">":
            return this.currentOperatorList.includes(">=") || this.currentOperatorList.includes(">");
          case ">=":
            return this.currentOperatorList.includes(">") || this.currentOperatorList.includes(">=");
          case "<":
            return this.currentOperatorList.includes("<=") || this.currentOperatorList.includes("<");
          case "<=":
            return this.currentOperatorList.includes("<") || this.currentOperatorList.includes("<=");

          default:
            break;
        }
      },
      changeOperator(options) {
        options.value = ''
        options.value1 = ''
        this.$emit('update:condition',)
      }
    },
  };
</script>

<style scoped lang="scss">
  .condition-list {
    max-height: 300px;
    overflow-y: auto;

    &:deep(.el-select .el-input) {
      width: 120px;
    }
    li + li {
      margin-top: 10px;
    }
    li {
      display: flex;
    }
    .condition-span {
      display: flex;
      width: 206px;
      span {
        flex: none;
        line-height: 32px;
        padding: 0 2px;
      }
    };
    .add-btn {
      position: sticky;
      bottom: 0;
    }
  }
</style>
