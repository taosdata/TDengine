<template>
  <ul class="condition-list">
    <li v-for="(item, index) in condition" :key="item.key">
      <el-input placeholder="" size="mini" v-model="item.value" class="input-with-select">
        <el-select v-model="item.operator" slot="prepend" placeholder="Operator">
          <el-option v-for="ite in conditionList" :disabled="getSelectableCondition(ite)" :key="ite" :value="ite"></el-option>
        </el-select>
        <el-button @click="del(index)" slot="append" icon="el-icon-minus"></el-button>
      </el-input>
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
    .add-btn {
      position: sticky;
      bottom: 0;
    }
  }
</style>
