<template>
  <div class="flexStart">
    <el-select
      v-model="data[config.field]"
      :allow-create="true"
      style="flex: 1"
      class="mr20"
      :disabled="loading"
      :placeholder="config.placeholder"
      filterable
      @change="change"
    >
      <el-option
        v-for="item in getOptions()"
        :key="item.value"
        v-bind="item"
      ></el-option>
    </el-select>
  </div>
</template>

<script>
export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    config: {
      type: Object,
      default: () => ({})
    },
    parentConfigList: {
      type: Array,
      default: () => []
    }
  },
  inject: ['sourceParent'],
  components: {},
  data() {
    return {
      loading: false,
      bucketList: []
    };
  },
  computed: {
    field() {
      return this.config.valueField || this.config.field;
    },
    tableConfig() {
      return  this.parentConfigList.find(item => item.field === 'table') ?? {};
    },
    endDateTimeConfig() {
      return  this.parentConfigList.find(item => item.field === 'endDateTime') ?? {};
    },
    beginDateTimeConfig() {
      return  this.parentConfigList.find(item => item.field === 'beginDateTime') ?? {};
    },
    isEdit() {
      return this.sourceParent.isEditable;
    },
    isMigrateMode() {
      return this.data[this.field] === 'migrate'
    }
  },
  watch: {
    data: {
      handler(val) {
        this.beginDateTimeConfig.required = this.data?.table == 'Runtime.dbo.History'
      },
      deep: true
    }
  },
  created() {},
  mounted() {
    if (this.isEdit) {
      this.change('isEditInit')
    }
  },
  methods: {
    getOptions() {
      if (typeof this.config.options === 'function') return this.config.options(this);
      return this.config.options;
    },
    change(val) {
      // 当mode为migrate时，table 只能是 Runtime.dbo.History
      // 开始时间和结束时间都为必填
      const tableList = this.tableConfig.options.map(item => {
        return {
          ...item,
          disabled: item.value === 'Runtime.dbo.Live' && this.isMigrateMode
        }
      })
      if (val !== 'isEditInit') {
        this.data.table = this.isMigrateMode ? 'Runtime.dbo.History' : ''
      }
      this.tableConfig.options = tableList
      this.endDateTimeConfig.required = this.isMigrateMode
    }
  }
};
</script>

<style scoped lang="scss"></style>
