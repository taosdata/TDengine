<template>
  <el-table
    :data="agentActivities"
    size="mini"
    class="activity-table"
    v-loading="loading"
  >
    <el-table-column
      prop="level"
      :label="$t('dataIn.acstatus')"
      width="150"
    >
      <span
        slot-scope="scope"
        :style="getLevelStyle(scope.row.level)"
      >
        <i
          class="el-icon-warning"
          v-if="scope.row.level == 'warn'"
        ></i>
        <i
          class="el-icon-error"
          v-if="scope.row.level == 'error'"
        ></i>
        <i
          class="el-icon-info"
          v-if="scope.row.level == 'info'"
        ></i>
        {{ scope.row.level }}
      </span>
    </el-table-column>
    <el-table-column
      prop="at"
      :label="$t('dataIn.startTime')"
      width="220"
    >
      <span slot-scope="scope">{{ parseTime(scope.row.at, 'YYYY-MM-DD HH:mm:ss') }}</span>
    </el-table-column>
    <el-table-column
      prop="activity"
      :label="$t('dataIn.currentac')"
    ></el-table-column>
    <el-table-column
      prop="context"
      :label="$t('dataIn.acctx')"
    ></el-table-column>
  </el-table>
</template>
<script>
import { parseTime } from '@/utils';
// import { getAgentActivities } from '@/api/agent';
// import { getTaskActivities } from '@/api/dataSource';
export default {
  props: {
    type: {
      type: String,
      default: 'task'
    },
    id: {
      type: Number,
      default: -1
    },
    selectId: {
      type: Number,
      default: -1
    }
  },
  components: {},
  data() {
    return {
      loading: false,
      expandRowKeys: [],
      agentActivities: []
    };
  },
  // watch: {
  //   selectId: {
  //     handler() {
  //       this.getActivities();
  //     },
  //     immediate: true
  //   }
  // },
  computed: {},
  created() {},
  mounted() {
    this.getActivities();
  },
  methods: {
    parseTime,
    async getActivities() {
      if (this.id !== this.selectId) {
        this.agentActivities = [];
        return;
      }
      let getFn;
      if (this.type === 'agent') {
        // getFn = getAgentActivities;
      } else if (this.type === 'task') {
        // getFn = getTaskActivities;
      }
      if (!getFn || this.selectId < 0) {
        return;
      }
      this.loading = true;
      this.agentActivities = [];
      const res = await getFn(this.id);
      if (!res) {
        this.loading = false;
        return;
      }
      const activitList = res?.map(item => {
        if (typeof item.context === 'object') {
          item.context = item.context?.message ? item.context.message : '';
        }
        return item;
      });
      this.agentActivities = activitList;
      this.loading = false;
    },
    getLevelStyle(level) {
      let style = '';
      switch (level) {
        case 'info':
          style = 'color: #67c23a';
          break;
        case 'warn':
          style = 'color: #e6a23c';
          break;
        case 'error':
          style = 'color: #fe6c6c';
          break;
      }
      return style;
    }
  }
};
</script>
<style scoped lang="scss">
.activity-table {
  padding-left: 5rem;
  z-index: 100;
  overflow-y: auto;
}
</style>
