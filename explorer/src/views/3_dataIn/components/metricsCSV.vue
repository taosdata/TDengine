<template>
  <div style="padding-bottom: 20px" v-loading="loading">
  
    <el-table :data="csvFiles" size="mini" stripe border>
      <el-table-column
        prop="path"
        show-overflow-tooltip
        :label="$t('dataIn.csvTableHeader.fileName')"
        min-width="240"
      />
      <el-table-column
        prop="startTime"
        sortable
        align="center"
        show-overflow-tooltip
        :label="$t('dataIn.csvTableHeader.startTime')"
        min-width="120"
      />
      <el-table-column
        prop="endTime"
        align="center"
        show-overflow-tooltip
        :label="$t('dataIn.csvTableHeader.endTime')"
        min-width="120"
      />
      <el-table-column
        prop="status"
        sortable
        align="center"
        show-overflow-tooltip
        :label="$t('dataIn.csvTableHeader.status')"
        width="120"
      >
        <template slot-scope="{ row }">
          <span>{{ $t('dataIn.csvFileStatus.' + row.status) }}</span>
        </template>
      </el-table-column>
      <el-table-column
        prop="amount"
        sortable
        align="right"
        show-overflow-tooltip
        :label="$t('dataIn.csvTableHeader.rows')"
        width="120"
      />
    </el-table>
  </div>
</template>

<script>
import { getCSVProgress } from '@/api/explorer/datain';
export default {
  props: {
    taskId: {
      type: Number,
    },
  },
  components: {},
  data() {
    return { 
      csvFiles: [],
      loading: true,
    };
  },
  async mounted() {
    try {
      this.csvFiles = await getCSVProgress(this.taskId);
      this.csvFiles.forEach(item => {
        item.fileName = item.path.split('/').pop();
        item.startTime = item.start_time ? item.start_time.substring(0, 19) : '';
        item.endTime = item.end_time ? item.end_time.substring(0, 19) : '';
      });
    } catch (error) {
      console.error("load csv file error:" + error);
    } finally {
      this.loading = false;
    }
  },
};
</script>

<style scoped lang="scss">

</style>
