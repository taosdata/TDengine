<template>
  <div>
    <el-table
      size="mini"
      border
      :data="data"
    >
      <el-table-column
        prop="name"
        show-overflow-tooltip
        :label="$t('dataIn.metricName')"
        min-width="280"
      ></el-table-column>
      <el-table-column
        prop="name"
        show-overflow-tooltip
        :label="$t('dataIn.metricValue')"
        min-width="180"
      >
        <template slot-scope="{ row }">
          {{ handleValue(row) }}
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import { parseTime } from '@/utils';
export default {
  props: {
    data: {
      type: Array,
      default: () => []
    }
  },
  components: {},
  data() {
    return {};
  },
  computed: {},
  watch: {},
  created() {},
  mounted() {},
  methods: {
    handleValue(data) {
      if (/timestamp/i.test(data.name) && !isNaN(Number(data.value))) {
        return parseTime(data.value, 'YYYY-MM-DD HH:mm:ss');
      } else {
        return data.value;
      }
    }
  }
};
</script>

<style scoped lang="scss"></style>
