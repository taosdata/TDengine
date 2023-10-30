<template>
  <div class="gird">
    <!-- 列表 -->
    <el-table
      :key="key"
      stripe
      tooltip-effect="light"
      @cell-dblclick="handleCellDblclick"
      size="mini"
      v-load-more.expand.immediate="{
        func: load,
        target: '.el-table__body-wrapper',
        delay: 200,
        distance: 100,
      }"
      :data="currentTableData"
      height="100%"
      style="border-bottom: none"
    >
      <!--数据源-->

      <template v-if="head.length">
        <el-table-column
          v-for="(item, index) in head"
          :key="item + index"
          :show-overflow-tooltip="true"
          :label="item"
        >
          <template slot-scope="{ row }">
            <el-tooltip :content="$t('console.cellCopyTip')">
              <span>{{ row[index] }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
      </template>
    </el-table>
    <section
      v-if="currentHistory && currentTableData.length"
      class="time-wrapper"
    >
      <div class="time-block">
        <span class="title">{{ $t('execute') }}:</span>
        <span class="value">{{ currentHistory.time }} ms</span>
      </div>
      <!-- <div class="time-block">
        <span class="title">{{ $t('network') }}:</span>
        <span class="value">{{ currentHistory.networkTime }} ms</span>
      </div>
      <div class="time-block">
        <span class="title">{{ $t('total') }}:</span>
        <span class="value">{{ currentHistory.totalTime }} ms</span>
      </div> -->
    </section>
  </div>
</template>
<script>
import { mapState } from "vuex";
import { copy } from '@/utils';
export default {
  name: "grid",
  data() {
    return {
      currentTableData: [],
      currentPage: 1,
      pageSize: 30,
      key: 0,
    };
  },
  components: {},
  computed: {
    ...mapState({
      dataSource: (state) => state.console.result,
      head: (state) => state.console.head,
      currentHistory: state => {
        const currentHistory = state.console.history[state.console.history.length - 1];
        if (currentHistory && currentHistory.type == 1) return currentHistory;
      }
    }),
  },
  watch: {
    dataSource: {
      handler(val) {
        this.key++;
        this.currentTableData = val.slice(0,this.pageSize)
        this.currentPage = 1;
      },
      immediate: true,
    },
  },
  mounted() {},
  methods: {
    handleCellDblclick(row, column) {
      console.log(row, column,'row, column');
      copy(row[column.property]);
    },
    load() {
      if (this.currentTableData.length === this.dataSource.length) return;
      this.currentPage++;
      this.currentTableData.push(
        ...this.dataSource
          .slice(
            this.pageSize * (this.currentPage - 1),
            this.pageSize * this.currentPage
          )
      );
    },
  },
};
</script>
<style lang="scss" scoped>
.gird {
  height: 100%;
  overflow: auto;
  overflow-x: hidden;
  &:deep(.el-table::before) {
    height: 0;
  }
}
.time-wrapper {
    position: absolute;
    bottom: -27px;
    left: 0;
    right: 0;
    .time-block {
      display: inline-block;
      margin-right: 20px;
      line-height: 20px;
      .title {
        font-size: 16px;
        margin-right: 5px;
        color: #4d6992;
      }
      .value {
        font-size: 14px;
        color: #999;
      }
    }
  }
</style>
