<template>
  <div class="gird">
    <!-- 列表 -->
    <el-table
      v-loading="loading"
      :key="key"
      stripe
      tooltip-effect="light"
      @cell-dblclick="handleCellDblclick"
      size="mini"
      v-load-more.expand.immediate="{
        func: load,
        func1: loadLeft,
        target: '.el-table__body-wrapper',
        delay: 200,
        distance: 100,
      }"
      :data="currentTableData"
      height="100%"
      style="border-bottom: none"
    >
      <!--数据源-->

      <template v-if="currentHead.length">
        <el-table-column
          v-for="(item, index) in currentHead"
          :key="item + index"
          :prop="item"
          :show-overflow-tooltip="true"
          :label="item"
        >
          <template slot-scope="{ row }">
            <el-tooltip :content="$t('console.cellCopyTip')" :open-delay='1000'>
              <span>{{ row[index] }}</span>
            </el-tooltip>
          </template>
          <template slot="header">
            <el-tooltip :content="item" placement="top-start">
              <span>{{ item }}</span>
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
      currentCol: 1,
      colSize: 20,
      key: 0,
      currentHead: [],
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
      },
      loading: (state) => state.console.gridLoading
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
    head: {
      handler(val) {
        this.currentHead = val.slice(0, this.colSize)
      },
      immediate: true,
    }
  },
  mounted() {},
  methods: {
    handleCellDblclick(row, column, cell, event) {
      console.log(row, column,'row, column');
      copy(event?.target?.textContent);
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
    loadLeft() {
      if (this.currentHead.length === this.head.length) return;
      this.currentCol++;
      this.currentHead.push(
        ...this.head
          .slice(
            this.colSize * (this.currentCol - 1),
            this.colSize * this.currentCol
          )
      )
    }
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
