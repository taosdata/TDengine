<template>
  <div class="dataset-result-table" v-if="showtable" ref="result" :style="{'max-height':defaultHeight, 'top': defaultTop}">
    <div class="title-block">
      <span class="title">{{ $t("datasource.transformer.resulttb") }}</span>
      <span class="title-block">
        <el-tooltip placement="top" effect="light" :open-delay="0">
          <template slot="content">
            {{ $t('fullscreen') }}
          </template>
          <span class='el-icon-full-screen' @click="drawer=true"></span>
        </el-tooltip>
        <span class='el-icon-close' @click="showtable=false"></span>
      </span>
    </div>
    <el-table
      border
      style="width: 100%"
      :max-height="defaultHeight-99"
      ref='table'
      :data="tableData"
      size="medium"
      v-loading="loading"
    >
      <el-table-column
        prop="id"
        show-overflow-tooltip
        label="id"
      >
        <template #header>
          <el-input
            style="width: 80%"
            v-model="searchId"
            size="mini"
            :placeholder="$t('filter')"
            @change="searchInputChange"
          >
            <template slot="prepend">id</template>
          </el-input>
        </template>
      </el-table-column>
      <el-table-column
        prop="name"
        show-overflow-tooltip
        label="name"
      >
        <template #header>
          <el-input
            style="width: 80%"
            v-model="searchName"
            size="mini"
            :placeholder="$t('filter')"
            @change="searchInputChange"
          >
            <template slot="prepend">name</template>
          </el-input>
        </template>
      </el-table-column>
      <el-table-column
        prop="enabled"
        show-overflow-tooltip
        label="enabled"
      >
        <template #header>
          <el-input
            style="width: 80%"
            v-model="searchEnabled"
            size="mini"
            :placeholder="$t('filter')"
            @change="searchInputChange"
          >
            <template slot="prepend">enabled</template>
          </el-input>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="false"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
    <el-drawer
      id="my-drawer"
      :title="$t('datasource.transformer.resulttb')"
      :visible.sync="drawer"
      direction="rtl"
      size="100%">
      <el-table
        border
        style="width: 100%"
        :max-height="fullTableHeight"
        ref='table'
        :data="tableData"
        size="small">
        <el-table-column
          prop="id"
          show-overflow-tooltip
          label="id"
        >
          <template #header>
            <el-input
              style="width: 80%"
              v-model="searchId"
              size="mini"
              :placeholder="$t('filter')"
              @change="searchInputChange"
            >
              <template slot="prepend">id</template>
            </el-input>
          </template>
        </el-table-column>
        <el-table-column
          prop="name"
          show-overflow-tooltip
          label="name"
        >
          <template #header>
            <el-input
              style="width: 80%"
              v-model="searchName"
              size="mini"
              :placeholder="$t('filter')"
              @change="searchInputChange"
            >
              <template slot="prepend">name</template>
            </el-input>
          </template>
        </el-table-column>
        <el-table-column
          prop="enabled"
          show-overflow-tooltip
          label="enabled"
        >
          <template #header>
            <el-input
              style="width: 80%"
              v-model="searchEnabled"
              size="mini"
              :placeholder="$t('filter')"
              @change="searchInputChange"
            >
              <template slot="prepend">enabled</template>
            </el-input>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        class="pagination"
        layout="total, prev, pager, next"
        :current-page.sync="currentPage"
        :page-size="pageSize"
        :hide-on-single-page="false"
        :total="total"
        @current-change="handlePageChange"
      ></el-pagination>
    </el-drawer>
  </div>
</template>
<script>
import { getDatasets } from '@/api/explorer/datain';
import { datasetsField } from '../utils';

export default {
  name: "ResultTable",
  props: {
    isEditable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      loading: true,
      tableData: [],//表格实际展示的数据
      pageSize: 200,
      total: 10,
      currentPage: 1,
      showtable: false,
      defaultHeight: 495,
      defaultTop: '50%',
      searchName: '',
      searchId: '',
      searchEnabled: '',
      drawer: false,
      fullTableHeight: 600,
      list: [],// 全部的点位数据
      filterTableData: [],//增加过滤条件的全部数据
    };
  },
  mounted() {
    this.getDatasetsData
  },
  computed: {
    ticket() {
      return this.$store.state.app.ticket
    }
  },
  watch: {
    "$store.state.app.complete"(val) {
      if (val) {
        // 点击预览需要重置 currentPage 
        this.currentPage = 1
        this.getDatasetsData()
        this.showtable = true
      }
    },
    drawer(val) {
      if (val) {
        this.$nextTick(()=> {
          this.fullTableHeight = this.getFullTableHeight()
        })
      }
    }
  },
  methods: {
    async searchInputChange() {
      this.loading = true;
      this.currentPage = 1;
      if (!this.searchId && !this.searchName && !this.searchEnabled) {
        this.getTableData(this.list);
        this.filterTableData = this.list;
      } else {
        this.filterTableData = await this.list.filter(data => (!this.searchName || data.name.toLowerCase().includes(this.searchName.toLowerCase())) &&
        (!this.searchId || data.id.toLowerCase().includes(this.searchId.toLowerCase())) &&
        (!this.searchEnabled || data.enabled.toString().toLowerCase().includes(this.searchEnabled.toLowerCase())))
        this.getTableData(this.filterTableData)
      }
      this.loading = false;
    },
    handlePageChange(currentPage) {
      this.currentPage = currentPage
      this.getTableData(this.filterTableData)
    },
    getTableData(data) {
      this.total = data.length;
      this.tableData = data.slice(
        this.pageSize * (this.currentPage - 1),
        this.pageSize * this.currentPage
      );
    },
    async getDatasetsData() {
      let res = await getDatasets(this.ticket,this.currentPage,1000000)
      if (res?.code == 0) {
        let { page, page_size, list, total} = res?.data
        this.currentPage = page
        this.list = list;
        this.filterTableData = list;
        this.getTableData(list)
      }
      this.$store.commit("app/SET_COMPLETE",false)
      this.getEleTop()
      this.loading = false
    },
  
    getEleTop() {
      let dom1 = document.getElementById(`${datasetsField}`)
      let dom2 = document.querySelector('.right-ui')
      let rect1 = dom1?.getBoundingClientRect()
      let rect2 = dom2?.getBoundingClientRect()
      this.defaultTop = rect1.top - rect2.top + 'px'
    },
    getFullTableHeight() {
      let dom = document.getElementById('my-drawer')
      let rect = dom.getBoundingClientRect()
      return rect.height - 150
    }
  },
};
</script>

<style lang="scss">
.dataset-result-table {
  border: 1px solid #e3e4e6;
  border-radius: 12px;
  padding: 20px;
  width: 100%;
  position: absolute;
  .block-page {
    overflow: auto;
  }
  // top: 54%;
  .title-block {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 15px;
    .title {
      color: #4259ce;
      font-size: 14px;
      font-weight: 600;
    }
    .el-icon-close{
      cursor: pointer;
    }
    .el-icon-full-screen {
      cursor: pointer;
      display: inline-block;
      width: 30px;
    }
  }
  ::v-deep {
    .el-pagination__jump{
      display:none;
    }
    .pagination{
      margin-top:15px;
    }
    .el-table {
      thead tr th {
        background-color: #f5f7fa;
      }

      .el-table--group::after{
        border-color: transparent !important;
      }
      &.el-table__cell {
        padding: 6px 0px!important;
      }
      .active-row {
        background: #ecf2fe !important;
      }
      &::before {
        background-color: transparent;
      }
    }
  }
}
</style>
