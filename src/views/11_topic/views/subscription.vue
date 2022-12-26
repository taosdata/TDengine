<template>
  <div>
    <div class="flexEnd">
      <el-button class="big-button" plain @click="dialog = true" size="small" icon="el-icon-plus">{{ $t("topic.addSubscription") }}</el-button>
    </div>
    <el-table style="margin-top: 20px" size="mini" :data="subscriptionList">
      <el-table-column :label="$t('topic.sourceUrl')" prop="id"></el-table-column>
      <el-table-column label="Token" prop="id"></el-table-column>
      <el-table-column :label="$t('topic.topic')" prop="id"></el-table-column>
      <el-table-column :label="$t('createTime')" prop="id"></el-table-column>
      <el-table-column :label="$t('topic.database')" prop="id"></el-table-column>
      <el-table-column fixed="right" width="50">
        <template slot-scope="{ row }">
          <el-button size="mini" @click="del(row)" plain icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
  </div>
</template>

<script>
  import { getSubscriptions } from "@/api/topic";
  export default {
    data() {
      return {
        subscriptionList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
      };
    },
    created() {
      this.getSubscriptions();
    },
    methods: {
      async getSubscriptions() {
        if (this.requestIng) return;
        this.requestIng = true;
        [this.subscriptionList, this.total] = await getSubscriptions({ currentPage: this.currentPage, pageSize: this.pageSize });
        this.requestIng = false;
      },
      del() {},
      handlePageChange() {
        this.getSubscriptions();
      },
    },
  };
</script>

<style></style>
