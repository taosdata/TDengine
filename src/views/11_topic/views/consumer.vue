<template>
  <div>
    <div class="flexEnd">
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh" :disabled="requestIng" style="font-size:14px;">{{
        $t("refresh")
      }}</el-button>
    </div>
    <el-table size="mini" :data="consumerList">
      <el-table-column :label="$t('topic.consumerID')" prop="consumer_id"></el-table-column>
      <el-table-column :label="$t('topic.consumerGroup')" prop="consumer_group"></el-table-column>
      <el-table-column :label="$t('topic.clientID')" prop="client_id"></el-table-column>
      <el-table-column :label="$t('status')" prop="status"></el-table-column>
      <el-table-column :label="$t('route.topic')" prop="topics"></el-table-column>
      <el-table-column :label="$t('topic.upTime')" prop="up_time">
        <span slot-scope="scope">{{ parsinginZone(scope.row.up_time) }}</span>
      </el-table-column>
      <el-table-column :label="$t('topic.subscribeTime')" prop="subscribe_time">
        <span slot-scope="scope">{{ parsinginZone(scope.row.subscribe_time) }}</span>
      </el-table-column>
      <el-table-column :label="$t('topic.rebalanceTime')" prop="rebalance_time">
        <span slot-scope="scope">{{ parsinginZone(scope.row.rebalance_time) }}</span>
      </el-table-column>
      <!-- <el-table-column label="Pid" prop="pid"></el-table-column> -->
      <!-- <el-table-column :label="$t('topic.endPoint')" prop="end_point"></el-table-column> -->
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
    <el-dialog align="center" :title="$t('topic.createTopic')" width="500px" :visible.sync="dialog">
      <el-input size="small" @input="errorText = ''" :placeholder="sqlTip" v-model="sql">
        <template slot="prepend">{{ sqlPrefix }}</template>
        <template slot="append">
          <el-tooltip class="item" effect="light">
            <div v-html="$t('topic.topicTip')" slot="content"></div>
            <i class="el-icon-info"></i>
          </el-tooltip>
        </template>
      </el-input>
      <p class="errorText">{{ errorText }}</p>
      <el-row style="margin-top: 20px">
        <el-col :span="11">
          <el-button size="small" @click="dialog = false" class="w100">{{ $t("cancel") }}</el-button>
        </el-col>
        <el-col :span="11" :offset="1">
          <el-button size="small" :disabled="requestIng || !sql" @click="createSubscribe" class="w100" type="primary">{{ $t("confirm") }}</el-button>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>

<script>
  import { getConsumers } from "@/api/topic";
  import { parsinginZone } from '@/utils'
  export default {
    data() {
      return {
        consumerList: [],
        requestIng: false,
        currentPage: 1,
        pageSize: 10,
        total: 0,
        dialog: false,
        sql: "",
        sqlPrefix: "",
        errorText: "",
        sqlTip: "",
        parsinginZone
      };
    },
    computed: {},
    created() {
      this.getConsumers();
    },
    methods: {
      refresh(){
        this.getConsumers();
      },
      async getConsumers() {
        if (this.requestIng) return;
        this.requestIng = true;
        [this.consumerList, this.total] = await getConsumers({ currentPage: this.currentPage, pageSize: this.pageSize });
        this.requestIng = false;
      },
      createSubscribe() {
        if (this.requestIng) return;
        this.requestIng = true;
      },
      handlePageChange() {
        this.getConsumers();
      },
    },
  };
</script>

<style></style>
