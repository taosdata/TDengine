<template>
  <div class="alerts_table">
    <el-empty v-if="!alertList.length" :image-size="200"></el-empty>
    <AlertItem v-for="item in alertList" :key="item.id" @click.native="view(item)" :alert="item" />
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <el-drawer size="800px" :title="$t('alert.alertDetail')" :visible.sync="drawer" direction="rtl">
      <Detail :info="detailInfo" />
    </el-drawer>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  import Detail from "../../../detail/index.vue";
  import { getAlertDetail } from "@/api/gateway/alert";
  import AlertItem from "./alertItem.vue";
  export default {
    components: { Detail, AlertItem },
    computed: {
      ...mapState({
        alertList: state => state.alert.alertList,
        pageSize: state => state.alert.pageSize,
        currentPage: state => state.alert.currentPage,
        total: state => state.alert.total,
      }),
      statusObj() {
        return {
          0: this.$t("alert.unread"),
          1: this.$t("alert.readed"),
        };
      },
      messageTypeObj() {
        return {
          1: this.$t("alert.cpuAlert"),
          2: this.$t("alert.memAlert"),
          3: this.$t("alert.diskAlert"),
        };
      },
    },
    data() {
      return {
        drawer: false,
        detailHistory: {},
        detailInfo: {},
      };
    },
    mounted() {
      if (this.$store.state.alert.alertId) {
        this.view({ id: this.$store.state.alert.alertId });
        this.$store.state.alert.alertId = "";
      }
    },
    methods: {
      handlePageChange(val) {
        this.$store.dispatch("alert/getAlertList", { current_page: val });
      },
      async view(row) {
        if (this.detailHistory[row.id]) {
          this.detailInfo = this.detailHistory[row.id];
        } else {
          let data = await getAlertDetail({ id: row.id }).catch(() => false);
          row.status = 1;
          this.detailInfo = data || {};
          this.detailHistory[row.id] = data;
          this.$store.dispatch("app/getNewAlert");
        }
        this.detailHistory[row.id] && (this.drawer = true);
      },
    },
  };
</script>

<style scoped>
  .alerts_table {
    margin-top: 14px;
  }

  .pagination {
    margin-top: 25px;
    text-align: center;
  }
  .unread {
    font-weight: bold;
  }
</style>
