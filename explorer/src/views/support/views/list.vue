<template>
  <div>
    <el-row>
      <el-input style="width: 300px" v-model="filter" size="small" :placeholder="$t('support.searchTip')"></el-input>
      <el-button :disabled="requestIng" plain style="margin-left: 20px" @click="pageChange(1)" size="small">{{ $t("search") }}</el-button>
    </el-row>
    <section class="tableStyle">
      <SupportItem v-for="item in issueList" :key="item.jiraIssueKey" :support="item" @click.native="view(item)"></SupportItem>
    </section>

    <div class="pagination-block">
      <el-pagination
        :current-page="currentPage"
        :hide-on-single-page="true"
        @current-change="pageChange"
        small
        layout="total,prev, pager, next"
        :total="total"
      >
      </el-pagination>
    </div>
  </div>
</template>

<script>
import { parseTime } from "@/utils";
import { mapState } from "vuex";
import SupportItem from "../components/supportItem.vue";
export default {
  components: { SupportItem },
  data() {
    return {
      drawer: false,
      history: [],

      requestIng: false,
      current: {},
    };
  },
  created() {
    this.$store.dispatch("issues/getIssueList");
  },
  computed: {
    ...mapState({
      issueList: state => state.issues.issueList,
      issueTypeObj: state => {
        let obj = {};
        state.issues.issuetype_list.forEach(item => {
          obj[item.value] = item.label;
        });
        return obj;
      },
      currentPage: state => state.issues.currentPage,
      pageSize: state => state.issues.pageSize,
      total: state => state.issues.total,
    }),
    filter: {
      get() {
        return this.$store.state.issues.filter;
      },
      set(val) {
        return this.$store.commit("issues/SET_FILTER", val);
      },
    },
    statusObj() {
      return {
        0: this.$t("support.close"),
        1: this.$t("support.processing"),
        2: this.$t("support.notProcess"),
      };
    },
  },
  methods: {
    view(order) {
      this.$router.push("/support/detail/" + order.id);
    },
    parseTime(date) {
      return parseTime(date, "YYYY-MM-DD kk:mm:ss");
    },
    pageChange(val) {
      this.$store.dispatch("issues/getIssueList", { current_page: val });
    },
  },
};
</script>

<style scoped>
.tableStyle {
  width: 100%;
  margin-top: 15px;
}
.pagination-block {
  margin: 20px 0 10px;
  text-align: center;
}
</style>
