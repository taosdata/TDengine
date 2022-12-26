<template>
  <div class="list">
    <section class="flexEnd">
      <el-button v-if="clusterList.length > 1" size="mini" @click="add" plain icon="el-icon-plus" class="big-button">{{
        $t("users.createNewUser")
      }}</el-button>
    </section>

    <el-table size="mini" style="margin-top: 20px" stripe tooltip-effect="dark" @row-click="edit" :data="userList">
      <el-table-column :show-overflow-tooltip="true" :label="$t('firstName')" width="150" prop="firstname"></el-table-column>
      <el-table-column :show-overflow-tooltip="true" :label="$t('lastName')" width="150" prop="lastname"></el-table-column>

      <el-table-column :show-overflow-tooltip="true" :label="$t('email')" min-width="230" prop="email"></el-table-column>

      <el-table-column :show-overflow-tooltip="true" :label="$t('role')" min-width="230" prop="role"></el-table-column>

      <el-table-column :label="$t('status')" width="100" prop="activated">
        <template slot-scope="scope">
          <p v-if="statusObj[scope.row.status]" effect="dark" size="small" :type="statusObj[scope.row.status].type">
            {{ statusObj[scope.row.status].text }}
          </p>
        </template>
      </el-table-column>

      <el-table-column :show-overflow-tooltip="true" :label="$t('createTime')" width="230" prop="create_time"></el-table-column>

      <el-table-column fixed="right" align="right" width="90">
        <template slot-scope="scope">
          <el-button
            size="mini"
            :disabled="requestIng"
            v-if="scope.row.status != 1"
            :title="$t('users.resendTip')"
            @click.stop="resend(scope.row)"
            plain
            icon="el-icon-message"
          ></el-button>
          <el-button size="mini" :disabled="requestIng" :title="$t('del')" @click.stop="del(scope.row)" plain icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>

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

    <el-dialog :title="$t('users.createNewUser')" :visible.sync="dialog" width="700px">
      <AddUser @close="dialog = false" />
    </el-dialog>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  import AddUser from "../components/addNewUser.vue";
  import { resendEamil } from "@/api/auth";
  import { delUser } from "@/api/user";
  export default {
    components: { AddUser },
    data() {
      this.statusObj = {
        0: {
          type: "danger",
          text: this.$t("inactivated"),
        },
        1: {
          type: "success",
          text: this.$t("activated"),
        },
        2: {
          type: "danger",
          text: this.$t("incomplete"),
        },
        3: {
          type: "info",
          text: this.$t("disabled"),
        },
      };
      return {
        cluster: "All",
        dialog: false,
        requestIng: false,
      };
    },
    computed: {
      ...mapState({
        userList: state => state.user.userList,
        clusterList(state) {
          return [{ alias: this.$t("users.all"), id: "All" }, ...state.app.clusters];
        },
        currentPage: state => state.user.currentPage,
        pageSize: state => state.user.pageSize,
        total: state => state.user.total,
      }),
    },
    created() {
      this.$store.dispatch("user/getUserList");
    },
    methods: {
      rowClick(row) {
        console.log(row);
      },
      add() {
        this.dialog = true;
      },
      handlePageChange(val) {
        this.$store.dispatch("user/getUserList", val);
      },
      resend(user) {
        if (this.requestIng) return;
        this.requestIng = true;
        resendEamil(user.email)
          .then(() => {
            this.$message({
              message: this.$t("sendSucc"),
              type: "success",
            });
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      edit(row) {
        this.$router.push("/user/detail/" + row.id);
      },
      del(data) {
        if (this.requestIng) return;
        this.$confirm(this.$t("users.delUser") + ":" + data.email + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requestIng = true;
          await delUser(data.id)
            .then(() => {
              this.$message.success(this.$t("delSucc"));
            })
            .finally(() => {
              this.$store.dispatch("user/getUserList");
              this.requestIng = false;
            });
        });
      },
    },
  };
</script>

<style lang="scss" scoped></style>
