<template>
  <div>
    <div class="flexEnd">
      <el-button
        v-permission.noCheck="'user:invite'"
        class="medium-btn"
        :disabled="!addBtnShow"
        @click="addDialog"
        plain
        size="small"
        icon="el-icon-plus"
        >{{ $t("accessControl.addNewUser") }}</el-button
      >
    </div>
    <el-table style="margin-top: 10px" size="mini" @row-click="rowClick" :data="userList">
      <el-table-column :label="IsAliyun ? $t('lastName') : $t('firstName')" width="200" :prop="IsAliyun ? 'lastName' : 'firstName'"></el-table-column>
      <el-table-column :label="IsAliyun ? $t('firstName') : $t('lastName')" width="200" :prop="IsAliyun ? 'firstName' : 'lastName'"></el-table-column>
      <el-table-column :label="$t('email')" min-width="200" prop="email"></el-table-column>
      <el-table-column :label="$t('accessControl.resources')" min-width="100" prop="num">
        <template>
          <a @click.prevent class="default-link">{{ $t("accessControl.resources") }}</a>
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('status')" width="100" prop="status">
        <template slot-scope="{ row }">
          <el-tag size="mini" :type="UserStatusTag[row.status]">{{ row.status }}</el-tag>
        </template>
      </el-table-column> -->
      <el-table-column width="160" :label="$t('accessControl.joinDate')" prop="joinDate"></el-table-column>
      <!-- <el-table-column :label="$t('role')" width="150" prop="roleList">
        <template slot-scope="{ row }">
          <el-tag size="mini" v-for="item in row.roleList" :key="item.id">{{ item.roleName }}</el-tag>
        </template>
      </el-table-column> -->

      <el-table-column v-if="$hasOrganizationPrivilege(['user:update', 'user:remove'])" fixed="right" :label="$t('operation')" width="120">
        <template slot-scope="scope">
          <span>
            <el-switch
              v-if="scope.row.status != 'INVITED'"
              v-permission="'user:update'"
              @click.native.stop
              active-color="#4259CE"
              :disabled="isOwner(scope.row) || isDisabeld(scope.row)"
              size="mini"
              @change="handleUserStatus($event, scope.row)"
              :value="scope.row.status != 'DISABLED'"
            >
            </el-switch>
            <el-button
              v-else
              :disabled="scope.row.status == 'ACTIVE' || isOwner(scope.row)"
              type="success"
              class="mini-btn"
              @click.stop="resendEamil(scope.row)"
            >
              Invite
            </el-button>
          </span>
          <el-button
            style="margin-left: 10px"
            v-permission="'user:remove'"
            :disabled="isOwner(scope.row)"
            class="mini-btn"
            @click.stop="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
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
    <UpgradeTip v-if="!addBtnShow" :html="$t('limitTip.user', [userNum])" />
    <el-dialog :close-on-click-modal="false" align="center" :width="width" :title="title" :visible.sync="dialog">
      <!-- <AddForm /> -->
      <component :is="comp" :key="dialogKey" @close="close" @update="getData" v-bind="dialogParams"></component>
    </el-dialog>
  </div>
</template>

<script>
  import { resendEamil } from "@/api/user";
  import { getOrganizationUser } from "@/api/organization";
  import AddForm from "../components/memberForm";
  import userRoleList from "../components/userRoleList";
  import UserResources from "../components/userResourceList";
  import { UserStatusTag, IsAliyun } from "@/const";
  import { disableOrganizationUser, enableOrganizationUser, deleteOrganizationUser } from "@/api/user";
  export default {
    components: {
      AddForm,
      userRoleList,
      UserResources,
    },
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        IsAliyun,
        userList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
        dialog: false,
        dialogType: 1,
        dialogParams: {},
        currentUser: {},
        dialogKey: 0,
      };
    },
    computed: {
      title() {
        return {
          0: this.$t("accessControl.addNewUser"),
          1: this.$t("accessControl.userRole"),
          2: this.$t("accessControl.resourceBy", [
            this.$t("user"),
            `[${this.$t("usernameTep", [this.currentUser.firstName, this.currentUser.lastName])}]`,
          ]),
        }[this.dialogType];
      },
      comp() {
        return {
          0: "AddForm",
          1: "userRoleList",
          2: "UserResources",
        }[this.dialogType];
      },
      user() {
        return this.$store.getters.userInfo;
      },
      width() {
        return {
          0: "600px",
          1: "1000px",
          2: "1000px",
        }[this.dialogType];
      },
      userNum() {
        return this.$store.state.currentPricePlan.userNum ?? 5;
      },
      addBtnShow() {
        return this.userNum > this.total || this.userNum === -1;
      },
    },
    created() {
      this.getData();
    },
    methods: {
      handlePageChange(val) {
        this.currentPage = val;
        this.getData();
      },
      getData() {
        if (this.requestIng) return;
        getOrganizationUser({
          current_page: this.currentPage,
          page_size: this.pageSize,
        })
          .then(({ content, total }) => {
            this.userList = content;
            this.total = total;
          })
          .catch(() => {
            this.userList = [];
            this.total = 0;
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      rowClick(row) {
        this.currentUser = row;
        this.dialogParams = {
          id: row.userId,
          email: row.email,
          disabled: row.status == "DISABLED",
        };
        this.dialogKey++;
        this.dialogType = 2;
        this.dialog = true;
      },
      resendEamil(data) {
        if (this.requestIng) return;
        this.requestIng = true;
        resendEamil(data.userId)
          .then(() => {
            this.$message.success(this.$t("sendSucc"));
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      addDialog() {
        this.dialogParams = {};
        this.dialog = true;
        this.dialogType = 0;
      },

      handleUserStatus(status, row) {
        if (this.requestIng) return;
        this.$confirm(this.$t(status ? "enable" : "disable") + ":" + row.email, this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            const fn = status ? enableOrganizationUser : disableOrganizationUser;
            fn(row.id, row.userId)
              .then(() => {
                this.$message.success(this.$t("operateSucc"));
              })
              .finally(() => {
                this.requestIng = false;

                this.getData();
              });
          })
          .catch(() => {});
      },
      isOwner(row) {
        return this.user.email === row.email;
      },
      isDisabeld(row) {
        return row.status == "INVITED" || row.status == "DELETED";
      },
      del(row) {
        if (this.requestIng) return;
        this.$confirm(this.$t("del") + ":" + row.email, this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            deleteOrganizationUser(row.id, row.userId)
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .finally(() => {
                this.requestIng = false;
                if (this.userList.length == 1 && this.currentPage > 1) {
                  this.currentPage--;
                }
                this.getData();
              });
          })
          .catch(() => {});
      },
      close() {
        this.handlePageChange(1);
        this.dialog = false;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
