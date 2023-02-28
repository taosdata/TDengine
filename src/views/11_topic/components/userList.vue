<template>
  <div>
    <div class="flexEnd">
      <el-button class="medium-btn" @click="addDialog" plain size="small" icon="el-icon-plus">{{ addBtnTitle }}</el-button>
    </div>
    <el-table style="margin-top: 10px" size="mini" :data="userList">
      <template v-if="type == 'user'">
        <el-table-column :label="$t('firstName')" min-width="100" prop="firstName"></el-table-column>
        <el-table-column :label="$t('lastName')" min-width="100" prop="lastName"></el-table-column>
        <el-table-column :label="$t('email')" min-width="200" prop="email"></el-table-column>
      </template>
      <template v-else>
        <el-table-column :label="$t('accessControl.groupName')" min-width="200" prop="groupName"></el-table-column>
      </template>
      <el-table-column :label="$t('role')" width="140" prop="num">
        <template slot-scope="{ row }">
          <el-tag size="mini" type="primary">{{ row.roleName }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="$t('expiration')" min-width="160" prop="expiration"> </el-table-column>
      <el-table-column min-width="160" :label="$t('grantTime')" prop="grantTime"></el-table-column>
      <!-- <el-table-column :label="$t('role')" width="150" prop="roleList">
        <template slot-scope="{ row }">
          <el-tag size="mini" v-for="item in row.roleList" :key="item.id">{{ item.roleName }}</el-tag>
        </template>
      </el-table-column> -->

      <el-table-column fixed="right" :label="$t('operation')" width="100">
        <template slot-scope="scope">
          <el-switch
            @click.native.stop
            active-color="#4259CE"
            :disabled="isDisabeld(scope.row)"
            size="mini"
            @change="handleUserStatus($event, scope.row)"
            :value="scope.row.status != 'DISABLED'"
          >
          </el-switch>
          <el-button
            style="margin-left: 10px"
            v-permission="'user:remove'"
            :disabled="isDisabeld(scope.row)"
            size="mini"
            @click.stop="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <!-- <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination> -->
    <el-dialog :close-on-click-modal="false" align="center" :width="width" :title="title" :visible.sync="dialog">
      <!-- <AddForm /> -->
      <component :is="comp" :type="type" @close="close" v-bind="dialogParams"></component>
    </el-dialog>
  </div>
</template>

<script>
  import {
    getTopicUsers,
    getTopicGroups,
    deleteTopicUser,
    deleteTopicGroup,
    disableTopicGroup,
    disableTopicUser,
    enableTopicGroup,
    enableTopicUser,
  } from "@/api/topic";
  import AddForm from "./addTopicUser.vue";
  import { UserStatusTag } from "@/const";
  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
      topicId: {
        type: String,
        default: "",
      },
    },
    components: {
      AddForm,
    },
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        userList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
        dialog: false,
        dialogType: 1,
        dialogParams: {},
        currentUser: {},
      };
    },
    computed: {
      title() {
        return {
          0: {
            user: this.$t("accessControl.addNewUsers"),
            group: this.$t("accessControl.addNewGroups"),
          }[this.type],
          1: this.$t("accessControl.resourceBy")
            .replace("{type}", this.$t("user"))
            .replace("{username}", `[${this.currentUser.firstName} ${this.currentUser.lastName}]`),
        }[this.dialogType];
      },
      comp() {
        return {
          0: "AddForm",
        }[this.dialogType];
      },
      addBtnTitle() {
        return {
          user: this.$t("accessControl.addNewUsers"),
          group: this.$t("accessControl.addNewGroups"),
        }[this.type];
      },
      user() {
        return this.$store.getters.userInfo;
      },
      width() {
        return {
          0: "380px",
          1: "1000px",
          2: "1000px",
        }[this.dialogType];
      },
      dataFn() {
        return {
          user: getTopicUsers,
          group: getTopicGroups,
        }[this.type];
      },
      disable() {
        return {
          user: disableTopicUser,
          group: disableTopicGroup,
        }[this.type];
      },
      enable() {
        return {
          user: enableTopicUser,
          group: enableTopicGroup,
        }[this.type];
      },
      deleteFn() {
        return {
          user: deleteTopicUser,
          group: deleteTopicGroup,
        }[this.type];
      },
      id() {
        return this.type == "user" ? "userRoleId" : "groupRoleId";
      },
    },
    watch: {
      topicId: {
        handler() {
          this.getData();
        },
        immediate: true,
      },
    },
    created() {},
    methods: {
      handlePageChange(val) {
        this.currentPage = val;
        this.getData();
      },
      getData() {
        if (this.requestIng || !this.topicId) return;
        this.dataFn(this.topicId)
          .then(data => {
            this.userList = data;
          })
          .catch(() => {
            this.userList = [];
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
        };
        this.dialogType = 2;
        this.dialog = true;
      },

      addDialog() {
        this.dialogParams = {
          topicId: this.topicId,
          filterList: this.type == "user" ? this.userList : this.userList.map(item => ({ id: item.groupId })),
        };
        this.dialog = true;
        this.dialogType = 0;
      },

      handleUserStatus(status, row) {
        if (this.requestIng) return;
        this.$confirm(this.getTipName(row, status ? "enable" : "disable"), this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            const fn = status ? this.enable : this.disable;
            fn(row[this.id])
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
        if (this.type != "user") return false;
        return row.userId == this.user.id;
      },
      getTipName(data, type) {
        let tip = this.$t("accessControl.change" + this.type + "RoleTip");
        tip = tip.replace(`{roleName}`, data.roleName?.toLowerCase());
        tip = tip.replace(`{type}`, type);
        if (this.type == "user") {
          if (this.email) {
            tip = tip.replace(`{email}`, this.email);
          } else {
            tip = tip.replace(`{email}`, data.firstName + " " + data.lastName);
          }
        } else {
          tip = tip.replace(`{groupName}`, this.group_name || data.groupName);
        }
        return tip || "the " + data.roleName + " role";
      },
      del(row) {
        if (this.requestIng) return;
        this.$confirm(this.getTipName(row, "delete"), this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;

            this.deleteFn(row[this.id])
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
