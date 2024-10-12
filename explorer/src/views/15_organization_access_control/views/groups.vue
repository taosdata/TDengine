<template>
  <div>
    <div class="flexEnd">
      <el-button
        :disabled="!addBtnShow"
        v-permission.noCheck="'group:add'"
        class="medium-btn"
        @click="addGroup"
        plain
        size="small"
        icon="el-icon-plus"
        >{{ $t("accessControl.addNewGroup") }}</el-button
      >
    </div>
    <el-table ref="table" @row-click="resource" class="box-table" size="mini" :data="groupList">
      <el-table-column :label="$t('accessControl.groupName')" min-width="200" prop="group_name">
        <template slot-scope="{ row }">
          <a @click.stop.prevent="manageUser(row)" class="default-link">{{ row.group_name + `(${row.num})` }}</a>
          <el-button class="mini-btn" v-permission="'group:edit'" type="text" @click.stop="edit(row)" icon="el-icon-edit"></el-button>
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('accessControl.userNum')" min-width="200" prop="num">
        <template slot-scope="{ row }">
          <a @click.stop.prevent="manageUser(row)" class="default-link">{{ row.num }}</a>
        </template>
      </el-table-column> -->
      <el-table-column :label="$t('accessControl.resources')" min-width="200" prop="num">
        <template>
          <a @click.prevent class="default-link">{{ $t("accessControl.resources") }}</a>
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('status')" width="200" prop="status">
        <template slot-scope="scope">
          <el-tag size="mini" :type="UserStatusTag[scope.row.status]">{{ scope.row.status }}</el-tag>
        </template>
      </el-table-column> -->
      <el-table-column width="200" :label="$t('createTime')" prop="create_time"></el-table-column>

      <el-table-column fixed="right" :label="$t('operation')" width="100">
        <template slot-scope="scope">
          <el-switch
            v-permission="'group-role:grant'"
            @click.native.stop
            active-color="#4259CE"
            size="mini"
            @change="handleGroupStatus($event, scope.row)"
            :value="scope.row.status != 'DISABLED'"
          >
          </el-switch>
          <el-button
            style="margin-left: 10px"
            v-permission="'group:delete'"
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
    <UpgradeTip v-if="!addBtnShow" :html="$t('limitTip.group', [userGroupNum])" />
    <el-dialog :close-on-click-modal="false" align="center" :title="title" :width="width" :visible.sync="dialog">
      <!-- <AddForm /> -->
      <component :is="comp" :key="dialogKey" @update="update" @close="close" v-bind="dialogParams"></component>
    </el-dialog>
  </div>
</template>

<script>
  import { UserStatusTag } from "@/const";
  import { getGroupList, disableOrganizationGroup, enableOrganizationGroup, deleteOrganizationGroup } from "@/api/gateway/data/dbs";
  import ResourceList from "../components/groupResourceList";
  import AddForm from "../components/addGroupForm";
  import UserList from "../components/groupUserList";
  export default {
    components: {
      AddForm,
      ResourceList,
      UserList,
    },
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        groupList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
        dialog: false,
        dialogType: 0,
        dialogParams: {},
        userList: [],
        currentGroup: {},
        dialogKey: 0,
      };
    },
    computed: {
      title() {
        return {
          0: this.$t("accessControl.addNewGroup"),
          1: this.$t("accessControl.manageGroupUsers").replace("user group", this.currentGroup.group_name),
          2: this.$t("accessControl.resourceBy", [this.$t("userGroup"), `[${this.currentGroup.group_name}]`]),
          3: this.$t("accessControl.editGroup"),
        }[this.dialogType];
      },
      comp() {
        return {
          0: "AddForm",
          1: "UserList",
          2: "ResourceList",
          3: "AddForm",
        }[this.dialogType];
      },
      width() {
        return {
          0: "500px",
          1: "1000px",
          2: "1000px",
          3: "400px",
        }[this.dialogType];
      },
      userGroupNum() {
        return this.$store.state.currentPricePlan.userGroupNum ?? 3;
      },
      addBtnShow() {
        return this.userGroupNum > this.total || this.userGroupNum === -1;
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
      update() {
        this.getData();
      },
      getData() {
        if (this.requestIng) return;
        return getGroupList({
          current_page: this.currentPage,
          page_size: this.pageSize,
        })
          .then(({ content, total }) => {
            this.groupList = content;
            this.total = total;
          })
          .catch(() => {
            this.groupList = [];
            this.total = 0;
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      close() {
        this.dialog = false;
        this.handlePageChange(1);
      },
      handleGroupStatus(status, row) {
        if (this.requestIng) return;
        this.$confirm(this.$t(status ? "enable" : "disable") + ":" + row.group_name, this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            const fn = status ? enableOrganizationGroup : disableOrganizationGroup;
            fn(row.id)
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
      del(row) {
        if (this.requestIng) return;
        this.$confirm(this.$t("del") + ":" + row.group_name, this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            deleteOrganizationGroup(row.id)
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .finally(() => {
                this.requestIng = false;
                if (this.groupList.length == 1 && this.currentPage > 1) {
                  this.currentPage--;
                }
                this.getData();
              });
          })
          .catch(() => {});
      },
      async manageUser(row) {
        this.dialogType = 1;
        this.currentGroup = row;
        this.dialogParams = {
          group_id: row.id,
        };
        this.dialog = true;
      },
      addGroup() {
        this.dialogType = 0;
        this.dialogParams.info = undefined;
        this.dialog = true;
      },
      edit(row) {
        this.dialogType = 3;
        this.dialogParams = {
          info: {
            id: row.id,
            name: row.group_name,
          },
        };
        this.dialog = true;
      },
      resource(data) {
        this.dialogType = 2;
        this.dialogKey++;
        this.currentGroup = data;
        this.dialogParams = {
          group_id: data.id,
          group_name: data.group_name,
          disabled: data.status == "DISABLED",
        };
        this.dialog = true;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
