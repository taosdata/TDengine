<template>
  <div class="">
    <div v-if="level" class="flexEnd">
      <el-button class="medium-btn" @click="dialog = true" v-permission.noCheck="'group-role:grant'" plain size="small" icon="el-icon-plus">{{
        $t("accessControl.addNewMember")
      }}</el-button>
    </div>
    <el-table max-height="500px" class="box-table" v-loading="loading" size="mini" :data="userRoleList">
      <el-table-column v-for="item in nameFieldList" :key="item" :label="$t(item)" min-width="100" :prop="item"></el-table-column>
      <el-table-column :label="$t('email')" min-width="200" prop="email"></el-table-column>
      <el-table-column width="200" :label="$t('accessControl.joinDate')" prop="joinDate"></el-table-column>

      <el-table-column v-if="level" fixed="right" align="right" :label="$t('operation')" width="100">
        <template slot-scope="scope">
          <el-button class="mini-btn" size="mini" @click="del(scope.row)" icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog append-to-body width="800px" center :title="$t('accessControl.addNewMember')" :close-on-click-modal="false" :visible.sync="dialog">
      <UserSelect :selected="userRoleList" @close="dialog = false" @change="change" />
    </el-dialog>
  </div>
</template>

<script>
  // import AddForm from "./memberForm.vue";
  import { UserStatusTag, IsAliyun } from "@/const";
  import { getGroupUserList, disableOrganizationGroupUser, addUserToGroup } from "@/api/gateway/data/dbs";
  import UserSelect from "@/components/UserSelect";
  export default {
    name: "",
    mixins: [],
    components: { UserSelect },
    props: {
      group_id: {
        type: String,
        default: "",
      },
      level: {
        type: Number,
        default: 1,
      },
    },
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        userRoleList: [],
        loading: false,
        dialog: false,
      };
    },
    computed: {
      nameFieldList() {
        let result = ["firstName", "lastName"];
        if (IsAliyun) {
          result = result.reverse();
        }
        return result;
      },
    },
    watch: {
      group_id: {
        handler() {
          this.getUserRoleList();
        },
        immediate: true,
      },
    },
    created() {},
    mounted() {},
    methods: {
      getUserRoleList() {
        if (this.loading) return;
        this.loading = true;
        getGroupUserList(this.group_id)
          .then(data => {
            this.userRoleList = data;
          })
          .finally(() => {
            this.loading = false;
          });
      },
      del(row) {
        this.$confirm(this.$t("del") + ":" + row.email, this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            disableOrganizationGroupUser(this.group_id, row.userId).then(() => {
              this.$message.success(this.$t("delSucc"));
              this.getUserRoleList();
              this.$emit("update");
            });
          })
          .catch(() => {});
      },
      change(add) {
        if (!add.length) return;
        addUserToGroup({ group_id: this.group_id, user_ids: add.map(item => item.userId) }, this.group_id).then(() => {
          this.$message.success(this.$t("addSucc"));
          this.getUserRoleList();
          this.$emit("update");
        });
      },
    },
  };
</script>

<style scoped lang="scss"></style>
