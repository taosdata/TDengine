<template>
  <div>
    <el-tabs v-model="activeTab" tab-position="left">
      <el-tab-pane v-if="validateFn(['user-role:grant', 'user-role:delete'], false)" name="user" :label="$t('accessControl.users')">
        <RoleList @update="getData" :level="type" :list="userGrants" />
      </el-tab-pane>
      <el-tab-pane v-if="validateFn(['group-role:grant', 'group-role:delete'], false)" name="userGroup" :label="$t('accessControl.userGroups')">
        <RoleList @update="getData" :levle="type" type="group" :list="groupGrants" />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
  import { getOrganizationUserAndGroupRoleList } from "@/api/organization";
  import { getAppUserGroupRole } from "@/api/app";
  import RoleList from "../components/grantRoleList";
  //   import AddForm from "./components/addForm";
  export default {
    props: {
      type: {
        type: String,
        default: "organization",
      },
    },
    components: {
      //   AddForm,
      RoleList,
    },
    data() {
      return {
        organizationList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
        dialog: false,
        groupGrants: [],
        userGrants: [],
        activeTab: "user",
      };
    },
    computed: {
      dataFn() {
        return this.type === "organization" ? getOrganizationUserAndGroupRoleList : getAppUserGroupRole;
      },
      validateFn() {
        return this.type === "organization" ? this.$hasOrganizationPrivilege : this.$hasInstancePrivilege;
      },
    },
    created() {
      this.getData();
    },
    methods: {
      handlePageChange(val = 1) {
        this.currentPage = val;
        this.getData();
      },
      getData() {
        if (this.requestIng) return;
        this.dataFn()
          .then(({ groupGrants, userGrants }) => {
            this.groupGrants = groupGrants || [];
            this.userGrants = userGrants || [];
          })
          .catch(() => {
            this.groupGrants = [];
            this.userGrants = [];
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
    },
  };
</script>

<style lang="scss" scoped></style>
