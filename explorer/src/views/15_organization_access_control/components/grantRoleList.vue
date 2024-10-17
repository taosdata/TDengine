<template>
  <div class="">
    <!-- <section v-if="isGrant" class="flexEnd">
      <el-button class="medium-btn" icon="el-icon-plus" plain>{{ btnTitle }}</el-button>
    </section> -->
    <el-table size="mini" :data="list" class="w100">
      <el-table-column v-for="item in columnConfig" v-bind="item" :key="item.prop" :label="$t(item.name)"></el-table-column>
      <el-table-column :label="$t('role')" width="150" prop="roleList">
        <template slot-scope="{ row }">
          <el-tag size="mini">{{ row.roleName }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="$t('status')" width="100" prop="status">
        <template slot-scope="{ row }">
          <el-tag size="mini" :type="UserStatusTag[row.status]">{{ row.status }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column v-if="isGrant || isDelete" fixed="right" :label="$t('operation')" width="180">
        <template slot-scope="{ row }">
          <el-switch v-if="isGrant" size="mini" :value="row.status != 'DISABLED'" @change="val => statusChange(val, row)" />
          <el-button v-if="isDelete" class="mini-btn" style="margin-left: 10px" @click="del(row)" icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
  import userAndGroupRole from "./userAndGroupRole";
  //   import AddForm from "./add";
  import cloumnConfig from "./cloumnConfig";
  import { UserStatusTag } from "@/const";

  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
      list: {
        type: Array,
        default: () => [],
      },
      level: {
        type: String,
        default: "organization",
      },
    },
    mixins: [userAndGroupRole],
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        dialog: false,
      };
    },
    computed: {
      isUser() {
        return this.type === "user";
      },
      btnTitle() {
        return this.$t(this.isUser ? "accessControl.addNewUser" : "accessControl.addNewGroup");
      },
      columnConfig() {
        return cloumnConfig[this.type];
      },
      validateFn() {
        return this.level === "organization" ? this.$hasOrganizationPrivilege : this.$hasInstancePrivilege;
      },
      isGrant() {
        return this.validateFn(this.type + "-role:grant");
      },
      isDelete() {
        return this.validateFn(this.type + "-role:delete");
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      add() {},
      close() {
        this.$store.dispatch("dbs/getDBUserList");
        this.dialog = false;
      },
      getData() {
        this.$emit("update");
      },
    },
  };
</script>

<style scoped lang="scss"></style>
