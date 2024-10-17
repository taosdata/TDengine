<template>
  <div class="">
    <section class="flexBetween">
      <section class="flexStart">
        <div class="detail-display">
          <p class="title">{{ $t("accessControl.currentOrganization") }}:</p>
          <p class="value">{{ $root.currentInfo.org.orgName }}</p>
        </div>
        <div v-if="level != 'organization'" class="detail-display">
          <p class="title">{{ $t("accessControl.currentInstance") }}:</p>
          <p class="value">{{ $root.currentInfo.ins.alias }}</p>
        </div>
      </section>
      <el-button
        v-permission:[permissionArg].noCheck="'user-role:grant'"
        :disabled="disabled"
        @click="addResource"
        class="medium-btn"
        icon="el-icon-plus"
        plain
        >{{ $t("accessControl.addAccess") }}</el-button
      >
    </section>
    <el-table max-height="500px" style="margin-top: 10px" size="mini" :data="dataList">
      <el-table-column
        v-if="level === 'organization'"
        show-overflow-tooltip
        :label="$t('accessControl.instance')"
        min-width="100"
        prop="instanceName"
      >
        <template slot-scope="{ row }">
          {{ aliasMap[row.instanceId] }}
        </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip label="DB" min-width="100" prop="databaseName"></el-table-column>
      <el-table-column show-overflow-tooltip width="150" :label="$t('accessControl.role')" prop="roleName">
        <template slot-scope="{ row }">
          <el-tag size="mini">{{ row.roleName }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip min-width="100" :label="$t('accessControl.group')" prop="groupName"></el-table-column>
      <el-table-column :label="$t('accessControl.startDate')" width="170" prop="startTime"> </el-table-column>
      <el-table-column :label="$t('expiration')" width="170" prop="expiration"> </el-table-column>
      <!-- <el-table-column :label="$t('status')" width="100" prop="status">
        <template slot-scope="{ row }">
          <el-tag size="mini" :type="UserStatusTag[row.status]">{{ row.status }}</el-tag>
        </template>
      </el-table-column> -->
      <el-table-column fixed="right" :label="$t('operation')" width="100" prop="sql">
        <template slot-scope="scope">
          <el-switch
            v-permission:[permissionArg]="'user-role:grant'"
            @click.native.stop
            :title="!!scope.row.groupId ? $t('accessControl.groupRoleDisabledReasonTip') : ''"
            active-color="#4259CE"
            :disabled="!!scope.row.groupId || disabled || scope.row.roleId == '1'"
            size="mini"
            @change="statusChange($event, scope.row)"
            :value="scope.row.status != 'DISABLED'"
          >
          </el-switch>
          <el-button
            :disabled="!!scope.row.groupId || disabled || scope.row.roleId == '1'"
            :title="!!scope.row.groupId ? $t('accessControl.groupRoleDisabledReasonTip') : ''"
            v-permission:[permissionArg]="'user-role:delete'"
            style="margin-left: 10px"
            class="mini-btn"
            size="mini"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog :close-on-click-modal="false" append-to-body width="400px" :visible.sync="dialog" :title="$t('accessControl.addAccess')">
      <ResourceRole refs="resourceRole" type="residue" :level="level" :params="params" :key="id" ref="resourceRole" v-model="newResource" />
      <span slot="footer" class="dialog-footer">
        <el-button size="mini" @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button v-permission:[permissionArg]="'user-role:grant'" size="mini" type="primary" @click="submit">{{ $t("confirm") }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
  import ResourceRole from "@/components/ResourceRole.vue";
  import userAndGroupRole from "./userAndGroupRole";
  import { getOrganizationResource, apendInstanceResource, getInstanceResource, appendOrganizationResource } from "@/api/user";
  import { UserStatusTag } from "@/const";
  import { sortResource } from "@/utils";
  export default {
    components: { ResourceRole },
    mixins: [userAndGroupRole],
    props: {
      id: {
        type: String,
        default: "",
      },
      level: {
        type: String,
        default: "organization",
      },
      email: {
        type: String,
        default: "",
      },
      disabled: {
        type: Boolean,
        default: false,
      },
    },
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        currentPage: 1,
        pageSize: 10,
        total: 0,
        dataList: [],
        isUser: true,
        type: "user",
        newResource: [],
        dialog: false,
        info: {
          group_list: [],
          roles: [],
        },
      };
    },
    computed: {
      params() {
        const params = {
          type: "1",
          id: this.id,
        };
        if (this.level == "instance") {
          params.app_id = this.$store.getters.appId;
        }
        return params;
      },
      permissionArg() {
        return this.level === "organization" ? "" : "ins";
      },
      aliasMap() {
        return this.$store.getters.instanceAliasMap;
      },
      resourceFn() {
        return this.level === "organization" ? getOrganizationResource : getInstanceResource;
      },
      appendResourceFn() {
        return this.level === "organization" ? appendOrganizationResource : apendInstanceResource;
      },
    },
    watch: {
      id: {
        handler() {
          this.getData();
        },
        immediate: true,
      },
    },
    created() {},
    mounted() {},
    methods: {
      handlePageChange(val) {
        this.currentPage = val;
        this.getData();
      },
      getData() {
        this.resourceFn(this.id).then(data => {
          this.dataList = sortResource(data);
        });
      },
      addResource() {
        this.dialog = true;
        this.$nextTick(() => {
          this.$refs.resourceRole.reset();
        });
      },
      submit() {
        if (!this.newResource.length) return (this.dialog = false);
        this.appendResourceFn({
          roles: this.newResource,
          user_id: this.id,
        }).then(() => {
          this.$message.success(this.$t("addSucc"));
          this.dialog = false;
          this.getData();
          this.$refs.resourceRole.reset();
        });
      },
    },
  };
</script>

<style scoped lang="scss"></style>
