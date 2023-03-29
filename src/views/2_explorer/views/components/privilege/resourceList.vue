<template>
  <div class="">
    <section class="flexBetween">
      <section class="flexStart">
        <div class="detail-display">
          <p class="title">{{ $t("accessControl.currentOrganization") }}:</p>
          <p class="value">{{ $root.currentInfo.org.orgName }}</p>
        </div>
        <div class="detail-display">
          <p class="title">{{ $t("accessControl.currentInstance") }}:</p>
          <p class="value">{{ $root.currentInfo.ins.alias }}</p>
        </div>
        <div class="detail-display">
          <p class="title">{{ $t("accessControl.currentDB") }}:</p>
          <p class="value">{{ $root.currentInfo.db.databaseName }}</p>
        </div>
      </section>
      <el-button v-permission @click="addResource" :disabled="disabled" class="medium-btn" icon="el-icon-plus" plain>{{
        $t("accessControl.addAccess")
      }}</el-button>
    </section>
    <el-table max-height="500px" class="box-table" size="mini" :data="dataList">
      <el-table-column show-overflow-tooltip width="150" :label="$t('accessControl.role')" prop="roleName">
        <template slot-scope="{ row }">
          <el-tag size="mini">{{ row.roleName }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip width="100" :label="$t('accessControl.group')" prop="groupName"></el-table-column>
      <el-table-column :label="$t('accessControl.startDate')" width="170" prop="startTime"> </el-table-column>
      <el-table-column :label="$t('expiration')" width="170" prop="expiration"> </el-table-column>
      <!-- <el-table-column :label="$t('status')" width="100" prop="status">
          <template slot-scope="{ row }">
            <el-tag size="mini" :type="UserStatusTag[row.status]">{{ row.status }}</el-tag>
          </template>
        </el-table-column> -->
      <el-table-column fixed="right" :label="$t('operation')" width="100" prop="sql">
        <template slot-scope="scope" v-if="(isUser && currentUser.id != scope.row.id) || !isUser">
          <el-switch
            @click.native.stop
            active-color="#4259CE"
            :disabled="disabled"
            v-permission
            size="mini"
            @change="statusChange($event, scope.row)"
            :value="scope.row.status != 'DISABLED'"
          >
          </el-switch>
          <el-button
            style="margin-left: 10px"
            v-permission
            :disabled="disabled"
            size="mini"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog :close-on-click-modal="false" append-to-body width="400px" :visible.sync="dialog" :title="$t('accessControl.addAccess')">
      <ResourceRole refs="resourceRole" type="residue" level="db" :params="params" :key="id" ref="resourceRole" v-model="newResource" />
      <span slot="footer" class="dialog-footer">
        <el-button size="mini" @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button size="mini" type="primary" @click="submit">{{ $t("confirm") }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
  import ResourceRole from "@/components/ResourceRole.vue";
  import { getDatabaseGroupResource } from "@/api/gateway/data/dbs";
  import userAndGroupRole from "@/views/15_organization_access_control/components/userAndGroupRole";
  import { getDBResource, apendInstanceResource } from "@/api/user";
  import { UserStatusTag } from "@/const";
  export default {
    components: { ResourceRole },
    mixins: [userAndGroupRole],
    props: {
      id: {
        type: String,
        default: "",
      },
      type: {
        type: String,
        default: "user",
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
        newResource: [],
        dialog: false,
        info: {
          group_list: [],
          roles: [],
        },
        level: "instance",
      };
    },
    computed: {
      databaseId() {
        return this.$store.state.console.currentInfoData.databaseId;
      },
      params() {
        const params = {
          type: "1",
          id: this.id,
          databaseId: this.databaseId,
        };
        if (this.level == "instance") {
          params.app_id = this.$store.getters.appId;
        }
        return params;
      },
      isUser() {
        return this.type == "user";
      },
      aliasMap() {
        return this.$store.getters.instanceAliasMap;
      },
      resourceParams() {
        return [this.id, this.databaseId];
      },
      resourceFn() {
        return this.isUser ? getDBResource : getDatabaseGroupResource;
      },
      currentUser() {
        return this.$store.getters.userInfo;
      },
    },
    watch: {
      resourceParams: {
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
        this.resourceFn(...this.resourceParams).then(data => {
          this.dataList = data;
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
        apendInstanceResource({
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
