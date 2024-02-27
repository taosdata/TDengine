<template>
  <div>
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
        v-permission:[permissionArg].noCheck="'group-role:grant'"
        :disabled="disabled"
        @click="addResource"
        class="medium-btn"
        icon="el-icon-plus"
        plain
        >{{ $t("accessControl.addAccess") }}</el-button
      >
    </section>
    <el-table max-height="500px" style="margin-top: 10px" size="mini" :data="resource">
      <el-table-column v-if="level == 'organization'" show-overflow-tooltip :label="$t('accessControl.instance')" width="200" prop="instanceName">
        <template slot-scope="{ row }">
          {{ aliasMap[row.instanceId] }}
        </template>
      </el-table-column>
      <el-table-column label="DB" show-overflow-tooltip width="100" prop="databaseName"></el-table-column>
      <el-table-column min-width="150" show-overflow-tooltip :label="$t('accessControl.role')" prop="">
        <template slot-scope="{ row }">
          <el-tag size="mini">{{ row.roleName }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="$t('accessControl.startDate')" width="200" prop="startDate"> </el-table-column>
      <el-table-column :label="$t('expiration')" width="200" prop="expiration"> </el-table-column>
      <el-table-column fixed="right" :label="$t('operation')" width="100" prop="sql">
        <template slot-scope="scope">
          <el-switch
            v-permission:[permissionArg]="'group-role:grant'"
            @click.native.stop
            :disabled="disabled"
            active-color="#4259CE"
            size="mini"
            @change="statusChange($event, scope.row)"
            :value="scope.row.status != 'DISABLED'"
          >
          </el-switch>
          <el-button
            v-permission:[permissionArg]="'group-role:delete'"
            style="margin-left: 10px"
            :disabled="disabled"
            class="mini-btn"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog append-to-body width="300px" center :visible.sync="dialog" :title="$t('accessControl.addAccess')" :close-on-click-modal="false">
      <ResourceRole ref="resourceRole" type="residue" :params="params" v-model="newResource" />
      <span slot="footer" class="dialog-footer">
        <el-button size="mini" @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button v-permission:[permissionArg]="'group-role:grant'" size="mini" type="primary" @click="submit">{{ $t("confirm") }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
  import userAndGroupRole from "./userAndGroupRole";
  import { UserStatusTag } from "@/const";
  import { sortResource } from "@/utils";
  import { apendInstanceGroupResource, apendOrganizationGroupResource, getOrganizationGroupResource, getInstanceGroupResource } from "@/api/gateway/data/dbs";
  import ResourceRole from "@/components/ResourceRole.vue";
  export default {
    name: "",
    mixins: [userAndGroupRole],
    components: { ResourceRole },
    props: {
      group_id: {
        type: String,
        default: "",
      },
      level: {
        type: String,
        default: "organization",
      },
      group_name: {
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
        dialog: false,
        isUser: false,
        type: "group",
        newResource: [],
        resource: [],
      };
    },
    computed: {
      params() {
        const params = {
          type: "2",
          id: this.group_id,
        };
        if (this.level == "instance") {
          params.app_id = this.$store.getters.appId;
        }
        return params;
      },
      permissionArg() {
        return this.level == "organization" ? "" : "ins";
      },
      dataFn() {
        return this.level == "organization" ? getOrganizationGroupResource : getInstanceGroupResource;
      },
      aliasMap() {
        return this.$store.getters.instanceAliasMap;
      },
      appendFn() {
        return this.level == "organization" ? apendOrganizationGroupResource : apendInstanceGroupResource;
      },
    },
    watch: {
      group_id: {
        handler: function () {
          this.getData();
        },
        immediate: true,
      },
    },
    methods: {
      addResource() {
        this.dialog = true;
        this.$nextTick(() => {
          this.$refs.resourceRole.reset();
        });
      },
      getData() {
        this.dataFn(this.group_id).then(data => {
          this.resource = sortResource(data);
        });
      },
      submit() {
        if (!this.newResource.length) return (this.dialog = false);
        this.appendFn(this.newResource, this.group_id).then(() => {
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
