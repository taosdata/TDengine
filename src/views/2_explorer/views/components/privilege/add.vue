<template>
  <div class="">
    <el-form style="text-align: left" ref="form" :model="info" label-position="left" label-width="120px">
      <el-form-item v-if="isUser" :label="$t('user')" prop="user_id" required>
        <UserSelect
          :filterList="userList"
          :placeholder="$t('accessControl.chooseUserTip')"
          :level="0"
          :multiple="false"
          v-model="info.user_id"
          :type="type"
        />
      </el-form-item>
      <el-form-item v-else :label="$t('accessControl.group')" prop="groupId" required>
        <UserSelect
          :filterList="groupList"
          :placeholder="$t('accessControl.chooseGroupTip')"
          :level="0"
          :multiple="false"
          v-model="info.groupId"
          :type="type"
        />
      </el-form-item>
      <el-form-item :label="$t('template')">
        <Template :type="type" :level="2" :params="params" @change="roles => (info.roles = roles)" />
      </el-form-item>
      <el-form-item :label="$t('role')" prop="roles">
        <ResourceRole :params="params" v-model="info.roles" />
      </el-form-item>
      <el-form-item label=" ">
        <el-button class="w100" v-permission @click="add" type="primary" size="small">{{ $t("add") }}</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
  import { apendInstanceResource } from "@/api/user";
  import { apendInstanceGroupResource } from "@/api/gateway/data/dbs";
  import UserSelect from "@/components/UserSelect/select.vue";
  import ResourceRole from "@/components/ResourceRole.vue";
  import Template from "@/components/TempleteRole.vue";
  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
    },
    components: { UserSelect, ResourceRole, Template },
    data() {
      return {
        info: {
          user_id: "",
          groupId: "",
          roles: [],
        },
        requestIng: false,
        roles: [],
      };
    },
    computed: {
      isUser() {
        return this.type === "user";
      },
      userList() {
        return this.$store.state.dbs.dbUser.map(item => ({ userId: item.id }));
      },
      groupList() {
        return this.$store.state.dbs.dbGroup;
      },
      infoData() {
        return this.$store.state.console.currentInfoData;
      },
      params() {
        return {
          app_id: this.$store.getters.appId,
          databaseId: this.infoData.databaseId,
        };
      },
      db() {
        return this.$store.state.dbs.selected_db;
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      add() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            const params = this.isUser ? [this.info] : [this.info.roles, this.info.groupId];
            const fn = this.isUser ? apendInstanceResource : apendInstanceGroupResource;
            fn(...params)
              .then(() => {
                this.$refs.form.resetFields();
                this.$message.success(this.$t("addSucc"));
                this.$emit("close");
              })
              .finally(() => {
                this.requestIng = false;
              });
          } else {
            return false;
          }
        });
      },
    },
  };
</script>

<style scoped lang="scss"></style>
