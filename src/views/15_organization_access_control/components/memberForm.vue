<template>
  <el-form :rules="rules" :hide-required-asterisk="true" ref="form" label-position="left" label-width="120px" :model="info">
    <el-form-item :label="$t('email')" prop="email" required>
      <el-input v-model="info.email"></el-input>
    </el-form-item>
    <el-form-item :label="$t('template')">
      <UserRoleTemplate @change="roles => (info.roles = roles)" :level="0" />
    </el-form-item>
    <el-form-item :label="$t('accessControl.userGroups')" prop="group_list">
      <UserSelect type="group" v-model="info.group_list" />
    </el-form-item>
    <el-form-item :label="$t('accessControl.roles')" prop="roles">
      <ResourceRole ref="resourceRole" v-model="info.roles" />
    </el-form-item>
    <el-form-item label=" ">
      <el-button class="w100" v-permission="'user:invite'" data-check="add" type="primary" @click="submit" :disabled="requestIng">
        {{ $t("add") }}
      </el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import { validEmail } from "@/utils/validate.js";
  import { inviteUser } from "@/api/user";
  import UserSelect from "@/components/UserSelect/select.vue";
  import ResourceRole from "@/components/ResourceRole.vue";
  import UserRoleTemplate from "@/components/TempleteRole";
  export default {
    name: "",
    mixins: [],
    components: { UserSelect, ResourceRole, UserRoleTemplate },
    props: {
      info: {
        type: Object,
        default: () => ({
          email: "",
          group_list: [],
          roles: [],
        }),
      },
    },
    data() {
      const checkEmail = async (_, value, callback) => {
        if (!value || !validEmail(value)) {
          return callback(new Error(this.$t("emailError")));
        }
      };
      const checkGroup = async (_, value, callback) => {
        if (!value.length && !this.info.roles.length) {
          return callback(new Error(this.$t("accessControl.groupAndRoleErrorTip")));
        }
      };
      const checkRole = async (_, value, callback) => {
        if (!value.length && !this.info.group_list.length) {
          return callback(new Error(this.$t("accessControl.groupAndRoleErrorTip")));
        }
      };
      return {
        requestIng: false,
        rules: {
          email: [{ validator: checkEmail, trigger: "blur" }],
          group_list: [{ validator: checkGroup, trigger: "change" }],
          roles: [{ validator: checkRole, trigger: "change" }],
        },
      };
    },
    computed: {},
    watch: {},
    created() {},
    mounted() {},
    methods: {
      submit() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            inviteUser(this.info)
              .then(() => {
                this.$refs.form.resetFields();
                this.$emit("close");
                this.$message.success(this.$t("addSucc"));
              })
              .finally(() => {
                this.requestIng = false;
              });
          }
        });
      },
    },
  };
</script>

<style scoped lang="scss"></style>
