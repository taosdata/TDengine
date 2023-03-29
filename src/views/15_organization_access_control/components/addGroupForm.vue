<template>
  <el-form style="text-align: left" :rules="rules" :hide-required-asterisk="true" ref="form" label-position="left" label-width="100px" :model="info">
    <el-form-item :label="$t('name')" prop="name" required>
      <el-input v-model="info.name"></el-input>
    </el-form-item>

    <template v-if="!isEdit">
      <el-form-item :label="$t('template')">
        <UserRoleTemplate @change="resources => (info.resources = resources)" :level="0" type="group" />
      </el-form-item>
      <el-form-item :label="$t('accessControl.users')" prop="user_ids">
        <UserSelect v-model="info.user_ids" />
      </el-form-item>
      <el-form-item :label="$t('role')" prop="resources" required>
        <RoleSelect :params="params" v-model="info.resources" />
      </el-form-item>
    </template>

    <p v-if="!isEdit" class="simple-tip">{{ $t("accessControl.addGroupUserTip") }}</p>
    <el-form-item label=" ">
      <el-button v-permission="permission" :key="permission" class="w100" type="primary" @click="submit" :disabled="requestIng">
        {{ $t(isEdit ? "change" : "create") }}
      </el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import UserSelect from "@/components/UserSelect/select";
  import { createGroup, updateGroup } from "@/api/gateway/data/dbs";
  import UserRoleTemplate from "@/components/TempleteRole";
  import RoleSelect from "@/components/ResourceRole";
  export default {
    name: "",
    mixins: [],
    components: { UserSelect, UserRoleTemplate, RoleSelect },
    props: {
      info: {
        type: Object,
        default: () => ({ name: "", user_ids: [], resources: [] }),
      },
    },
    data() {
      return {
        requestIng: false,
      };
    },
    computed: {
      isEdit() {
        return !!this.info.id;
      },
      params() {
        return {};
      },
      permission() {
        return this.isEdit ? "group:edit" : "group:add";
      },
      rules() {
        return {
          name: [{ required: true, message: this.$t("required", [this.$t("name")]) }],
          resources: [{ required: true, message: this.$t("required", [this.$t("role")]) }],
        };
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      submit() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            const fn = this.isEdit ? updateGroup : createGroup;
            fn(this.info, this.info.id)
              .then(() => {
                this.$refs.form.resetFields();
                this.$message.success(this.$t(this.isEdit ? "changeSucc" : "createSucc"));
                this.$emit("close");
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
