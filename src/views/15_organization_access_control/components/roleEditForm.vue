<template>
  <el-form ref="form" :rules="rules" label-position="left" label-width="100px" :model="info">
    <el-form-item :label="$t('name')" prop="name">
      <el-input v-model="info.name"></el-input>
    </el-form-item>
    <el-form-item :label="$t('privileges')" prop="privileges" required>
      <PrivilegeTree v-model="info.privileges" />
    </el-form-item>
    <p class="errorText">{{ errorText }}</p>
    <el-form-item label=" ">
      <el-button v-permission="permission" :key="permission" class="w100" type="primary" @click="submit" :disabled="requestIng">
        {{ $t(isEdit ? "change" : "create") }}
      </el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import PrivilegeTree from "./privilegeTree.vue";
  import { createRole, updateRole } from "@/api/role";
  export default {
    components: { PrivilegeTree },
    props: {
      info: {
        type: Object,
        default: () => {
          return {
            name: "",
            privileges: {
              org: [],
              common: [],
              instance: [],
              db: [],
            },
          };
        },
      },
    },
    data() {
      const verifyPrivilege = (_, val, callback) => {
        this.errorText = "";
        if (val.org.length || val.common.length || val.instance.length || val.db.length) {
          callback();
        } else {
          callback(new Error(this.$t("accessControl.privilegeTip")));
        }
      };
      return {
        requestIng: false,
        errorText: "",
        rules: {
          name: [
            {
              required: true,
              message: this.$t("required", [this.$t("name")]),
              trigger: "blur",
            },
          ],
          privileges: [
            {
              validator: verifyPrivilege,
              trigger: "change",
            },
          ],
        },
      };
    },
    computed: {
      isEdit() {
        return !!this.info.id;
      },
      permission() {
        return this.isEdit ? "role:update" : "role:add";
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
            // if (
            //   Object.keys(this.info.privileges).every(key => {
            //     return !this.info.privileges[key].length;
            //   })
            // ) {
            //   this.$error(this.$t("accessControl.privilegeTip"));
            //   return;
            // }
            this.requestIng = true;
            if (this.isEdit) {
              return updateRole(
                {
                  accountId: this.info.accountId,
                  name: this.info.name,
                  version: this.info.version,
                  ...this.info.privileges,
                },
                this.info.id
              )
                .then(() => {
                  this.$message.success(this.$t("changeSucc"));
                  this.$emit("close");
                })
                .finally(() => {
                  this.requestIng = false;
                });
            }
            createRole({
              name: this.info.name,
              ...this.info.privileges,
            })
              .then(() => {
                this.$refs.form.resetFields();
                this.$message.success(this.$t("createSucc"));
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
