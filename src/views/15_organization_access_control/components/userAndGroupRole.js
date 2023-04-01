import {
  enableOrganizationGroupRole,
  enableInstanceGroupRole,
  deleteInstanceGroupRole,
  disableInstanceGroupRole,
  disableOrganizationGroupRole,
  deleteOrganizationGroupRole,
} from "@/api/gateway/data/dbs";
import {
  enableInstanceUserRole,
  deleteInstanceUserRole,
  enableOrganizationUserRole,
  disableOrganizationUserRole,
  deleteOrganizationUserRole,
  disableInstanceUserRole,
} from "@/api/gateway/data/dbs";
const ableParams = ["status", "expiration", "version"];
export default {
  computed: {
    dataIdFiled() {
      return this.type + "RoleId";
    },
    disableFn() {
      return {
        user: {
          organization: disableOrganizationUserRole,
          instance: disableInstanceUserRole,
        },
        group: {
          organization: disableOrganizationGroupRole,
          instance: disableInstanceGroupRole,
        },
      }[this.type][this.level];
    },
    enableFn() {
      return {
        user: {
          organization: enableOrganizationUserRole,
          instance: enableInstanceUserRole,
        },
        group: {
          organization: enableOrganizationGroupRole,
          instance: enableInstanceGroupRole,
        },
      }[this.type][this.level];
    },
    deleteFn() {
      return {
        user: {
          organization: deleteOrganizationUserRole,
          instance: deleteInstanceUserRole,
        },
        group: {
          organization: deleteOrganizationGroupRole,
          instance: deleteInstanceGroupRole,
        },
      }[this.type][this.level];
    },
  },
  methods: {
    statusChange(val, data) {
      if (this.requesting) return;
      this.$confirm(this.getTipName(data, val ? "enable" : "disable"), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      }).then(async () => {
        this.requesting = true;
        const params = this.getAbleParams(data);
        const fn = val ? this.enableFn : this.disableFn;
        await fn(params)
          .then(() => {
            this.$message.success(this.$t("operateSucc"));
          })
          .catch(() => {});
        this.requesting = false;
        this.getData();
      });
    },

    getAbleParams(data) {
      const result = Object.fromEntries(ableParams.map(item => [item, data[item]]));
      result[this.getParamsIdFiled(data)] = data[this.dataIdFiled] || data.id;
      return result;
    },
    del(data) {
      if (this.requesting) return;
      this.$confirm(this.getTipName(data, "del"), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      }).then(async () => {
        this.requesting = true;

        this.deleteFn({ [this.getParamsIdFiled(data)]: data[this.dataIdFiled] || data.id, version: data.version })
          .then(() => {
            this.$message.success(this.$t("delSucc"));
          })
          .finally(() => {
            this.requesting = false;
            this.getData();
            this.$emit("update");
          });
      });
    },
    getTipName(data, type) {
      const email = this.email || this.$t("usernameTep", [data.firstName, data.lastName]);
      let tip = this.$t("accessControl.changeRoleTip", [
        data.roleName?.toLowerCase(),
        this.$t(type).toLowerCase(),
        this.$t(this.type).toLowerCase(),

        this.isUser ? email : this.group_name || data.groupName,
      ]);

      return tip || "the " + data.roleName + " role";
    },
    getParamsIdFiled(data) {
      if (data.groupId) return "group_role_id";
      return this.type + "_role_id";
    },
  },
};
