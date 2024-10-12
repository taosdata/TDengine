<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('accessControl.orgTitle')"></MainContentHeader>
    <div class="content">
      <LinkTab :tabs="tabs" />
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
  export default {
    data() {
      return {};
    },
    computed: {
      tabs() {
        const tab = [];
        if (this.$hasOrganizationPrivilege(["user:remove", "user:invite", "user:update"], false)) {
          tab.push({
            label: this.$t("accessControl.users"),
            name: "/organizationAccessControl",
          });
        }
        if (this.$hasOrganizationPrivilege(["group:delete", "group:edit", "group:add"], false)) {
          tab.push({
            label: this.$t("accessControl.userGroups"),
            name: "/organizationAccessControl/userGroups",
          });
        }
        if (this.$hasOrganizationPrivilege(["role:delete", "role:update", "role:add"], false)) {
          tab.push({
            label: this.$t("accessControl.roles"),
            name: "/organizationAccessControl/roles",
          });
        }
        // if (this.$hasOrganizationPrivilege(["user-role:grant", "user-role:delete", "group-role:grant", "group-role:delete"], false)) {
        //   tab.push({
        //     label: this.$t("accessControl.grants"),
        //     name: "/organizationAccessControl/grants",
        //   });
        // }
        return tab;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
