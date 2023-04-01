<template>
  <el-tabs v-model="activiteName">
    <el-tab-pane  name="user" :label="$t('accessControl.users')">
      <List />
    </el-tab-pane>
    <!-- <el-tab-pane v-if="groupShow" name="userGroup" :label="$t('accessControl.userGroups')">
      <List type="group" />
    </el-tab-pane> -->
  </el-tabs>
</template>

<script>
  import List from "./list";
  export default {
    name: "",
    mixins: [],
    components: { List },
    props: {},
    data() {
      return {
        activiteName: "user",
      };
    },
    provide() {
      return {
        dbPrivilege: this.dbPrivilege,
      };
    },
    computed: {
      currentDB() {
        return this.$store.state.console.currentInfoData.name;
      },
      dbPrivilege() {
        return this.$store.state.console.currentInfoData.privileges;
      },
      userShow() {
        return this.dbPrivilege.some(item => item.name.includes("user"));
      },
      groupShow() {
        return this.dbPrivilege.some(item => item.name.includes("group"));
      },
    },
    watch: {
      // currentDB: {
      //   handler() {
      //     if (this.userShow) {
      //       this.$store.dispatch("dbs/getDBUserList");
      //     }
      //     if (this.groupShow) {
      //       this.$store.dispatch("dbs/getDBGroupList");
      //       if (!this.userShow) {
      //         this.activiteName = "userGroup";
      //       }
      //     }
      //   },
      //   immediate: true,
      // },
    },
    created() {},
    mounted() {},
    methods: {},
  };
</script>

<style scoped lang="scss"></style>
