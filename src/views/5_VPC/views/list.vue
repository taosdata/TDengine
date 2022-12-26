<template>
  <div>
    <section class="list-header">
      <el-button class="big-button" :disabled="cannotAdd" icon="el-icon-plus" @click="add" size="small" plain>{{ $t("vpc.addNewVPC") }}</el-button>
    </section>
    <VPC></VPC>
    <p v-if="cannotAdd" @click="upgrade" class="default-tip" v-html="$t('vpc.upgradeTip')"></p>
    <el-dialog center width="750px" :visible.sync="dialog">
      <CreateVPC @close="close" />
    </el-dialog>
  </div>
</template>

<script>
  import VPC from "../components/vpc.vue";
  import CreateVPC from "../components/createVPC.vue";
  export default {
    components: {
      VPC,
      CreateVPC,
    },
    data() {
      return {
        dialog: false,
      };
    },
    computed: {
      cannotAdd() {
        return !this.$store.getters.currentServerLevel;
      },
    },
    created() {},
    methods: {
      close() {
        this.dialog = false;
      },
      add() {
        this.dialog = true;
      },
      upgrade(e) {
        if (e.target.tagName == "A") {
          this.$store.commit("SET_UPGRADE_DIALOG_VISIBLE", true);
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .list-header {
    text-align: right;
  }
</style>
