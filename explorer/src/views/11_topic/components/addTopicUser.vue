<template>
  <el-form ref="form" label-position="left" label-width="120px" :model="info">
    <el-form-item v-if="isUser" :label="$t('route.users')" prop="user_ids" required>
      <UserSelect :filterList="filterList" v-model="info.user_ids" :type="type" />
    </el-form-item>
    <el-form-item v-else :label="$t('accessControl.userGroups')" prop="user_group_ids" required>
      <UserSelect :filterList="filterList" v-model="info.user_group_ids" :type="type" />
    </el-form-item>
    <el-form-item :label="$t('expiration')" prop="expiration">
      <UTCDateTimePicker
        class="w100"
        :picker-options="$root.afterTimePickerOptions"
        v-model="info.expiration"
        type="datetime"
        popper-class="resource-expiration-popover"
        value-format="timestamp"
        placeholder=""
      >
      </UTCDateTimePicker>
    </el-form-item>
    <el-form-item label=" ">
      <el-button :disabled="requestIng" class="w100" @click="add" type="primary">{{ $t("add") }}</el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import UserSelect from "@/components/UserSelect/select.vue";
  import UTCDateTimePicker from "@/components/UTCDateTimePicker.vue";
  import { addTopicUser, addTopicGroup } from "@/api/topic";
  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
      filterList: {
        type: Array,
        default: () => [],
      },
      topicId: {
        type: String,
        default: "",
      },
    },
    components: { UserSelect, UTCDateTimePicker },
    data() {
      return {
        info: {
          user_ids: [],
          user_group_ids: [],
          expiration: "",
        },
        requestIng: false,
      };
    },
    computed: {
      isUser() {
        return this.type === "user";
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
            const fn = this.isUser ? addTopicUser : addTopicGroup;
            fn({
              topic_id: this.topicId,
              ...this.info,
            })
              .then(() => {
                this.$refs.form.resetFields();
                this.$message.success(this.$t("addSucc"));
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
<style lang="scss">
  .resource-expiration-popover > .el-picker-panel__footer > button:nth-child(1) {
    display: none;
  }
</style>
