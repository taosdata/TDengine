<template>
  <el-form ref="form" :model="info" label-width="80px">
    <el-form-item :label="$t('route.users')" placeholder="" prop="user_ids" required>
      <el-select class="w100" placeholder="" multiple collapse-tags v-model="info.user_ids">
        <el-option v-for="item in userList" :key="item.userId" :label="item.email" :value="item.userId"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item label=" ">
      <el-button class="w100" size="small" type="primary" @click="submit">{{ $t("confirm") }}</el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import { grantTopic } from "@/api/topic";
  // import { getAppUser } from "@/api/app";
  // import { loadPageData } from "@/utils";
  export default {
    props: {
      topic_name: {
        type: String,
        default: "",
      },
    },
    components: {},
    data() {
      return {
        userList: [],
        info: {
          user_ids: [],
        },
        requestIng: false,
      };
    },
    computed: {
      currentUser() {
        return this.$store.getters.userInfo;
      },
    },
    watch: {},
    // created() {
    //   this.getUser();
    // },
    mounted() {},
    methods: {
      // getUser() {
      //   loadPageData(getAppUser).then(data => {
      //     this.userList = data.filter(item => item.userId != this.currentUser.id);
      //   });
      // },
      submit() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            grantTopic({
              topic_name: this.topic_name,
              user_ids: this.info.user_ids,
              app_id: this.$store.getters.appId,
            })
              .then(() => {
                this.$emit("close");
                this.$refs.form.resetFields();
                this.$message.success(this.$t("operateSucc"));
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
