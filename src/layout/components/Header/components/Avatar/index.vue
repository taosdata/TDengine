<template>
  <div class="avatar_wrapper">
    <el-dropdown trigger="hover" placement="bottom">
      <div class="avatar_block">
        <span>{{ user }}</span>
      </div>
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item>
          <router-link class="drop-block" to="/profile">
            <Icon name="profile" class="dropdown_icon"></Icon>
            {{ $t("setting.profile") }}
          </router-link>
        </el-dropdown-item>
        <!-- <el-dropdown-item v-if="hasActivity">
          <router-link class="drop-block" to="/activity">
            <Icon name="activity" class="dropdown_icon"></Icon>
            {{ $t("route.activity") }}</router-link
          >
        </el-dropdown-item> -->
        <div class="custom-divider"></div>
        <el-dropdown-item>
          <div @click="logout" class="drop-block">
            <!-- 图标有问题，需特殊处理 -->
            <Icon name="signout" class="dropdown_icon" style="width: 20px; height: 20px;"></Icon>
            <span style="color:#4259ce">{{ $t("signOut") }}</span>
            
          </div>
        </el-dropdown-item>
      </el-dropdown-menu>
    </el-dropdown>
    <!-- <el-dialog
      :title="dialogInfo.title"
      :visible.sync="dialogVisible"
      :modal-append-to-body="dialogInfo.modal"
      :width="dialogInfo.width"
    >
      <changeForm
        v-if="currentComp == 1"
        :needEmail="false"
        @close="close"
      ></changeForm>
      <profile v-if="currentComp == 2" @close="close" />
    </el-dialog> -->
  </div>
</template>

<script>
  import Icon from "@/components/Icon";
  export default {
    name: "avatar",         
    components: { Icon },
    data() {
      return {
        dialogVisible: false,
        currentComp: 1,
        dialogInfo: {
          title: "",
          modal: false,
          width: "500px",
        },
      };
    },
    computed: {
      user() {
        return this.$store.state.app.userInfo?.lastname?.trim()?.slice(0, 1)?.toUpperCase() || "T";
      },
      hasActivity() {
        return this.$store.getters.role == "1";
      },
    },
    methods: {
      logout() {
        localStorage.removeItem('base_url')
        localStorage.removeItem('documentWebsite')
        localStorage.removeItem('supportWebsite')
        localStorage.removeItem('TDengine-Token')
        localStorage.removeItem('username')
        localStorage.removeItem('pwd')
        this.$store.dispatch("app/logout");
        this.$router.push({
          path:'/login'
        })
      },
      close() {
        this.dialogVisible = false;
      },
      changePass() {
        this.dialogInfo = {
          title: this.$t("changePass"),
          modal: false,
          width: "500px",
        };
        this.currentComp = 1;
        this.dialogVisible = true;
      },
      setting() {
        this.dialogInfo = {
          title: this.$t("setting.profile"),
          modal: false,
          width: "500px",
        };
        this.currentComp = 2;
        this.dialogVisible = true;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .avatar_wrapper {
    cursor: pointer;
  }

  .avatar_block {
    margin-top: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    border: 1px solid $color-primary;
    border-radius: 50%;
    color: $color-primary;
  }
  .avatar_svg {
    width: 26px;
    height: 26px;
  }
  .drop-block {
    display: flex;
    align-items: center;
    padding: 6px 0;
  }
  .dropdown_icon {
    width: 20px;
    height: 20px;
    margin-right: 8px;
  }
  .custom-divider{
    display: none;
  }
</style>
