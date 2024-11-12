<template>
  <div class="header">
    <div
      :class="['headerLeft', showHeaderLeft ? '' : 'hidden']"
      @click="clickShowVersion"
    >
      <!-- <ClusterSelector></ClusterSelector> -->
      <ul class="license" v-if="license[0]">
        <!-- <li>
          <span>{{ $t("dashboard.expiretime") }}：</span>
          <span class="value">{{this.license[0].expire_time | filterNull}}</span>
        </li> -->
        <li>
          <span class="version">{{ $t(`header.${industry}`) }}：</span>
          <span class="value" :style="{ color: version.includes('Expired') ? 'red': ''}">{{ version }}</span>
        </li>
      </ul>
    </div>
    <div class="headerRight">
      <Timezone></Timezone>
      <Document v-if="docUrl"></Document>
      <!-- <International></International> -->
      <!-- <Support v-if="supportUrl"></Support>
      <Document v-if="docUrl"></Document> -->
      <!-- <Github></Github> -->
      <!-- <International></International> -->

      <!-- <el-tooltip class="item" effect="light" :content="$t('route.alerts')" placement="top-start" v-if="hasAlert">
        <router-link class="header-item" to="/alert" :class="{ alert: alerts }">
          <Icon name="alert" class="avatar_svg"></Icon>
        </router-link>
      </el-tooltip>
      <Help></Help> -->
      <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
      <Avatar></Avatar>

     
    </div>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";

import {
  Avatar,
  ClusterSelector,
  Help,
  Support,
  Document,
  Timezone,
  International,
} from "./components";
export default {
  components: {
    Avatar,
    ClusterSelector,
    Help,
    Support,
    Document,
    Timezone,
    International,
  },
  data() {
    return {
      showHeaderLeft: true,
      clickCount: 0,
      clickNum: 0,
      issueTypeList: [],
      license: [],
      version: "",
      supportUrl: localStorage.getItem("supportWebsite"),
      docUrl: localStorage.getItem("documentWebsite"),
      grants: [],
      industry: 'version'
    };
  },
  filters: {
    filterNull(val) {
      if (Object.is(val, null)) {
        return 0;
      } else {
        return val;
      }
    },
  },
  computed: {
    alerts() {
      return this.$store.state.app.newAlert.length;
    },
    hasAlert() {
      return this.$store.getters.role == "1";
    },
    locallanguage(){
      if(this.$i18n.locale=='zh'){
        return 'EN'
      }else{
        return '中'
      }
    },
    oem() {
      let oem =  process.env.VUE_APP_CUS_NAME && process.env.VUE_APP_CUS_NAME !== "TDengine"
        ? process.env.VUE_APP_CUS_NAME
        : "TDengine";
      return oem;
    }
  },
  // 监听,当路由发生变化的时候执行
  watch:{
    $route: {
      handler (to,from,next){
        try {
          if (to.name != "Login") {
            this.getLicense()
          }
          next && next();
        } catch (error) {
          console.log(error);
        }
      },
      immediate: true
    }
  },
  created() {
    // this.getLicense();
    if (this.$COMMUNITY) {
      this.docUrl = false;
    }
  },
  mounted() {
    if (process.env.VUE_APP_CUS_CONFIG) {
      let config = JSON.parse(process.env.VUE_APP_CUS_CONFIG);
      if (Object.hasOwnProperty.call(config, "serverVersionDisplay")) {
        this.showHeaderLeft = config?.serverVersionDisplay?.hide;
      }

      this.clickCount = config?.serverVersionDisplay?.showByClick;
    }
  },
  methods: {
    switchLanguage() {
      if(this.$i18n.locale=='zh'){
        this.$i18n.locale='en'
        localStorage.setItem("local_language", "en");
      }else{
        this.$i18n.locale='zh'
        localStorage.setItem("local_language", "zh");
      }
    },
   
    clickShowVersion() {
      if (process.env.VUE_APP_CUS_CONFIG) {
        this.clickNum++;
        if (this.clickNum > this.clickCount) return;
        if (this.clickNum == this.clickCount) {
          this.showHeaderLeft = true;
        }
      }
    },
    getVersion(val) {
      if (val.match(/\./g).length > 3) {
        return val.substr(0, val.lastIndexOf("."));
      } else {
        return val;
      }
    },
    async getLicense() {
      try {
        let res = await sendSQLReq('show grants;')
        this.grants = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        await sendSQLReq(
          `select server_version(), version, (expire_time < now) as valid from information_schema.ins_cluster;`
        ).then((res) => {
          this.license = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          localStorage.setItem("agent_version", this.getVersion(this.license[0]["server_version()"]));
          let versionName = ''
          switch (this.grants[0].version) {
            case "trial":
            case `${this.oem} Enterprise Edition trial`:
              versionName = this.license[0].valid
                ? "Trial Expired"
                : "Trial"
              break;
            case "official":
            case `${this.oem} Enterprise Edition official`:
              versionName = this.license[0].valid
                ? "Enterprise License Expired"
                : "Enterprise"
              break;
            case "TDengine Power Edition trial":
              versionName = "Trial";
              this.industry = "power";
              break;
            case "TDengine Power Edition official":
              versionName = "Official"
              this.industry = "power";
              break;
            default:
              versionName = "Community"
              break;
          }
          this.version =
            this.getVersion(this.license[0]["server_version()"]) +
            " " + versionName
          localStorage.setItem("serverVersion",this.version);
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.header {
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  background-color: #fff;
  padding-right: 40px;
  padding-left: 40px;
  position: sticky;
  top: 0;
  z-index: 1;
  height: 58px;
  width: 100%;
  border-bottom: 1px solid #eaecef;
  flex-shrink: 0;
}

.avatar_svg {
  width: 80%;
  height: 80%;
}
.header-item {
  margin-top: 4px;
  margin-right: 20px;
  border-radius: 50%;
  width: 25px;
  height: 25px;
  border: 1px solid $color-primary;
  color: $color-primary;
  @extend .flexCenter;
  cursor: pointer;
}
.alert {
  position: relative;
}
.alert::before {
  content: "";
  position: absolute;
  bottom: 5px;
  right: 0;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background-color: $color-danger;
  animation: blink 1.5s linear infinite;
}
@keyframes blink {
  0% {
    opacity: 0;
  }
  50% {
    opacity: 1;
  }
  100% {
    opacity: 0;
  }
}
.headerRight {
  display: flex;
  flex-direction: row;
  align-items: center;
}
.license {
  display: flex;
  span {
    font-size: 18px;
  }
  .value {
    color: #4259ce;
  }
  li {
    margin-right: 50px;
  }
}
.headerLeft.hidden {
  opacity: 0;
}
.language {
  margin-top: 4px;
  margin-right:20px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border: 1px solid #4259ce;
  border-radius: 50%;
  color: #4259ce;
}
</style>
