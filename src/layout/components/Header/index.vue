<template>
  <div class="header">
    <div class="headerLeft">
      <!-- <ClusterSelector></ClusterSelector> -->
      <ul class="license" v-if="license[0]">
        <!-- <li>
          <span>{{ $t("dashboard.expiretime") }}：</span>
          <span class="value">{{this.license[0].expire_time | filterNull}}</span>
        </li> -->
        <li>
          <span class="version">{{ $t("header.version") }}：</span>
          <span class="value">{{ version }}</span>
        </li>
      </ul>
    </div>
    <div class="headerRight">
      <Timezone></Timezone>
      <Document v-if="docUrl"></Document>

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
} from "./components";
export default {
  components: { Avatar, ClusterSelector, Help, Support, Document, Timezone },
  data() {
    return {
      issueTypeList: [],
      license: [],
      version: "",
      supportUrl: localStorage.getItem("supportWebsite"),
      docUrl: localStorage.getItem("documentWebsite"),
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
  },
  created() {
    this.getLicense();
  },
  methods: {
    getVersion(val) {
      return val.substr(0, val.lastIndexOf("."));
    },
    async getLicense() {
      try {
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
          this.version =
            this.getVersion(this.license[0]["server_version()"]) +
            " " +
            this.license[0].version.charAt(0).toUpperCase() +
            this.license[0].version.slice(1) +
            " " +
            (this.license[0].version == "trial"
              ? this.license[0].valid
                ? "Expired"
                : ""
              : this.license[0].valid
              ? "License Expired"
              : "");
          console.log(
            this.version,
            "this.license---header",
            this.license[0].version,
            this.license[0].valid,
            this.license[0].version == "trial"
              ? this.license[0].valid
                ? "Expired"
                : ""
              : this.license[0].valid
              ? "License Expired"
              : ""
          );
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
</style>
