<template>
  <div>
    <template v-if="step == 1">
      <transition v-if="displayDetail" name="plan">
        <!-- <el-button icon="el-icon-arrow-left" style="margin-bottom: 10px" @click="displayDetail = false" circle></el-button> -->
        <el-table border :data="displayKey">
          <el-table-column align="center" label="" width="180">
            <template slot-scope="{ row }">
              <strong>{{ $t("plan." + row) }}</strong>
            </template>
          </el-table-column>
          <el-table-column align="center" label="Free Plan" prop="desc" min-width="180">
            <template slot-scope="{ row }">
              {{ freeDetail[row] }}
            </template>
          </el-table-column>
          <el-table-column align="center" label="Standard Plan" prop="summary" min-width="180">
            <template slot-scope="{ row }">
              {{ standardDetail[row] }}
            </template>
          </el-table-column>
          <el-table-column align="center" label="Enterprise Plan" prop="price" min-width="180">
            <template slot-scope="{ row }">
              <span style="cursor: pointer" @click="concatUs(enterpriseDetail[row])">{{ enterpriseDetail[row] }}</span>
            </template>
          </el-table-column>
        </el-table>
      </transition>
      <transition name="plan" v-else>
        <div class="planContainer" v-loading="loading">
          <el-card v-for="(item, index) in planList" @click.native="handle(index)" :key="item.name" :class="[item.class]" class="plan">
            <div class="titleContainer" :class="{ active: item.priceLevel == currentPlanIndex }">
              <div class="title">{{ item.name }}</div>
            </div>
            <div class="infoContainer">
              <!-- <div class="price">
          <p>$6</p>
          <span>/mo</span>
        </div> -->
              <div class="p desc">
                <em>{{ item.desc }}</em>
              </div>
              <ul class="features">
                <template v-if="item.summary">
                  <li v-for="ite in item.summary" :key="ite">
                    <span class="title">
                      {{ $t("plan." + ite) }}
                    </span>
                    <span class="value">{{ standardDetail[ite] }}</span>
                  </li>
                </template>
              </ul>
            </div>
            <el-button
              :type="item.btnType"
              :disabled="currentPlanIndex >= index || !plan.length"
              @click.stop="handle(index)"
              size="medium"
              class="selectPlan"
              >{{ item.btnText }}</el-button
            >
          </el-card>
        </div>
      </transition>
      <el-divider></el-divider>
      <section class="flexCenter">
        <button v-if="plan.length" plain :disabled="!plan.length" @click="displayDetail = !displayDetail" class="plan-detail">
          {{ $t(displayDetail ? "plan.planTitle" : "plan.planDetail") }}
        </button>
      </section>
    </template>
    <CreditCard v-else-if="step == 2" @close="close" />
  </div>
</template>

<script>
  import CreditCard from "@/components/CreditCard";
  import { getPlan } from "@/api/gateway/app";
  import { BusinessEmail } from "@/const";
  export default {
    props: {
      currentPlan: {
        type: String,
        default: "",
      },
      cloudAndRegion: {
        type: Object,
        default: () => {
          return {
            cloudId: "1",
            regionId: "1",
          };
        },
      },
    },
    components: { CreditCard },
    data() {
      this.displayKey = [
        "dataIn",
        "storage",
        "dataOut",
        "queries",
        "inserts",
        "instances",
        "databases",
        "tables",
        "dataRetention",
        "dataBackup",
        "dataSynchronization",
        "vpcSupport",
        "users",
        "serviceLevel",
      ];
      return {
        planStandardMap: {},
        displayDetail: false,
        step: 1,
        plan: [],
        historyPlan: {},
        timer: null,
        loading: false,
      };
    },
    computed: {
      planList() {
        return this.$t("plan.planList");
      },
      currentPlanIndex() {
        return this.plan.findIndex(item => item.priceLevel === this.currentPlan);
      },
      hasCard() {
        return !!this.$store.state.billing.creditCardInfo.cardNumber;
      },
      freeDetail() {
        const free = this.plan[0];
        if (!free) return {};
        return {
          dataIn: this.$t("plan.freeDetail.dataIn").replace("{size}", 5).replace("{time}", free.limitRateInsertByteInterval.replace(/\D/g, "")),
          storage: this.$t("plan.free"),
          dataOut: this.$t("plan.freeDetail.dataOut").replace("{size}", 5).replace("{time}", free.limitRateQueryByteInterval.replace(/\D/g, "")),
          queries: this.$t("plan.freeDetail.queries")
            .replace("{size}", free.limitRateQueryNum)
            .replace("{time}", free.limitRateQueryNumInterval.replace(/\D/g, "")),
          inserts: this.$t("plan.freeDetail.inserts")
            .replace("{size}", free.limitRateInsertNum)
            .replace("{time}", free.limitRateInsertNumInterval.replace(/\D/g, "")),
          instances: this.$t("plan.freeDetail.clusters").replace("{size}", free.clusterNum),
          databases: this.$t("plan.freeDetail.databases").replace("{size}", free.databaseNum),
          tables: this.$t("plan.freeDetail.tables").replace("{size}", free.stableNum),
          dataRetention: this.$t("plan.freeDetail.dataRetention").replace("{time}", free.dataRetention),
          dataBackup: this.$t("plan.notIncluded"),
          dataSynchronization: this.$t("plan.notIncluded"),
          vpcSupport: this.$t("plan.notIncluded"),
          users: this.$t("plan.freeDetail.users").replace("{size}", free.accountUserNum),
          serviceLevel: this.$t("plan.discordChannel"),
        };
      },
      standardDetail() {
        const standard = this.plan[1];
        if (!standard) return {};
        return {
          dataIn: this.$t("plan.standardDetail.dataIn").replace("{size}", standard.ingressPrice),
          storage: this.$t("plan.standardDetail.storage").replace("{size}", standard.storageHourPrice),
          dataOut: this.$t("plan.standardDetail.dataOut").replace("{size}", 0.09),
          queries: this.$t("plan.standardDetail.queries").replace("{size}", standard.queryCountPrice).replace("{count}", standard.queryCount),
          inserts: this.$t("plan.standardDetail.inserts").replace("{size}", standard.insertCountPrice).replace("{count}", standard.insertCount),
          instances: this.$t("plan.unlimited"),
          databases: this.$t("plan.unlimited"),
          tables: this.$t("plan.unlimited"),
          dataRetention: this.$t("plan.unlimited"),
          dataBackup: this.$t("plan.included"),
          dataSynchronization: this.$t("plan.included"),
          vpcSupport: this.$t("plan.included"),
          users: this.$t("plan.unlimited"),
          serviceLevel: this.$t("plan.supportTickets"),
        };
      },
      enterpriseDetail() {
        return {
          dataIn: this.$t("plan.contactSales"),
          storage: this.$t("plan.contactSales"),
          dataOut: this.$t("plan.contactSales"),
          queries: this.$t("plan.contactSales"),
          inserts: this.$t("plan.contactSales"),
          instances: this.$t("plan.unlimited"),
          databases: this.$t("plan.unlimited"),
          tables: this.$t("plan.unlimited"),
          dataRetention: this.$t("plan.unlimited"),
          dataBackup: this.$t("plan.included"),
          dataSynchronization: this.$t("plan.included"),
          vpcSupport: this.$t("plan.notIncluded"),
          users: this.$t("plan.unlimited"),
          serviceLevel: this.$t("plan.contactSales"),
        };
      },
    },

    watch: {
      step(val) {
        this.$emit("update:step", val);
      },
      cloudAndRegion: {
        handler() {
          this.getPlan();
        },
        deep: true,
        immediate: true,
      },
    },
    methods: {
      async getPlan() {
        let key = this.cloudAndRegion.cloudId + "_" + this.cloudAndRegion.regionId;
        if (this.historyPlan[key]) {
          this.plan = this.historyPlan[key];
        } else {
          this.loading = true;
          this.plan = await getPlan({
            cloudId: this.cloudAndRegion.cloudId,
            regionId: this.cloudAndRegion.regionId,
            planType: "BASE",
          }).catch(() => []);
          if (this.plan.length > 0) {
            this.historyPlan[key] = this.plan;
          }
          this.loading = false;
        }
      },
      handle(index) {
        switch (index) {
          case 0:
            break;
          case 1:
            // 如果没有信用卡信息需要输入信用卡信息后才能选择
            if (!this.hasCard) {
              return (this.step = 2);
            }
            break;
          case 2:
            return this.$alert(this.$t("plan.concatBusiness").replace(/{email}/g, BusinessEmail), this.$t("tips"), {
              confirmButtonText: this.$t("confirm"),
              dangerouslyUseHTMLString: true,
              callback: () => {},
            });
          default:
            break;
        }
        this.$emit("change", this.plan[index]);
        this.$emit("close");
      },
      close() {
        this.step = 1;
      },
      concatUs(name) {
        if (name !== this.$t("plan.contactSales")) return;
        this.$emit("close");
        this.$store.commit("SET_CONTACT_DIALOG_VISIBLE", true);
      },
    },
  };
</script>

<style lang="scss" scoped>
  $accent-color: #1abc9c;
  $text-color: #2d3b48;
  $plan-padding: 1em;
  $plan-margin: 1em;
  $title-background: #f3f3f3;
  $active-bac: #2ecc71;
  $active-color: #fff;
  $title-size: 1.45em;
  $price-size: 1.35em;
  $feature-size: 1em;
  .planContainer {
    display: flex;
    flex-wrap: wrap;
    margin: 0 $plan-margin 0;
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-around;
    position: relative;
    // padding-bottom: 60px;
  }
  .plan-detail {
    font-size: 16px;
    // width: 30%;
    // padding: 10px;
    background: none;
    border: none;
    cursor: pointer;
    color: $color-primary;
  }
  .plan {
    background: white;
    width: 24em;
    cursor: pointer;
    box-sizing: border-box;
    text-align: center;
    margin: 0 $plan-margin $plan-margin;
    // margin-bottom: $plan-margin;
    position: relative;
    padding-bottom: 50px;
    transition: all 0.3 ease-in-out;
    &:hover {
      // transform: scale(1.1);
      .titleContainer {
        color: $active-color;
        background-color: $active-bac;
      }
    }

    .image {
      max-width: 100%;
      max-height: 200px;
      object-fit: contain;
    }
    .titleContainer {
      $clip-path-y: 10px;
      $clip-path-x: 10px;
      background-color: $title-background;
      padding: $plan-padding;
      color: $accent-color;
      clip-path: polygon(
        0 $clip-path-y,
        $clip-path-x 0,
        calc(100% - #{$clip-path-x}) 0,
        100% $clip-path-y,
        100% calc(100% - #{$clip-path-y}),
        calc(100% - #{$clip-path-x}) 100%,
        $clip-path-x 100%,
        0 calc(100% - #{$clip-path-y})
      );
      .title {
        font-size: $title-size;
        text-transform: uppercase;
        font-weight: 700;
      }
      &.active {
        color: $active-color;
        background-color: $active-bac;
      }
    }
    .infoContainer {
      padding: $plan-padding;
      color: $text-color;
      box-sizing: border-box;

      .price {
        font-size: $price-size;
        padding: $plan-padding 0;
        font-weight: 600;
        margin-top: 0;
        display: inline-block;
        width: 80%;
        p {
          font-size: $price-size;
          display: inline-block;
          margin: 0;
        }
        span {
          font-size: $price-size * 0.75;
          display: inline-block;
        }
      }
      .desc {
        padding-bottom: $plan-padding;
        border-bottom: 2px solid $title-background;
        margin: 0 auto;
        width: 90%;
        word-break: keep-all;
        em {
          font-size: $feature-size;
          font-weight: 500;
        }
      }
      .features {
        font-size: $feature-size;
        list-style: none;
        padding-left: 0;
        background-size: auto 100%;
        background-repeat: no-repeat;
        background-position: center center;
        min-height: 200px;
        li {
          padding: $plan-padding/2;
          text-align: left;
          display: flex;
          line-height: 18px;
          .title {
            width: 90px;
            font-size: 14px;
            flex-shrink: 0;
            padding-right: 10px;
            color: #757373;
          }
          .value {
            // text-align: center;
            flex: 1;
            // color: #414141;
          }
        }
      }
    }
    .selectPlan {
      position: absolute;
      bottom: 14px;
      left: 20px;
      right: 20px;
    }
  }
  .free .features {
    background-image: url("~@/assets/images/free-plan-bg.svg");
  }
  .standard .features {
    background-image: url("~@/assets/images/standard-plan-bg.svg");
  }
  .enterprise .features {
    background-image: url("~@/assets/images/enterprise-plan-bg.svg");
  }
  @media screen and (max-width: 25em) {
    .planContainer {
      margin: 0;
      .plan {
        width: 100%;
        margin: $plan-margin 0;
      }
    }
  }
  .plan-enter-active,
  .plan-leave-active {
    transition: transform 0.8s;
  }
  .plan-enter, .plan-leave-to /* .fade-leave-active below version 2.1.8 */ {
    position: absolute;
    opacity: 0.2;
    transform: rotateY(180deg);
  }
</style>
