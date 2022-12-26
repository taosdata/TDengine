<template>
  <div class="payment">
    <p class="warning-tip" v-if="overview.state == 3" v-html="paymentFailedTip" @click="tipClick"></p>
    <section class="payment-card">
      <el-card class="custom-card">
        <template v-if="overview.state == 1">
          <section class="card-item w5">
            <h1 class="title">{{ $t("billing.occurredCost") }}</h1>
            <p class="num">${{ overview.occurredCost | handlePrice }}</p>
            <p>{{ $t("billing.since") }} {{ overview.startTime | handleDate }}</p>
          </section>
          <section class="card-item w5">
            <h1 class="title">{{ $t("billing.estimatedCost") }}</h1>
            <p class="num">${{ overview.estimatedCost | handlePrice }}</p>
            <p>{{ $t("billing.to") }} {{ overview.endTime | handleDate }}</p>
          </section>
        </template>
        <section v-else class="card-item w10">
          <h1 class="title">{{ $t("billing.cost") }}</h1>
          <p class="num">${{ overview.cost | handlePrice }}</p>
          <p>{{ overview.startTime | handleDate }} - {{ overview.endTime | handleDate }}</p>
        </section>
      </el-card>
      <el-card class="custom-card">
        <section class="card-item w5">
          <h1 class="title">{{ $t("billing.openingBalance") }}</h1>
          <p class="num">${{ overview["openingBalance"] | handlePrice }}</p>
          <p>{{ $t("billing.at") }} {{ overview.startTime | handleDate }}</p>
        </section>
        <section class="card-item w5">
          <h1 class="title">{{ $t("billing.credit") }}</h1>
          <p class="num">${{ overview.credit | handlePrice }}</p>
          <p v-if="overview.credit">{{ $t("billing.receivedAt") }} {{ overview.lastReceivedTime | handleDate }}</p>
        </section>
      </el-card>
      <el-card class="custom-card">
        <section v-if="overview.state == 1" class="card-item w10">
          <h1 class="title">{{ $t("billing.estimatedPyament") }}</h1>
          <p class="num">${{ overview.estimatedPayment | handlePrice }}</p>
          <p>{{ $t("billing.at") }} {{ overview.endTime | handleDate }}</p>
        </section>
        <section v-else class="card-item w10">
          <h1 class="title">{{ $t("billing.payment") }}</h1>
          <p class="num">${{ overview.payment | handlePrice }}</p>
          <p>{{ $t("billing.dueAt") }} {{ overview.dueTime | handleDate }}</p>
        </section>
      </el-card>
    </section>
    <section class="flexEnd">
      <el-date-picker
        size="mini"
        type="daterange"
        range-separator="—"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        :picker-options="$root.pickerOptions"
        align="left"
        value-format="timestamp"
        v-model="date"
        @change="handleChange"
      >
      </el-date-picker>
    </section>
    <el-table tooltip-effect="light" :data="receiptData">
      <el-table-column min-width="150" :show-overflow-tooltip="true" prop="receiptNo" label="ID"></el-table-column>
      <el-table-column width="150" :label="$t('date')">
        <template slot-scope="{ row }">
          {{ row.createTime | handleDate }}
        </template>
      </el-table-column>
      <el-table-column min-width="200" :show-overflow-tooltip="true" :label="$t('billing.servicePeriod')">
        <template slot-scope="{ row }"> {{ row.startTime | handleDate }}-{{ row.endTime | handleDate }} </template>
      </el-table-column>
      <el-table-column min-width="80" :label="$t('billing.cost')" prop="cost"></el-table-column>
      <el-table-column min-width="80" :label="$t('billing.credit')" prop="credit"></el-table-column>
      <el-table-column min-width="100" :label="$t('billing.payment')" prop="payment"></el-table-column>
      <el-table-column min-width="130" :label="$t('billing.paymentMethod')" prop="paymentMethodName"></el-table-column>
      <el-table-column min-width="130" :label="$t('billing.openingBalance')" prop="openingBalance"></el-table-column>
      <el-table-column min-width="120" :label="$t('billing.balance')" prop="balance"></el-table-column>
      <el-table-column :label="$t('billing.receipt')" width="100" fixed="right">
        <template slot-scope="{ row }">
          <a target="_blank" @click.prevent="getReceiptUrl(row)">{{ $t("download") }}</a>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      small
      layout="total, prev, pager, next"
      class="pagination"
      :hide-on-single-page="true"
      :page-size="pageSize"
      :current-page.sync="currentPage"
      :total="total"
      @current-change="getReceipt"
    >
    </el-pagination>
  </div>
</template>

<script>
  /**
 * state状态：
1账单未生成，显示预计费用
2账单生成待付款
3账单支付失败
 */
  import moment from "moment";
  import { getReceipt, getReceiptUrl } from "api/billing";
  import { OFFSETUTCTIME } from "@/const";
  import { download } from "@/utils";
  const endTimeSuffix = 86399999;
  export default {
    data() {
      return {
        date: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        receiptData: [],
        requestIng: false,
      };
    },
    filters: {
      handlePrice(value) {
        if (!value) return 0.0;
        return Number(value).toFixed(2);
      },
      handleDate(value) {
        if (!value) return "";
        return moment.utc(Number(value)).format("YYYY-MM-DD") || "";
      },
    },
    computed: {
      overview() {
        return this.$store.state.billing.overview;
      },
      paymentFailedTip() {
        return this.$t("billing.paymentFailedTip").replace(/\{(.+)\}/g, `<a @click='updateCard'>$1</a>`);
      },
    },
    created() {
      this.getReceipt();
    },
    methods: {
      makingInvoice() {},
      setPaymentMethod() {},
      tipClick(e) {
        if (e.target?.nodeName == "A") {
          this.$store.state.billing.creditDialog = true;
        }
      },
      getReceipt() {
        if (this.requestIng) return;
        this.requestIng = true;
        getReceipt({
          currentPage: this.currentPage,
          pageSize: this.pageSize,
          startTime: this.date[0] ? this.date[0] - OFFSETUTCTIME : null,
          endTime: this.date[1] ? this.date[1] - OFFSETUTCTIME + endTimeSuffix : null,
        })
          .then(({ content, total }) => {
            this.receiptData = content;
            this.total = total;
          })
          .catch(() => {
            this.receiptData = [];
            this.total = 0;
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      handleChange(val) {
        if (!val) {
          this.date = [];
        }
        this.currentPage = 1;
        this.getReceipt();
      },
      getReceiptUrl(data) {
        getReceiptUrl({ accountBillId: data.accountBillId }).then(url => {
          download(url);
        });
      },
    },
  };
</script>

<style scoped lang="scss">
  .add-block {
    margin: 10px 0;
    text-align: right;
  }
  .num {
    font-size: 20px;
    font-weight: bold;
    line-height: 30px;
  }
  .payment-card {
    display: flex;
    flex-wrap: wrap;
    margin-bottom: 20px;
  }
  .custom-card {
    // margin-top: 20px;
    height: 100%;
    margin: 20px 20px 0 0;
    flex: 1 0 content;
    &::v-deep .el-card__body {
      display: flex;
    }
  }
  .title {
    font-size: 18px;
    margin-bottom: 10px;
    font-weight: 500;
  }
  .card-item {
    padding-right: 20px;
    // display: inline-block;
    min-width: 180px;
  }
  .w5 {
    width: 50%;
  }
  .w10 {
    width: 100%;
  }
</style>
