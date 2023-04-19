<template>
  <div class="tutorial">
    <el-card v-loading="loading">
      <section class="tutorial-content">
        <section class="left">
          <img class="image-contain" :src="currentLanding.img" :alt="currentLanding.title" />
        </section>
        <section class="right">
          <h1 class="title">
            {{ currentLanding.title }}
          </h1>
          <article v-html="currentLanding.desc"></article>
        </section>
      </section>
      <section class="operate-btn">
        <div class="left">
          <a :href="$t('docsUrl')" target="_blank">{{ $t("document") }}</a>
        </div>
        <div class="right">
          <el-button size="small" v-show="step > 0" @click="step--" plan>{{ $t("prev") }}</el-button>
          <el-button @click="step++" v-show="step < landing.length - 1" type="primary" size="small">{{ $t("next") }}</el-button>
        </div>
      </section>
    </el-card>
  </div>
</template>

<script>
  import { loadImage } from "@/utils/load";
  export default {
    data() {
      this.landing = [
        {
          title: this.$t("landing.metricTitle"),
          desc: this.$t("landing.metricDesc"),
          img: "/static/landing/metric.jpg",
        },
        {
          title: this.$t("landing.labelTitle"),
          desc: this.$t("landing.labelDesc"),
          img: "/static/landing/label.jpg",
        },
        {
          title: this.$t("landing.dataCollectionTitle"),
          desc: this.$t("landing.dataCollectionDesc"),
          img: "/static/landing/dcp.jpg",
        },
        {
          title: this.$t("landing.tableTitle"),
          desc: this.$t("landing.tableDesc"),
          img: "/static/landing/sample.png",
        },
        {
          title: this.$t("landing.superTableTitle"),
          desc: this.$t("landing.superTableDesc"),
          img: "/static/landing/stable.jpg",
        },
        {
          title: this.$t("landing.subtableTitle"),
          desc: this.$t("landing.subtableDesc"),
          img: "/static/landing/subtable.jpg",
        },
        {
          title: this.$t("landing.databaseTitle"),
          desc: this.$t("landing.databaseDesc"),
          img: "/static/landing/database.png",
        },
        {
          title: this.$t("landing.instanceTitle"),
          desc: this.$t("landing.instanceDesc"),
          img: "/static/landing/instances.png",
        },
      ];
      return {
        step: 0,
        loading: false,
      };
    },
    computed: {
      currentLanding() {
        return this.landing[this.step];
      },
    },
    created() {
      this.loadImage();
    },
    methods: {
      loadImage() {
        this.loading = true;
        Promise.all(this.landing.map(item => loadImage(item.img))).then(() => {
          this.loading = false;
        });
      },
    },
  };
</script>

<style lang="scss" scoped>
  .tutorial-content {
    display: flex;
    justify-content: space-between;
    .left {
      width: 53%;
    }
    .right {
      width: 45%;
      .title {
        font-size: 30px;
      }
      article {
        margin-top: 20px;
        font-size: 16px;
      }
    }
  }
  .operate-btn {
    margin-top: 20px;
    display: flex;
    justify-content: space-between;
    &:deep(.el-button) {
      min-width: 60px;
    }
  }
</style>
