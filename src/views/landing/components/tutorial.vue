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
  import { createLanding } from './utils';

  export default {
    data() {
      return {
        step: 0,
        loading: false,
        landing: createLanding()
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
    watch:{
      "$i18n.locale":{
        handler(val){
          this.landing = createLanding()
        }
      }
    }
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
