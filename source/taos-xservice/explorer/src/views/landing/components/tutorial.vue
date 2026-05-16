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
          <article v-dompurify-html="currentLanding.desc"></article>
        </section>
      </section>
      <section class="operate-btn">
        <div class="left">
          <a :href="$t('docsUrl')" target="_blank">{{ $t('document') }}</a>
        </div>
        <div class="right">
          <el-button v-show="step > 0" size="default" plan @click="step--">{{ $t('prev') }}</el-button>
          <el-button v-show="step < landing.length - 1" type="primary" size="default" @click="step++">{{
            $t('next')
          }}</el-button>
        </div>
      </section>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { loadImage } from '@/utils/load';
import { t } from '@/lang';
const landing = computed(() => {
  return [
    {
      title: t('landing.metricTitle'),
      desc: t('landing.metricDesc'),
      img: '/static/landing/metric.jpg'
    },
    {
      title: t('landing.labelTitle'),
      desc: t('landing.labelDesc'),
      img: '/static/landing/label.jpg'
    },
    {
      title: t('landing.dataCollectionTitle'),
      desc: t('landing.dataCollectionDesc'),
      img: '/static/landing/dcp.jpg'
    },
    {
      title: t('landing.tableTitle'),
      desc: t('landing.tableDesc'),
      img: '/static/landing/sample.png'
    },
    {
      title: t('landing.superTableTitle'),
      desc: t('landing.superTableDesc'),
      img: '/static/landing/stable.jpg'
    },
    {
      title: t('landing.subtableTitle'),
      desc: t('landing.subtableDesc'),
      img: '/static/landing/subtable.jpg'
    },
    {
      title: t('landing.databaseTitle'),
      desc: t('landing.databaseDesc'),
      img: '/static/landing/database.png'
    }
  ];
});

const step = ref(0);
const loading = ref<boolean>(false);

const currentLanding = computed(() => {
  return landing.value[step.value];
});

function loadImageData() {
  loading.value = true;
  Promise.all(landing.value.map(item => loadImage(item.img))).then(() => {
    loading.value = false;
  });
}
loadImageData();
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
  display: flex;
  justify-content: space-between;
  margin-top: 20px;

  &:deep(.el-button) {
    min-width: 60px;
  }
}
</style>
