<template>
  <div>
    <el-upload class="upload-demo" drag action="https://jsonplaceholder.typicode.com/posts/" multiple>
      <i class="el-icon-upload"></i>
      <div class="el-upload__text" v-html="$t('dataIn.uploadText')"></div>
      <div class="el-upload__tip" slot="tip" v-html="$t('dataIn.uploadTip')"></div>
    </el-upload>
    <section>
      <h1 class="file-name">customer_data.csv</h1>
      <p class="file-info">
        {{ fileInfoText }}
      </p>
      <p class="compress-info">
        {{ $t("for") }}
        <el-dropdown>
          <span class="el-dropdown-link"> 下拉菜单<i class="el-icon-arrow-down el-icon--right"></i> </span>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item>黄金糕</el-dropdown-item>
            <el-dropdown-item>狮子头</el-dropdown-item>
            <el-dropdown-item>螺蛳粉</el-dropdown-item>
            <el-dropdown-item disabled>双皮奶</el-dropdown-item>
            <el-dropdown-item divided>蚵仔煎</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <span v-html="compressFileInfoText"></span>
      </p>
      <h1 class="plan-title">{{ planTitle }}</h1>
      <el-table :data="fileData">
        <el-table-column :label="$t('calculator.scenario')">
          <template slot-scope="{ $index }"> #{{ $index + 1 }} </template>
        </el-table-column>
        <el-table-column :label="$t('calculator.perDayDevice')">
          <template slot-scope="">
            <el-input-number controls-position="right" size="small"></el-input-number>
          </template>
        </el-table-column>
        <el-table-column :label="$t('calculator.queryPerDay')">
          <template slot-scope="">
            <el-input-number controls-position="right" size="small"></el-input-number>
            <span class="every-time">{{ concatTime() }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('calculator.priceLastMonth')">
          <template slot-scope=""> $ 0.0000 </template>
        </el-table-column>
      </el-table>
    </section>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        plan: "Stanard",
        fileData: new Array(5).fill({
          scenario: "",
          perDayDevice: "",
          queryPerDay: "",
          priceLastMonth: "",
        }),
      };
    },
    computed: {
      fileInfoText() {
        return this.$t("calculator.fileInfo");
      },
      compressFileInfoText() {
        return this.$t("calculator.fileCompressResult");
      },
      planTitle() {
        return this.plan + " " + this.$t("calculator.planPricing");
      },
    },
    methods: {
      concatTime() {
        return this.$t("calculator.every");
      },
    },
  };
</script>

<style lang="scss" scoped>
  .upload-demo {
    margin-top: 20px;
    width: 360px;
  }
  .every-time {
    margin-left: 10px;
  }
  .file-name {
    font-size: 16px;
    line-height: 40px;
  }
  .file-info {
    font-size: 14px;
    line-height: 30px;
  }
  .compress-info {
    font-size: 14px;
    line-height: 30px;
  }
  .plan-title {
    font-size: 16px;
    line-height: 40px;
  }
</style>
