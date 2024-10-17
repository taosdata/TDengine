<template>
  <div class="file">
    <ul v-if="list.length">
      <li v-for="item in list" :key="item.id">
        <p class="nowrap" title="item.name">{{ item.name }}</p>
        <div v-if="item.type == 'image'" class="image">
          <img class="image-contain" v-lazy="item.url" :alt="item.name" />
        </div>
        <div class="image" v-else>
          <Icon class="file-icon" name="file" />
        </div>
        <div class="icon-wrapper">
          <el-icon v-if="item.type == 'image'" class="el-icon-view" title="view" @click.native="view(item.url)"></el-icon
          ><el-icon style="margin-left: 10px" title="download" class="el-icon-download" @click.native="download(item.url, item.name)"></el-icon>
        </div>
      </li>
    </ul>
    <el-empty v-else :image-size="200"></el-empty>
    <el-dialog :visible.sync="dialogTableVisible" :close-on-click-modal="false">
      <img width="100%" :src="url" alt="" />
    </el-dialog>
  </div>
</template>

<script>
import { parseTime, download } from "@/utils";
export default {
  props: {
    list: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      url: "",
      dialogTableVisible: false,
    };
  },
  methods: {
    parseTime(date) {
      return parseTime(date, "YYYY-MM-DD kk:mm:ss");
    },
    view(url) {
      this.url = url;
      this.dialogTableVisible = true;
    },
    download(url, name) {
      download(url, name);
    },
  },
};
</script>

<style lang="scss" scoped>
.file {
  margin-top: 20px;
}
.title {
  font-size: 20px;
  line-height: 30px;
  color: #333;
}
$border-color: #ddd;
ul {
  display: flex;
  li {
    position: relative;
    width: 200px;
    height: 200px;
    margin-right: 20px;
    border: 1px solid #ddd;
    text-align: center;
    display: flex;
    flex-direction: column;
    // align-items: center;
    padding: 10px 20px;
    cursor: pointer;
    border-bottom: 1px solid $border-color;
    &:hover {
      .icon-wrapper {
        display: flex;
      }
    }
  }
  .image {
    flex: 1;
  }
  .date {
    font-size: 12px;
  }
  .content {
    font-size: 16px;
  }
  .icon-wrapper {
    top: 0;
    left: 0;
    @extend .flexCenter;
    position: absolute;
    background-color: rgba(0, 0, 0, 0.2);
    display: none;
    font-size: 26px;
    height: 100%;
    width: 100%;
    color: #fff;
  }
  .file-icon {
    width: 200px;
    height: 160px;
  }
}
</style>
