<template>
  <div class="view">
    <section class="view-header">
      <section class="left">
        <img class="image-contains" :src="getImg(config.name, config.icon)" alt="" />
      </section>
      <section class="right">
        <el-steps align-center :active="activeTab" finish-status="success">
          <el-step @click.native="handleClickStep(index)" v-for="(item, index) in steps" :key="item.title" :title="item.title"></el-step>
        </el-steps>
      </section>
    </section>
    <section id="view-content" class="markdown-body">
      <component :url="url" :token="token" :is="component" :user="username" :password="decryptPwd"></component>
    </section>
  </div>
</template>

<script>
  import * as config from "@/utils/config";
  import { debounce, decrypt } from "@/utils";
  import "github-markdown-css/github-markdown-light.css";
  export default {
    props: {
      lang: {
        type: String,
        default: "",
      },
      category: {
        type: String,
        default: "",
      },
    },
    computed: {
      language() {
        // return this.$store.state.app.userInfo.language || "en";
        return "en";
      },
      config() {
        let lang = window.decodeURIComponent(this.lang);
        return (
          config[this.category].find(item => {
            return item.name == lang;
          }) || {}
        );
      },
      component() {
        return typeof this.config.docs?.[this.language] == "string" || !this.config.docs?.[this.language] ? "" : this.config.docs?.[this.language];
      },
      steps() {
        return this.config.steps || [];
      },
      url() {
        // return this.$store.state.app.current_cluster.urlPath;
        // return this.$store.state.app.current_cluster.gateway_url;
        return localStorage.getItem('base_url')
      },
      token() {
        // return this.$store.state.app.current_cluster?.token?.token || "";
        return localStorage.getItem('TDengine-Token')?localStorage.getItem('TDengine-Token'):''
      },
      username() {
        return localStorage.getItem("username")
          ? localStorage.getItem("username")
          : "";
      },
      decryptPwd() {
        return decrypt(localStorage.getItem("pwd")) || '';
      }
    },
    data() {
      return {
        activeTab: 1,
        mdContent: "",
        domList: [],
        topList: [],
        element: null,
      };
    },
    watch: {
      language() {
        //语言切换刷新页面
        window.location.reload();
      },
    },
    mounted() {
      // 在这里保存元素
      this.$nextTick(() => {
        this.domList = this.steps.map(item => document.getElementById(item.dom));
      });
      let fn = debounce(e => {
        let top = e.target.scrollTop;
        let currentTop = Math.ceil(top + elementHeight);

        this.activeTab =
          this.getOffsetTop().findLastIndex(item => {
            return (item.start <= top && item.end >= currentTop) || (item.start <= currentTop && item.end >= currentTop);
          }) + 1;
        this.activeTab = this.activeTab || 1;
      }, 100);
      this.element = document.querySelector(".main_content");
      const elementHeight = parseFloat(document?.defaultView?.getComputedStyle(this.element).height || this.element.offsetHeight);
      this.element.addEventListener("scroll", fn);
      this.$once("hook:beforeDestroy", () => {
        this.element.removeEventListener("scroll", fn);
      });
      // 处理a标签，添加属性target="_blank"
      let aList = document.querySelectorAll("#view-content a");
      aList.forEach(item => {
        item.setAttribute("target", "_blank");
      });
    },
    methods: {
      handleClickStep(index) {
        let dom = this.domList[index];
        if (dom) {
          this.scrollTo(dom);
        }
        this.activeTab = index + 1;
      },
      getOffsetTop() {
        let topList = [];
        this.domList.forEach((dom, index) => {
          topList.push({
            start: dom?.offsetTop,
          });
          if (index > 0 && index < this.steps.length - 1) {
            topList[index - 1].end = topList[index].start;
          }
          if (index == this.steps.length - 1) {
            topList[index - 1].end = topList[index].start;
            topList[index].end = this.element.scrollHeight;
          }
        });
        return topList;
      },
      getImg(name, icon) {
        if(name=='REST API'){
          name='restapi'
        }
        if(name=='TDengine CLI'){
          name='tdenginecli'
        }
        if(name=='Google Data Studio'){
          name='gdStudio'
        }
        try {
          return require(`@/assets/images/${icon || name}.svg`);
        } catch (err) {
          return require(`@/assets/logo.svg`);
        }
      },
      scrollTo(dom) {
        this.element.scrollTo({
          top: dom.offsetTop,
          behavior: "smooth",
        });
      },
    },
  };
</script>
<style lang="scss" scoped>
  .view {
    position: relative;
    background-color: #fff;
    @include content-padding;
  }
  .view-header {
    position: sticky;
    top: -20px;
    display: flex;
    padding-bottom: 10px;
    padding-top: 10px;
    background-color: #fff;
    z-index: 6;
  }
  .view::v-deep .markdown-body .highlight pre,
  .view::v-deep .markdown-body pre {
    position: relative;
  }
  .view::v-deep .token-select {
    display: flex;
    align-items: center;
    margin-bottom: 20px;
    .label {
      font-size: 18px;
      margin-right: 20px;
      font-family: Amazon Ember, Helvetica Neue, Roboto, Arial, sans-serif;
    }
  }
  .token-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    font-size: 14px;
    font-weight: 600;
  }
  .view ::v-deep .el-empty {
    padding: 0;
  }
  #view-content {
    margin-top: 10px;
  }
  .left {
    width: auto;
    display: inline-block;
    flex-shrink: 0;
    height: 81px;
    flex-shrink: 0;
    .image-contains {
      height: 100%;
      object-fit: contain;
    }
  }
  .right {
    flex: 1;
    margin-left: -40px;
    overflow: hidden;
  }
</style>
