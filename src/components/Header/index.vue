<template>
  <div class="header-container">
    <div class="inside-header">
      <div class="site-logo">
        <a href="#!" title="TD Hero" rel="home">
          <img
            alt="TD Hero"
            src="../../assets/site-logo.webp"
            title="TD Hero"
            width="200"
          />
        </a>
      </div>
      <div class="inside-navigation">
        <nav class="main-navigation">
          <div id="primary-menu" class="mainNav">
            <ul id="menu-menu">
              <li class="gitIframe">
                <a href="https://github.com/taosdata/TDengine"
                  ><iframe
                    src="https://tdengine.com/star.html?user=taosdata&amp;repo=TDengine&amp;type=star&amp;count=true"
                    frameBorder="0"
                    scrolling="0"
                    width="130"
                    height="32"
                    title="GitHub"
                  ></iframe
                ></a>
              </li>
              <li>
                <a href="https://docs.tdengine.com/get-started/">Get Started</a>
              </li>
              <li><a href="https://tdengine.com/products">Products</a></li>
              <li>
                <a href="https://docs.tdengine.com/">Documentation</a>
              </li>
              <li><a href="https://tdengine.com/blog">Blog</a></li>
              <!-- <li><a href="https://www.taosdata.com/media">Support</a></li> -->
              <li><a href="https://tdengine.com/support">Support</a></li>
              <!-- <li><a href="#!" @click="search">Search</a></li> -->
              <!--  <li class="sub-select"><a href="#!">
                    <svg t="1641792212584" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="2035" width="32" height="32">
                        <path d="M960 512c0-246.4-201.6-448-448-448-80 0-156.8 22.4-220.8 57.6-48 25.6-86.4 60.8-112 92.8A441.6 441.6 0 0 0 64 512c0 246.4 201.6 448 448 448 19.2 0 38.4-3.2 57.6-3.2h6.4c67.2-3.2 118.4-28.8 160-57.6 134.4-76.8 224-220.8 224-387.2z m-64 0c0 92.8-32 179.2-89.6 246.4-28.8-16-115.2-54.4-131.2-64-16-6.4-41.6-3.2-70.4 12.8-3.2-3.2-3.2-6.4-6.4-9.6l-105.6-102.4c0-9.6 0-22.4 6.4-25.6 9.6 0 35.2 19.2 51.2 41.6 6.4 9.6 19.2 12.8 28.8 9.6 12.8-3.2 19.2-9.6 25.6-19.2 3.2-6.4 25.6-76.8-9.6-115.2l-3.2-3.2c3.2-16 60.8-48 89.6-64 54.4-32 99.2-57.6 105.6-96 3.2-16 6.4-38.4-105.6-118.4-12.8-9.6-32-6.4-41.6 3.2L566.4 288l-48-32 64-35.2c12.8-6.4 19.2-19.2 16-32-3.2-12.8-12.8-22.4-25.6-25.6l-118.4-25.6c16-6.4 38.4-9.6 57.6-9.6 211.2 0 384 172.8 384 384z m-268.8 364.8c9.6-41.6 6.4-83.2 0-108.8 9.6-6.4 22.4-12.8 28.8-12.8 16 6.4 60.8 28.8 96 44.8-12.8 16-32 32-57.6 48-22.4 12.8-44.8 22.4-67.2 28.8z m-195.2-265.6c-6.4-9.6-9.6-16-16-25.6-44.8-70.4-76.8-115.2-112-121.6h-9.6c-9.6 0-16 3.2-25.6 6.4-19.2-19.2-48-96-67.2-179.2 6.4-9.6 19.2-28.8 35.2-48l3.2-3.2c0-3.2 3.2-3.2 6.4-6.4 19.2-16 41.6-35.2 67.2-51.2 16-9.6 35.2-16 54.4-22.4l54.4 32c3.2 0 6.4 3.2 9.6 3.2l44.8 9.6-35.2 19.2c-9.6 6.4-16 16-16 25.6s6.4 22.4 12.8 28.8l115.2 73.6c12.8 9.6 28.8 6.4 41.6-6.4l73.6-76.8c22.4 16 41.6 35.2 51.2 44.8-12.8 12.8-48 32-70.4 44.8-57.6 32-112 64-118.4 112-3.2 12.8 0 28.8 9.6 44.8-22.4-12.8-48-19.2-70.4-3.2-38.4 25.6-41.6 70.4-38.4 99.2zM128 512c0-51.2 9.6-99.2 28.8-140.8 19.2 67.2 51.2 144 92.8 160 16 6.4 32 3.2 44.8-3.2 16 12.8 51.2 64 67.2 92.8 38.4 57.6 57.6 92.8 89.6 92.8 12.8 3.2 67.2 19.2 112 35.2v16s19.2 76.8-6.4 128c-16 3.2-28.8 3.2-44.8 3.2-211.2 0-384-172.8-384-384z" p-id="2036" fill="#ffffff"></path>
                        </svg> 
                    </a>
                    <ul class="sub-menu">
                        <li class="languagecn"><a href="#!">中文</a></li>
                        <li class="languageen"><a href="#!" aria-current="page">English</a></li>
                    </ul>
                </li> -->
            </ul>
          </div>
        </nav>
      </div>
    </div>
    <!-- <PopSearch :hidden.sync="hidden"></PopSearch> -->
  </div>
</template>

<script>
import PopSearch from "./components/pop.vue"
export default {
  components: {
    PopSearch
  },
  data() {
    return {
      hiddenSubMenu: true,
      hidden: false
    };
  },
  mounted() {},
  methods: {
    menuToggle(){
        this.hiddenSubMenu = !this.hiddenSubMenu
    },
    search(){
       this.hidden = true
    }
  },
};
</script>

<style scoped>
.header-container {
  width: 100%;
  height: 100px;
  position: relative;
  background-image: url("../../assets/banner-bg.webp");
  background-repeat: no-repeat;
  background-position: center center;
  background-size: cover;
  font-family: "Open Sans", Helvetica, "Hiragino Sans GB", sans-serif,
    "Apple Color Emoji";
}

.inside-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  max-width: 1240px;
  padding: 20px;
  margin-left: auto;
  margin-right: auto;
}

.site-logo {
  width: 200px;
}

.gridContainer {
  margin-left: auto;
  margin-right: auto;
}

.inside-navigation {
  display: flex;
  flex-wrap: wrap;
}

.mobile-menucontrol-wrapper {
  display: none;
}

.main-navigation {
  margin-top: 5px;
}

.main-navigation .mainNav > ul {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  list-style: none;
  margin: 0;
  padding-left: 0;
  height: 60px;
  line-height: 60px;
}

.gitIframe {
  margin-top: 8px;
}

.main-navigation .mainNav > ul > li a {
  font-size: 17.6px !important;
  font-weight: 300;
  padding-left: 10px;
  padding-right: 10px;
  letter-spacing: 0px;
  color: #ffffff;
}

.sub-select {
  position: relative;
}
</style>
