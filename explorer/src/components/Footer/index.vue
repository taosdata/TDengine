<template>
  <div>
    <div class="footer">
      <div class="container">
        <div class="inside">
          <div class="top">
            <div class="left">
              <figure class="logo">
                <img
                  src="../../assets/site-logo.webp"
                  class="logoImg"
                  width="100"
                  alt=" Database"
                />
              </figure>
              <div class="tdengine">
                <p>
                  TDengine is an open source time-series database with high
                  performance, scalability and SQL support. In addition, it has
                  built-in caching, stream processing, data subscription and
                  other functions to reduce the complexity and costs of
                  development and operations.
                </p>
              </div>
            </div>
            <div class="right">
              <div class="sales">
                <span class="button" @click="contactUs">Contact us</span>
              </div>
              <div class="social">
                <a
                  class="social-btn"
                  href="https://discord.gg/VZdSuUg4pS?utm_source=website&amp;utm_medium=widget&amp;utm_campaign=channel&amp;utm_id=discord"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="twitter link"
                >
                  <span class="icon">
                    <svg
                      alt=" Database"
                      role="img"
                      height="20"
                      width="20"
                      xmlns="http://www.w3.org/2000/svg"
                      version="1.1"
                      viewBox="0 0 1280 1024"
                    >
                      <path
                        fill="#ffffff"
                        d="M1049.062 139.672a3 3 0 0 0-1.528-1.4A970.13 970.13 0 0 0 808.162 64.06a3.632 3.632 0 0 0-3.846 1.82 674.922 674.922 0 0 0-29.8 61.2 895.696 895.696 0 0 0-268.852 0 619.082 619.082 0 0 0-30.27-61.2 3.78 3.78 0 0 0-3.848-1.82 967.378 967.378 0 0 0-239.376 74.214 3.424 3.424 0 0 0-1.576 1.352C78.136 367.302 36.372 589.38 56.86 808.708a4.032 4.032 0 0 0 1.53 2.75 975.332 975.332 0 0 0 293.65 148.378 3.8 3.8 0 0 0 4.126-1.352A696.4 696.4 0 0 0 416.24 860.8a3.72 3.72 0 0 0-2.038-5.176 642.346 642.346 0 0 1-91.736-43.706 3.77 3.77 0 0 1-0.37-6.252 502.094 502.094 0 0 0 18.218-14.274 3.638 3.638 0 0 1 3.8-0.512c192.458 87.834 400.82 87.834 591 0a3.624 3.624 0 0 1 3.848 0.466 469.066 469.066 0 0 0 18.264 14.32 3.768 3.768 0 0 1-0.324 6.252 602.814 602.814 0 0 1-91.78 43.66 3.75 3.75 0 0 0-2 5.222 782.11 782.11 0 0 0 60.028 97.63 3.728 3.728 0 0 0 4.126 1.4A972.096 972.096 0 0 0 1221.4 811.458a3.764 3.764 0 0 0 1.53-2.704c24.528-253.566-41.064-473.824-173.868-669.082zM444.982 675.16c-57.944 0-105.688-53.174-105.688-118.478s46.818-118.482 105.688-118.482c59.33 0 106.612 53.64 105.686 118.478 0 65.308-46.82 118.482-105.686 118.482z m390.76 0c-57.942 0-105.686-53.174-105.686-118.478s46.818-118.482 105.686-118.482c59.334 0 106.614 53.64 105.688 118.478 0 65.308-46.354 118.482-105.688 118.482z"
                      ></path>
                    </svg>
                  </span>
                </a>
                <a
                  class="social-btn"
                  href="https://twitter.com/TaosData??utm_source=website&amp;utm_medium=widget&amp;utm_campaign=channel&amp;utm_id=twitter"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="facebook link"
                >
                  <span class="icon">
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      viewBox="0 0 512 512"
                      width="20"
                      height="20"
                      aria-hidden="true"
                      alt=" Database"
                      role="img"
                    >
                      <path
                        d="M459.37 151.716c.325 4.548.325 9.097.325 13.645 0 138.72-105.583 298.558-298.558 298.558-59.452 0-114.68-17.219-161.137-47.106 8.447.974 16.568 1.299 25.34 1.299 49.055 0 94.213-16.568 130.274-44.832-46.132-.975-84.792-31.188-98.112-72.772 6.498.974 12.995 1.624 19.818 1.624 9.421 0 18.843-1.3 27.614-3.573-48.081-9.747-84.143-51.98-84.143-102.985v-1.299c13.969 7.797 30.214 12.67 47.431 13.319-28.264-18.843-46.781-51.005-46.781-87.391 0-19.492 5.197-37.36 14.294-52.954 51.655 63.675 129.3 105.258 216.365 109.807-1.624-7.797-2.599-15.918-2.599-24.04 0-57.828 46.782-104.934 104.934-104.934 30.213 0 57.502 12.67 76.67 33.137 23.715-4.548 46.456-13.32 66.599-25.34-7.798 24.366-24.366 44.833-46.132 57.827 21.117-2.273 41.584-8.122 60.426-16.243-14.292 20.791-32.161 39.308-52.628 54.253z"
                        fill="currentColor"
                      ></path>
                    </svg>
                  </span>
                </a>
                <a
                  class="social-btn"
                  href="https://github.com/taosdata/TDengine"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="facebook link"
                >
                  <span class="icon">
                    <svg
                      alt=" Database"
                      role="img"
                      viewBox="0 0 512 512"
                      height="20"
                      width="20"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        d="M256 32C132.3 32 32 134.9 32 261.7c0 101.5 64.2 187.5 153.2 217.9 1.4.3 2.6.4 3.8.4 8.3 0 11.5-6.1 11.5-11.4 0-5.5-.2-19.9-.3-39.1-8.4 1.9-15.9 2.7-22.6 2.7-43.1 0-52.9-33.5-52.9-33.5-10.2-26.5-24.9-33.6-24.9-33.6-19.5-13.7-.1-14.1 1.4-14.1h.1c22.5 2 34.3 23.8 34.3 23.8 11.2 19.6 26.2 25.1 39.6 25.1 10.5 0 20-3.4 25.6-6 2-14.8 7.8-24.9 14.2-30.7-49.7-5.8-102-25.5-102-113.5 0-25.1 8.7-45.6 23-61.6-2.3-5.8-10-29.2 2.2-60.8 0 0 1.6-.5 5-.5 8.1 0 26.4 3.1 56.6 24.1 17.9-5.1 37-7.6 56.1-7.7 19 .1 38.2 2.6 56.1 7.7 30.2-21 48.5-24.1 56.6-24.1 3.4 0 5 .5 5 .5 12.2 31.6 4.5 55 2.2 60.8 14.3 16.1 23 36.6 23 61.6 0 88.2-52.4 107.6-102.3 113.3 8 7.1 15.2 21.1 15.2 42.5 0 30.7-.3 55.5-.3 63 0 5.4 3.1 11.5 11.4 11.5 1.2 0 2.6-.1 4-.4C415.9 449.2 480 363.1 480 261.7 480 134.9 379.7 32 256 32z"
                      ></path>
                    </svg>
                  </span>
                </a>
                <a
                  class="social-btn"
                  href="https://www.linkedin.com/company/tdengine"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="twitter link"
                  ><span class="icon"
                    ><svg
                      aria-hidden="true"
                      alt=" Database"
                      role="img"
                      height="20"
                      width="20"
                      viewBox="0 0 448 512"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        fill="#ffffff"
                        d="M100.28 448H7.4V148.9h92.88zM53.79 108.1C24.09 108.1 0 83.5 0 53.8a53.79 53.79 0 0 1 107.58 0c0 29.7-24.1 54.3-53.79 54.3zM447.9 448h-92.68V302.4c0-34.7-.7-79.2-48.29-79.2-48.29 0-55.69 37.7-55.69 76.7V448h-92.78V148.9h89.08v40.8h1.3c12.4-23.5 42.69-48.3 87.88-48.3 94 0 111.28 61.9 111.28 142.3V448z"
                      ></path></svg></span
                ></a>
                <a
                  class="social-btn"
                  href="https://stackoverflow.com/questions/tagged/td-engine"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="twitter link"
                  ><span class="icon"
                    ><svg
                      height="20"
                      width="20"
                      alt=" Database"
                      role="img"
                      xmlns="http://www.w3.org/2000/svg"
                      version="1.1"
                      viewBox="0 0 1024 1024"
                    >
                      <path
                        fill="#ffffff"
                        d="M1024 640v384H0V640h128v256h768v-256zM192 704h640v128H192z m15.136-138.528l27.712-124.96 624.832 138.496-27.712 124.96z m72.512-256.928l54.08-116 580.032 270.464-54.08 116z m712.064 52.928l-77.92 101.536L406.048 73.408 462.4 0h58.24z"
                      ></path></svg></span
                ></a>
              </div>
            </div>
          </div>
          <div class="bottom">
            <div class="bottomLeft">
              <!-- <p>
                © <span>2022</span> 涛思数据 | 京公网安备 11010502047618号 |
                京ICP备17069529号-1
              </p> -->
              © <span>2022</span> TDengine
            </div>
            <div class="bottomRight">
              <a class="bottomLink" href="https://tdengine.com/careers"
                >Careers</a
              >
              <a class="bottomLink" href="https://tdengine.com/terms-of-service"
                >Terms</a
              >
              <a class="bottomLink" href="https://tdengine.com/privacy"
                >Privacy</a
              >
              <a class="bottomLinkLast" href="https://tdengine.com//about"
                >About</a
              >
            </div>
          </div>
        </div>
      </div>
    </div>
    <Popup :hidden.sync="isShowPopup" :form="form" v-if="isShowPopup"></Popup>
  </div>
</template>
<script>
import Popup from "./components/popup.vue";
export default {
  components: {
    Popup,
  },
  data() {
    return {
      isShowPopup: false,
      form: {},
    };
  },
  mounted() {},
  watch: {},
  methods: {
    subScript() {
      window.open("https://www.taosdata.com/news-letter");
    },
    contactUs() {
      let data = {
        title: "Contact us",
        src: "contactSales",
      };
      this.isShowPopup = true;
      this.form = data;
    },
  },
};
</script>
<style scoped >
.footer {
  width: 100%;
  display: block;
  line-height: 1.5;
  font-size: 14px;
  font-weight: 400;
  color: white;
  font-family: "Open Sans", Helvetica, "Hiragino Sans GB", sans-serif,
    "Apple Color Emoji";
}
.container {
  margin-top: 0;
  position: relative;
}

.container::before {
  content: "";
  background-image: url("../../assets/banner-bg.webp");
  background-repeat: no-repeat;
  background-position: center center;
  background-size: cover;
  z-index: 0;
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  transition: inherit;
}

.inside {
  padding: 30px 20px 0;
  max-width: 1240px;
  margin-left: auto;
  margin-right: auto;
  z-index: 1;
  position: relative;
  margin-top: 10px;
}

.top {
  display: flex;
  align-items: flex-end;
  flex-wrap: wrap;
  margin-top: 10px;
  position: relative;
}

.left {
  padding-left: 0;
  width: 50%;
  text-align: left;
}



.right {
  padding-left: 0;
  width: 50%;
  margin-top: 10px;
}


.bottom {
  margin-top: 10px;
  border-top-style: solid;
  border-top-width: 1px;
  border-color: white;
  padding-top: 1.5em;
  padding-bottom: 2.25em;
  margin-left: auto;
  margin-right: auto;
  display: flex;
  flex-wrap: wrap;
}

.bottomLeft {
  width: 75%;
  text-align: left;
}

.bottomRight {
  width: 25%;
  display: flex;
  justify-content: flex-end;
  flex-wrap: wrap;
}


.bottomRight a {
  color: white;
  padding-right: 10px;
  text-decoration: none;
}

.bottomRight a.bottomLinkLast {
  padding-right: 0;
}

.logo {
  margin-bottom: 0;
  max-width: 100%;
  height: auto;
  margin: 0 0 1em;
  padding: 0;
  border: 0;
  margin-bottom: 0;
}

.tdengine {
  margin-top: 20px;
}

.logoImg {
  vertical-align: middle;
  margin-top: 10px;
}

.footerLogoLink {
  opacity: 0.5;
  transition: opacity var(--ifm-transition-fast)
    var(--ifm-transition-timing-default);
}

.footerLogoLink:hover {
  opacity: 1;
}

.sales {
  display: flex;
  justify-content: flex-end;
}


.button {
  background-color: white;
  color: #1860f0;
  font-size: 15px;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 10px;
  margin-bottom: 10px;
  cursor: pointer;
}

.button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  text-decoration: none;
  transition: 0.2s background-color ease-in-out, 0.2s color ease-in-out,
    0.2s border-color ease-in-out, 0.2s opacity ease-in-out,
    0.2s box-shadow ease-in-out;
}

.social {
  text-align: right;
  margin-bottom: 1.5em;
  margin-top: 10px;
  padding: 5px 0;
  line-height: 1.5;
  font-size: 18px;
  display: flex;
  justify-content: flex-end;
  /* flex-wrap: wrap; */
  align-items: flex-start;
  clear: both;
}

.social-btn {
  padding: 6px;
  border-radius: 50px;
  margin-left: 20px;
  border-style: solid;
  border-width: 2px;
  color: white;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  transition: 0.2s background-color ease-in-out, 0.2s color ease-in-out,
    0.2s border-color ease-in-out, 0.2s opacity ease-in-out,
    0.2s box-shadow ease-in-out;
}

.icon {
  align-items: center;
  font-size: 0.8em;
  cursor: pointer;
  display: inline-flex;
  text-align: center;
  line-height: 0;
}

.icon svg {
  height: 20px;
  width: 20px;
  fill: currentColor;
}

span.wechat {
  position: relative;
}
span.wechat:hover .wechatSocialImg {
  display: block;
}

.wechatSocialImg {
  left: -100px;
  padding-top: 10px;
  position: absolute;
  bottom: 100%;
  z-index: 1;
  display: none;
  padding-bottom: 10px;
}

.indexModuleTooltipInner {
  align-items: center;
  background: #fff;
  border-radius: 8px;
  border: 1px solid #2d7df0;
  color: #000;
  display: flex;
  height: 10rem;
  padding: 0.5rem;
  width: 20rem;
}

.indexModuleTooltipInner img {
  height: 9.5rem;
  margin-right: 0.5rem;
  width: 9.5rem;
}

.indexModuleTooltipInner p {
  font-size: 14px;
}

.indexModuleTooltipInner img {
  height: 7.5rem;
  margin-right: 0.5rem;
  width: 7.5rem;
}

.subScript {
  background-color: #1860f0;
  color: #ffffff;
  font-size: 15px;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 10px;
  margin-right: 12px;
  margin-bottom: 10px;
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  text-align: center;
}
.subScript span:first-child {
  padding: 2px 4px 0px 0px;
}
#menuShow {
  position: fixed;
  background-color: var(--ifm-color-emphasis-200);
  z-index: 400;
  width: 55px;
  height: 30px;
  bottom: 80px;
  right: 20px;
  text-align: center;
  display: none;
  border-radius: 8px;
  cursor: pointer;
  color: var(--ifm-color-emphasis-1000);
  font-size: 18px;
}
#menuShow:hover {
  background-color: var(--ifm-color-emphasis-300);
}
</style>
