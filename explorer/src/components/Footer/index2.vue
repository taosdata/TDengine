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
                <!-- <span class="subScript" @click="subScript">
                  <span>
                    <svg
                      aria-hidden="true"
                      alt=" Database"
                      role="img"
                      height="1em"
                      width="1em"
                      viewBox="0 0 512 512"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        fill="currentColor"
                        d="M173.898 439.404l-166.4-166.4c-9.997-9.997-9.997-26.206 0-36.204l36.203-36.204c9.997-9.998 26.207-9.998 36.204 0L192 312.69 432.095 72.596c9.997-9.997 26.207-9.997 36.204 0l36.203 36.204c9.997 9.997 9.997 26.206 0 36.204l-294.4 294.401c-9.998 9.997-26.207 9.997-36.204-.001z"
                      ></path>
                    </svg>
                  </span>
                  <span>订阅</span>
                </span> -->
                <span class="button">Contact us </span>
              </div>
              <div class="social">
                <span class="social-btn wechat">
                  <span class="icon">
                    <svg
                      alt=" Database"
                      role="img"
                      viewBox="0 0 576 512"
                      height="20"
                      width="20"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        d="M385.2 167.6c6.4 0 12.6.3 18.8 1.1C387.4 90.3 303.3 32 207.7 32 100.5 32 13 104.8 13 197.4c0 53.4 29.3 97.5 77.9 131.6l-19.3 58.6 68-34.1c24.4 4.8 43.8 9.7 68.2 9.7 6.2 0 12.1-.3 18.3-.8-4-12.9-6.2-26.6-6.2-40.8-.1-84.9 72.9-154 165.3-154zm-104.5-52.9c14.5 0 24.2 9.7 24.2 24.4 0 14.5-9.7 24.2-24.2 24.2-14.8 0-29.3-9.7-29.3-24.2.1-14.7 14.6-24.4 29.3-24.4zm-136.4 48.6c-14.5 0-29.3-9.7-29.3-24.2 0-14.8 14.8-24.4 29.3-24.4 14.8 0 24.4 9.7 24.4 24.4 0 14.6-9.6 24.2-24.4 24.2zM563 319.4c0-77.9-77.9-141.3-165.4-141.3-92.7 0-165.4 63.4-165.4 141.3S305 460.7 397.6 460.7c19.3 0 38.9-5.1 58.6-9.9l53.4 29.3-14.8-48.6C534 402.1 563 363.2 563 319.4zm-219.1-24.5c-9.7 0-19.3-9.7-19.3-19.6 0-9.7 9.7-19.3 19.3-19.3 14.8 0 24.4 9.7 24.4 19.3 0 10-9.7 19.6-24.4 19.6zm107.1 0c-9.7 0-19.3-9.7-19.3-19.6 0-9.7 9.7-19.3 19.3-19.3 14.5 0 24.4 9.7 24.4 19.3.1 10-9.9 19.6-24.4 19.6z"
                        fill="#ffffff"
                      ></path>
                    </svg>
                  </span>
                  <div class="wechatSocialImg">
                    <div class="indexModuleTooltipInner">
                      <img
                        src="../../assets/tdengineqrcode_1.jpeg"
                        alt=" Database 公众号"
                      />
                      <div>
                        <p>添加公众号：</p>
                        <p>TDengine</p>
                      </div>
                    </div>
                  </div>
                </span>
                <a
                  class="social-btn"
                  href="https://weibo.com/u/6368262736"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="twitter link"
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
                        d="M407 177.6c7.6-24-13.4-46.8-37.4-41.7-22 4.8-28.8-28.1-7.1-32.8 50.1-10.9 92.3 37.1 76.5 84.8-6.8 21.2-38.8 10.8-32-10.3zM214.8 446.7C108.5 446.7 0 395.3 0 310.4c0-44.3 28-95.4 76.3-143.7C176 67 279.5 65.8 249.9 161c-4 13.1 12.3 5.7 12.3 6 79.5-33.6 140.5-16.8 114 51.4-3.7 9.4 1.1 10.9 8.3 13.1 135.7 42.3 34.8 215.2-169.7 215.2zm143.7-146.3c-5.4-55.7-78.5-94-163.4-85.7-84.8 8.6-148.8 60.3-143.4 116s78.5 94 163.4 85.7c84.8-8.6 148.8-60.3 143.4-116zM347.9 35.1c-25.9 5.6-16.8 43.7 8.3 38.3 72.3-15.2 134.8 52.8 111.7 124-7.4 24.2 29.1 37 37.4 12 31.9-99.8-55.1-195.9-157.4-174.3zm-78.5 311c-17.1 38.8-66.8 60-109.1 46.3-40.8-13.1-58-53.4-40.3-89.7 17.7-35.4 63.1-55.4 103.4-45.1 42 10.8 63.1 50.2 46 88.5zm-86.3-30c-12.9-5.4-30 .3-38 12.9-8.3 12.9-4.3 28 8.6 34 13.1 6 30.8.3 39.1-12.9 8-13.1 3.7-28.3-9.7-34zm32.6-13.4c-5.1-1.7-11.4.6-14.3 5.4-2.9 5.1-1.4 10.6 3.7 12.9 5.1 2 11.7-.3 14.6-5.4 2.8-5.2 1.1-10.9-4-12.9z"
                        fill="#ffffff"
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
                  ><span class="icon"
                    ><svg
                      alt=" Database"
                      role="img"
                      viewBox="0 0 512 512"
                      height="20"
                      width="20"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        d="M256 32C132.3 32 32 134.9 32 261.7c0 101.5 64.2 187.5 153.2 217.9 1.4.3 2.6.4 3.8.4 8.3 0 11.5-6.1 11.5-11.4 0-5.5-.2-19.9-.3-39.1-8.4 1.9-15.9 2.7-22.6 2.7-43.1 0-52.9-33.5-52.9-33.5-10.2-26.5-24.9-33.6-24.9-33.6-19.5-13.7-.1-14.1 1.4-14.1h.1c22.5 2 34.3 23.8 34.3 23.8 11.2 19.6 26.2 25.1 39.6 25.1 10.5 0 20-3.4 25.6-6 2-14.8 7.8-24.9 14.2-30.7-49.7-5.8-102-25.5-102-113.5 0-25.1 8.7-45.6 23-61.6-2.3-5.8-10-29.2 2.2-60.8 0 0 1.6-.5 5-.5 8.1 0 26.4 3.1 56.6 24.1 17.9-5.1 37-7.6 56.1-7.7 19 .1 38.2 2.6 56.1 7.7 30.2-21 48.5-24.1 56.6-24.1 3.4 0 5 .5 5 .5 12.2 31.6 4.5 55 2.2 60.8 14.3 16.1 23 36.6 23 61.6 0 88.2-52.4 107.6-102.3 113.3 8 7.1 15.2 21.1 15.2 42.5 0 30.7-.3 55.5-.3 63 0 5.4 3.1 11.5 11.4 11.5 1.2 0 2.6-.1 4-.4C415.9 449.2 480 363.1 480 261.7 480 134.9 379.7 32 256 32z"
                      ></path></svg></span
                ></a>
                <span
                  class="social-btn wechat"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="twitter link"
                >
                  <span class="icon"
                    ><svg
                      alt=" Database"
                      role="img"
                      height="20"
                      width="20"
                      xmlns="http://www.w3.org/2000/svg"
                      version="1.1"
                      viewBox="0 0 1291 1024"
                    >
                      <path
                        fill="#ffffff"
                        d="M781.793876 523.871639a229.106808 229.106808 0 0 0 88.712761-185.157367v-8.138785a230.327626 230.327626 0 0 0-225.444355-225.444355 230.327626 230.327626 0 0 0-225.851294 225.037416 240.908047 240.908047 0 0 0 88.712761 185.157367A348.746953 348.746953 0 0 0 282.479394 854.713264c8.138785 162.775707 153.009165 169.286736 354.444103 169.286736s354.444103 0 362.582888-169.286736c17.905328-145.684258-88.71276-274.684006-217.712509-330.841625z"
                      ></path>
                      <path
                        fill="#ffffff"
                        d="M403.340357 507.594069a290.554638 290.554638 0 0 1-48.425773-162.775708 284.857488 284.857488 0 0 1 81.387853-209.573723 228.29293 228.29293 0 0 1 88.712761-64.296404A203.469634 203.469634 0 0 0 363.053369 0.140801a225.037415 225.037415 0 0 0-225.444355 225.444355 240.908047 240.908047 0 0 0 88.712761 185.157367A356.478799 356.478799 0 0 0 0.063542 749.315994c8.138785 122.08178 96.851546 153.009165 225.444354 169.286736 0-16.277571-8.138785-32.148202-8.138785-48.425773a438.680531 438.680531 0 0 1 185.157367-362.582888z m660.462432-96.851546a229.106808 229.106808 0 0 0 88.712761-185.157367A220.154144 220.154144 0 0 0 926.664256 0.140801a203.469634 203.469634 0 0 0-162.775708 72.43519 304.390573 304.390573 0 0 1 177.018582 265.731342v8.138785A317.00569 317.00569 0 0 1 895.329932 515.732854a410.194782 410.194782 0 0 1 185.157367 362.582888 102.141756 102.141756 0 0 1-8.138785 48.425773c128.999748-8.138785 209.573723-40.693927 217.305569-169.286736a352.002467 352.002467 0 0 0-225.444355-346.305317z"
                      ></path>
                    </svg>
                  </span>
                  <div class="wechatSocialImg">
                    <div class="indexModuleTooltipInner">
                      <img
                        src="../../assets/tdengine.webp"
                        alt=" Database 技术交流群"
                      />
                      <div>
                        <p>加小T为好友，即可加入物联网大数据技术前沿群</p>
                      </div>
                    </div>
                  </div>
                </span>
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
                <a
                  class="social-btn"
                  href="https://www.zhihu.com/people/tbase"
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
                        d="M564.7 230.1V803h60l25.2 71.4L756.3 803h131.5V230.1H564.7z m247.7 497h-59.9l-75.1 50.4-17.8-50.4h-18V308.3h170.7v418.8zM526.1 486.9H393.3c2.1-44.9 4.3-104.3 6.6-172.9h130.9l-0.1-8.1c0-0.6-0.2-14.7-2.3-29.1-2.1-15-6.6-34.9-21-34.9H287.8c4.4-20.6 15.7-69.7 29.4-93.8l6.4-11.2-12.9-0.7c-0.8 0-19.6-0.9-41.4 10.6-35.7 19-51.7 56.4-58.7 84.4-18.4 73.1-44.6 123.9-55.7 145.6-3.3 6.4-5.3 10.2-6.2 12.8-1.8 4.9-0.8 9.8 2.8 13 10.5 9.5 38.2-2.9 38.5-3 0.6-0.3 1.3-0.6 2.2-1 13.9-6.3 55.1-25 69.8-84.5h56.7c0.7 32.2 3.1 138.4 2.9 172.9h-141l-2.1 1.5c-23.1 16.9-30.5 63.2-30.8 65.2l-1.4 9.2h167c-12.3 78.3-26.5 113.4-34 127.4-3.7 7-7.3 14-10.7 20.8-21.3 42.2-43.4 85.8-126.3 153.6-3.6 2.8-7 8-4.8 13.7 2.4 6.3 9.3 9.1 24.6 9.1 5.4 0 11.8-0.3 19.4-1 49.9-4.4 100.8-18 135.1-87.6 17-35.1 31.7-71.7 43.9-108.9L497 850l5-12c0.8-1.9 19-46.3 5.1-95.9l-0.5-1.8-108.1-123-22 16.6c6.4-26.1 10.6-49.9 12.5-71.1h158.7v-8c0-40.1-18.5-63.9-19.2-64.9l-2.4-3z"
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
              <a class="bottomLink" href="https://taosdata.com/careers"
                >Careers</a
              >
              <a class="bottomLink" href="https://taosdata.com/terms-of-service"
                >Terms</a
              >
              <a class="bottomLink" href="https://taosdata.com/privacy"
                >Privacy</a
              >
              <a class="bottomLinkLast" href="https://taosdata.com/about"
                >About</a
              >
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  data() {
    return {};
  },
  mounted() {},
  watch: {},
  methods: {
    subScript() {
      window.open("https://www.taosdata.com/news-letter");
    }
  }
};
</script>
<style scoped>
.footer {
  width: 100%;
  display: block;
  line-height: 1.5;
  font-size: 14px;
  font-weight: 400;
  color: white;
  font-family: "Open Sans", Helvetica, "Hiragino Sans GB", sans-serif,
    "Apple Color Emoji";
  flex-shrink: 0;
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

@media (max-width: 767px) {
  .left {
    width: 100%;
    padding-left: 0;
  }
}

.right {
  padding-left: 0;
  width: 50%;
  margin-top: 10px;
}
@media (max-width: 767px) {
  .right {
    padding-left: 0;
    width: 100%;
    margin-top: 10px;
  }
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

@media (max-width: 767px) {
  .bottomLeft {
    width: 100%;
  }
}
.bottomRight {
  width: 25%;
  display: flex;
  justify-content: flex-end;
  flex-wrap: wrap;
}
@media (max-width: 767px) {
  .bottomRight {
    width: 100%;
    justify-content: flex-start;
    margin-top: 10px;
  }
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
@media (max-width: 767px) {
  .sales {
    justify-content: flex-start;
  }
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
@media (max-width: 997px) {
  #menuShow {
    display: block;
  }
}
</style>
