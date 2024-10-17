<template>
  <div>
    <p>
      {{ $t("docs.dashboard.topdesc")
      }}<a href="https://www.grafana.com/"> Grafana</a
      >{{ $t("docs.dashboard.topdesc1") }}
      <!-- <a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >. -->
    </p>
    <!-- <p>
      {{ $t("docs.virtual.grafana.topdesc2")
      }}<a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >.
    </p> -->
  
    <h2 id="install-grafana">{{ $t("docs.virtual.grafana.step1") }}</h2>
    <p>
        {{ $t("docs.virtual.grafana.step1desc")
      }}(<a href="https://grafana.com/grafana/download"
        >https://grafana.com/grafana/download</a
      >).
    </p>
    <h2 id="install-tdengine-plugin">{{ $t("docs.virtual.grafana.step2") }}</h2>
    <el-tabs v-model="activeTab">
    <el-tab-pane name="plugins" label="Plugins">
      <!-- <p>{{$t('docs.virtual.grafana.pluginsdesc')}}</p> -->
      <p v-html="$t('docs.virtual.grafana.plugin1desc') "></p>
      <p v-html="$t('docs.virtual.grafana.plugin2desc')"></p>
      <p v-html="$t('docs.virtual.grafana.plugin3desc')"></p>
      <p v-html="$t('docs.virtual.grafana.plugin4desc')"></p>
    </el-tab-pane>
    <el-tab-pane name="script" label="Script"><p>{{ $t('docs.virtual.grafana.script1') }}</p>
    <pre
          v-highlight="
            `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
          "
        ><code class="language-bash"></code></pre>
        <p v-html="$t('docs.virtual.grafana.script2') "></p>
        <pre
          v-highlight="
            `sudo systemctl restart grafana-server.service
`
          "
        ><code class="language-bash"></code></pre>

    </el-tab-pane>
    
    </el-tabs>
    <!-- <p>{{ $t("docs.virtual.grafana.step2desc") }}</p>
    <pre
      v-highlight="
    
        `export ${replaceTDENGINE}_TOKEN=&quot;${token}&quot;
export ${replaceTDENGINE}_URL=&quot;${url}&quot;
`
      "
    ><code class="language-bash"></code></pre> -->
    <!-- <p>{{ $t("docs.virtual.grafana.step2desc1") }}</p>
    <pre
      v-highlight="
        `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.virtual.grafana.step2desc2") }}</p>
    <pre
      v-highlight="
        `sudo systemctl restart grafana-server.service
`
      "
    ><code class="language-bash"></code></pre> -->
    <h2 id="verify-plugin">{{ $t("docs.virtual.grafana.step3") }}</h2>
    <p>{{ $t('docs.virtual.grafana.step3desc') }}</p>
    <p>
      <i class="el-icon-s-opportunity" style="color: gold;font-size: 20px"></i>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <p style="display:flex;align-items: baseline;margin-bottom: 0px;">
        <span style="width:60px;">{{ $t('docs.virtual.grafana.step3desc1') }}</span>
      <pre
          v-highlight="
       
            `${url}
`
          "
        ><code class="language-bash"></code></pre>
    </p>
    <p style="display:flex;align-items: baseline;margin-bottom: 0px;">
        <span style="width:60px;">{{ $t('docs.virtual.grafana.step3desc2') }}</span>
      <pre
          v-highlight="
            `${username}
`
          "
        ><code class="language-bash"></code></pre>
    </p>




    <p v-html="$t('docs.virtual.grafana.step3desc3')"></p>
    <!-- <p>{{ $t("docs.virtual.grafana.step3desc") }}</p>
    <p v-if="!isOEM">
      <img
        src="./assets/grafana/verifying-tdengine-datasource.webp"
        alt="Verify TDengine data source"
      />
    </p> -->
    <!-- <h2 id="use-grafana">{{ $t("docs.virtual.grafana.step4") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step4desc") }}
    <span v-if="!isOEM">
      {{ $t("docs.virtual.grafana.step4desc1")
      }}
      <a
        :href="`${$t('urlPart')}/third-party/grafana#create-dashboard`"
        >{{ $t("docs.virtual.grafana.step4desc2") }}</a
      >   {{ $t("docs.virtual.grafana.step4desc3") }}</span>
    </p> -->
    <!-- <h2 id="add-grafana-dbsource">{{ $t("docs.dashboard.step5") }}</h2>
    <p v-html="$t('docs.dashboard.nav')"></p>
    <p>
      <img
        src="./assets/dashboard/configuration.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.subsearch')"></p>
    <p>
      <img
        src="./assets/dashboard/add.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.settingtd')"></p>
    <p>
      <img
        src="./assets/dashboard/howto-add-datasource.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p>{{ $t("docs.dashboard.savetest") }}</p>
    <p>
      <img
        src="./assets/dashboard/done.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p> -->
    <h2 id="import-dashboard">{{ $t("docs.dashboard.step5") }}</h2>
    <p v-html="$t('docs.dashboard.desc51')"></p>
    <p v-html="$t('docs.dashboard.desc52')"></p>
    <p v-html="$t('docs.dashboard.desc53')"></p>
    <p v-html="$t('docs.dashboard.desc54')"></p>
    <!-- <p v-html="$t('docs.dashboard.import')"></p>
    <p>
      <img
        src="./assets/dashboard/import.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.cont1')"></p>
    <p v-html="$t('docs.dashboard.cont2')"></p>
    <p>
      <img
        src="./assets/dashboard/search.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.cont3')"></p>
    <p>
      <img
        src="./assets/dashboard/keeper1.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p>{{ $t("docs.dashboard.cont4") }}</p> -->
  </div>
</template>

<script>
import Prism from "prismjs";
import "prismjs/themes/prism.css";
import "prismjs/components/prism-bash";
import { copy } from "@/utils/index";
export default {
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
    
  },
  data() {
    return {
      username:localStorage.getItem('username'),
      activeTab:'plugins',
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    };
  },
  computed: {
    urlPart() {
      return this.$i18n.locale.includes("en") ? "tdengine" : "taosdata";
    },
    replaceTDENGINE() {
      return this.isOEM ? "" : "TDENGINE";
    },
  },
  mounted() {
    Prism.highlightAll();
  },
  methods: {
    copyCode(val) {
      let text = document.querySelector(`.${val}`);
      copy(text.children[0].innerText);
    },
  },
};
</script>
<style lang="scss" scoped>
img {
  width: 894px;
  // height: 229px;
}
.copy-icon,
.copy-icon2,
.copy-icon3,
.copy-icon4,
.copy-icon5,
.copy-icon6 {
  visibility: hidden;
  display: flex;
  align-items: center;
  white-space: nowrap;
  cursor: pointer;
  color: #4259ce;
  position: absolute;
  right: 20px;
  top: 20px;
}
.first,.second,.fifth,.fourth,.sixth{
  padding-bottom: 5px;
  line-height: 0.5!important;
}
.first:hover {
  .copy-icon {
    visibility: visible;
  }
}
.second:hover {
  .copy-icon2 {
    visibility: visible;
  }
}
.third:hover {
  .copy-icon3 {
    visibility: visible;
  }
}
.fourth:hover {
  .copy-icon4 {
    visibility: visible;
  }
}
.fifth:hover {
  .copy-icon5 {
    visibility: visible;
  }
}
.sixth:hover {
  .copy-icon6 {
    visibility: visible;
  }
}
.float {
  position: absolute;
  top: -80px;
  right: -30px;
  z-index: 999;
  display: flex;
  width: 330px;
  padding: 14px 26px 14px 13px;
  border-radius: 8px;
  box-sizing: border-box;
  border: 1px solid #ebeef5;
  background-color: #fff;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
  transition: opacity 0.3s, transform 0.3s, left 0.3s, right 0.3s, top 0.4s,
    bottom 0.3s;
  overflow: hidden;
}
</style>
<style lang="scss">
::v-deep {
  .markdown-body pre {
    line-height: 0.1 !important;
  }
}
</style>
