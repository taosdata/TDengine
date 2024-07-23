<template>
  <div>
    <h2 :id="'{{config}}'">{{ $t("component.docConfig.title") }}</h2>
    <p>{{ $t("component.docConfig.content", [urlDes]) }}</p>
    <p>
      <i class="el-icon-s-opportunity" style="color: gold;font-size: 20px"></i>
      <span class="docker-tip">{{ $t("dockerTip", [`${baseurl.split('//')[1]}`] )}}</span>
    </p>
    <el-tabs class="doc-config-tab" value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre v-highlight="contentBash"><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre v-highlight="contentCMD"><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="contentPower"
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>
    <p>{{ $t("component.docConfig.bottom") }}</p>
  </div>
</template>
<script>
export default {
  namne: "DocConfig",
  props: {
    url: {
      type: String,
      default: "",
    },
    token: {
      type: String,
      default: "",
    },
    id: {
      type: String,
      default: "config",
    },
    needToken: {
      type: Boolean,
      default: true,
    },
    urlKey: {
      type: String,
      default: "TDENGINE_URL",
    },
    urlDes: {
      type: String,
      default: "URL and Token",
    },
  },
  computed: {
    contentBash() {
      return this.getContent("bash");
    },
    contentCMD() {
      return this.getContent("cmd");
    },
    contentPower() {
      return this.getContent("psh");
    },
    baseurl() {
      return localStorage.getItem("base_url");
    },
  },
  methods: {
    getContent(cType) {
      let result = "";
      const tmpURLKey = this.urlKey;
      const tURL = this.url;
      let tmpURL = `${tmpURLKey}="${tURL}"`;
      let tmpToken = "";
      let mtoken = "";
      if (this.needToken) {
        mtoken = this.token;
        tmpToken = `TDENGINE_TOKEN="${mtoken}"`;
      }
      switch (cType) {
        case "bash": {
          cType = "export ";
          break;
        }
        case "cmd": {
          cType = "set ";
          tmpURL = `${tmpURLKey}=${tURL}`;
          tmpToken = `TDENGINE_TOKEN=${mtoken}`;
          break;
        }
        case "psh": {
          cType = "$env:";
          tmpURL = `${tmpURLKey}='${tURL}'`;
          tmpToken = `TDENGINE_TOKEN='${mtoken}'`;
          break;
        }
      }
      result = `${cType}${tmpURL}`;
      if (this.needToken&&this.$route.name!=='Topic Example') {//数据订阅的python不展示token
        result += `\n${cType}${tmpToken}`;
      }
      return result;
    },
  }
};
</script>
