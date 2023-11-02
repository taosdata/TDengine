<template>
  <div>
    <p>{{ $t("docs.tool.cli.topdesc") }}</p>
    <h2 id="installation">{{ $t("docs.tool.cli.step1") }}</h2>
    <p>
      {{ $t("docs.tool.cli.step1desc") }}
      <b>{{ $t("docs.tool.cli.step1desc1") }}</b
      >&nbsp;{{ $t("docs.tool.cli.step1desc2") }}<a :href="installUrlLinux">Linux</a
      >{{ $t("docs.tool.cli.step1desc3")
      }}<a :href="installUrlWindows">Windows</a
      >{{ $t("docs.tool.cli.step1desc3") }} <a :href="installUrlMac">MacOS-x64</a
      >{{ $t("docs.tool.cli.step1desc3") }} <a :href="installURLArm">MacOS-arm64</a
      >{{ $t("docs.tool.cli.step1desc4") }}
    </p>
    <h2 id="config">{{ $t("docs.tool.cli.step2") }}</h2>
    <el-tabs v-model="sysActivateTab" groupId="sys">
      <el-tab-pane name="linux" label="Linux">
        <p>{{ $t("docs.tool.cli.step2desc") }}</p>
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <p>{{ $t("docs.tool.cli.step2desc1") }}</p>
        <pre
          v-highlight="
            `set TDENGINE_DSN=${DSN}
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ $t("docs.tool.cli.step2desc2") }}</p>
        <pre
          v-highlight="
            `$env:TDENGINE_DSN='${DSN}'
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac" groupId="sys">
        <p>{{ $t("docs.tool.cli.step2desc3") }}</p>
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="connect">{{ $t("docs.tool.cli.step3") }}</h2>
    <el-tabs value="linux" groupId="sys">
      <el-tab-pane name="linux" label="Linux">
        <!-- <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre> -->
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <!-- <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre> -->
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos.exe -E %TDENGINE_DSN%
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac">
        <!-- <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre> -->
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="using-tdengine-cli">{{ $t("docs.tool.cli.step4") }}</h2>
    <p>{{ $t("docs.tool.cli.step4desc") }}</p>
    <pre
      v-highlight
    ><code>{{ code }}
</code></pre>
    <p>
      {{ $t("docs.tool.cli.step4desc1") }}&nbsp;
      <!-- <a
        :href="`${$t('urlPart')}/reference/taos-shell#execute-sql-script-file`"
        target="_blank"
        >{{ $t("docs.tool.cli.step4desc2") }}</a
      >&nbsp;{{ $t("docs.tool.cli.step4desc3") }} -->
    </p>
  </div>
</template>

<script>
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
    user: {
      type: String,
      default: ''
    },
    password: {
      type: String,
      default: ''
    }
  },
  data() {
    return {
      sysActivateTab: "linux",
      version:localStorage.getItem('agent_version'),
      code:`Welcome to the TDengine shell from Linux, Client Version:${localStorage.getItem('agent_version')}
Copyright (c) 2023 by TAOS Data, Inc. All rights reserved.

Successfully connect to tdengine.com:8085 in restful mode

taos>`
    };
  },
  computed: {
    DSN() {
      return `taos://${this.user}:${this.password}@${this.url.replace(/https?:\/\//, "")}`;
    },
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    installUrlLinux() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-enterprise-client-${this.version}-Linux-x64.tar.gz`;
    },
    installUrlMac() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-client-${this.version}-macOS-x64.pkg`;
    },
    installURLArm(){
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-client-${this.version}-macOS-arm64.pkg`;
    },
    installUrlWindows() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-enterprise-client-${this.version}-Windows-x64.exe`;
    },
  },
};
</script>
