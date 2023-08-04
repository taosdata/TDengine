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
      >{{ $t("docs.tool.cli.step1desc3") }} <a :href="installUrlMac">Mac</a
      >{{ $t("docs.tool.cli.step1desc4") }}
    </p>
    <h2 id="config">{{ $t("docs.tool.cli.step2") }}</h2>
    <el-tabs v-model="sysActivateTab" groupId="sys">
      <el-tab-pane name="linux" label="Config on Linux">
        <p>{{ $t("docs.tool.cli.step2desc") }}</p>
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Config on Windows (beta)">
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
      <el-tab-pane name="mac" label="Config on Mac (beta)" groupId="sys">
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
      <el-tab-pane name="linux" label="Connect on Linux">
        <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Connect on Windows (beta)">
        <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos.exe -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Connect on Mac (beta)">
        <p>{{ $t("docs.tool.cli.step3desc") }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ $t("docs.tool.cli.step3desc1") }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="using-tdengine-cli">{{ $t("docs.tool.cli.step4") }}</h2>
    <p>{{ $t("docs.tool.cli.step4desc") }}</p>
    <pre
      v-highlight
    ><code>Welcome to the TDengine shell from Linux, Client Version:3.0.0.0
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Successfully connect to cloud.tdengine.com:8085 in restful mode

taos&gt;
</code></pre>
    <p>
      {{ $t("docs.tool.cli.step4desc1") }}&nbsp;
      <a
        :href="`https://docs.${urlPart}.com/reference/taos-shell#execute-sql-script-file`"
        >{{ $t("docs.tool.cli.step4desc2") }}</a
      >&nbsp;{{ $t("docs.tool.cli.step4desc3") }}
    </p>
  </div>
</template>

<script>
import { TdengineVersion } from "@/const";
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
    };
  },
  computed: {
    DSN() {
      return `taos://${this.user}:${this.password}@${this.url.replace(/https?:\/\//, "")}`;
    },
    urlPart() {
      return navigator.language.includes('en') ?"tdengine": "taosdata";
    },
    installUrlLinux() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-client-${TdengineVersion}-Linux-x64.tar.gz`;
    },
    installUrlMac() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-client-${TdengineVersion}-macOS-x64.pkg`;
    },
    installUrlWindows() {
      const urlPart = this.urlPart;
      return `https://www.${urlPart}.com/assets-download/3.0/TDengine-client-${TdengineVersion}-Windows-x64.exe`;
    },
  },
};
</script>
