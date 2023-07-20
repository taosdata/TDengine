<template>
  <div class="agent-doc">
    <h1>1.{{ $t("taosagents.step1") }}</h1>
    <section>
      <el-tabs v-model="activeTab">
        <el-tab-pane name="tab1" :label="$t('taosagents.step1linux')">
          <p v-html="$t('taosagents.linuxdesc')"></p>
          <pre class="agent-code" style="padding-top:0px;"><code class="language-bash">
tmpdir=`mktemp -d`
cd  $tmpdir
mkdir agent-installer
cd agent-installer
wget -c https://www.taosdata.com/assets-download/3.0/taosx-agent-latest-linux-x64.tar.gz
tar xvf taosx-agent-latest-linux-x64.tar.gz
cd taosx-agent-*
./install.sh
# remove files
#cd ../../; rm -rf $tmpdir</code>
<span class="copy-icon" @click="copyCode('agent-code')">
          <i class="el-icon-copy-document"></i>
          {{ $t("copy") }}
        </span>
        </pre>
        </el-tab-pane>
        <el-tab-pane name="tab2" :label="$t('taosagents.step1window')">
          <p v-html="$t('taosagents.windowdesc')"></p>
        </el-tab-pane>
      </el-tabs>
    </section>
    <h1>2.{{ $t("taosagents.step2") }}</h1>
    <section>
      <p>{{ $t("taosagents.step2sub1") }}</p>
      <el-tabs v-model="activeTab">
        <el-tab-pane name="tab1" :label="$t('taosagents.step1linux')">
          <p v-html="$t('taosagents.step2sub2linux')"></p>
        </el-tab-pane>
        <el-tab-pane name="tab2" :label="$t('taosagents.step1window')">
          <p v-html="$t('taosagents.step2sub2window')"></p>
        </el-tab-pane>
      </el-tabs>

      <p>{{ $t("taosagents.step2sub3") }}</p>
      <el-alert
        style="margin-bottom: 10px"
        :title="$t('copyagentWaring')"
        type="warning"
        :closable="false"
        show-icon
      >
      </el-alert>
      <pre v-highlight><code>endpoint = "{{endpoint}}"
token = "{{token}}"
</code></pre>
      <p>{{ $t("taosagents.step2sub4") }}</p>
    </section>
    <h1>3.{{ $t("taosagents.step3") }}</h1>
    <section>
      <p>{{ $t("taosagents.step3sub1") }}</p>
      <el-tabs v-model="activeTab">
        <el-tab-pane name="tab1" :label="$t('taosagents.step1linux')">
          <p v-html="$t('taosagents.step3sub2linux')"></p>
          <p v-html="$t('taosagents.step3sub3linux')"></p>
        </el-tab-pane>
        <el-tab-pane name="tab2" :label="$t('taosagents.step1window')">
          <p v-html="$t('taosagents.step3sub2window')"></p>
          <p v-html="$t('taosagents.step3sub3window')"></p>
        </el-tab-pane>
      </el-tabs>

      
    </section>
    <h1>4.{{ $t("taosagents.step4") }}</h1>
    <section>
      <p>{{ $t("taosagents.step4sub1") }}</p>
    </section>
  </div>
</template>
<script>
import Prism from "prismjs";
import "prismjs/themes/prism.css";
import "prismjs/components/prism-bash";
import { copy } from "@/utils/index";
export default {
  name: "AgentDoc",
  props: {
    token: {
      type: String,
      default: "",
    },
  },
  data() {
    return {
      activeTab: "tab1",
      endpoint: localStorage.getItem("local_endpoint"),
    };
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
h1 {
  text-align: left;
  color: #0969da;
  font-size: 18px;
  margin-bottom: 10px;
}
p {
  text-align: left;
  margin-bottom: 10px;
  code {
    background: rgba(175, 184, 193, 0.2) !important;
  }
}
.pre-code {
  background-color: #f6f8fa;
  padding: 10px;
  margin-bottom: 10px;
  text-align: left;
  white-space: break-spaces;
  code {
    background: transparent !important;
  }
}
::v-deep {
  code {
    background: rgba(175, 184, 193, 0.2) !important;
    border-radius: 6px;
    padding: 4px;
  }
}
.agent-code {
  background-color: #f6f8fa;
  line-height: 3px;
  code {
    background: transparent !important;
    line-height: 8px;
  }
  &:hover {
    .copy-icon {
      visibility: visible;
    }
  }
}
.copy-icon {
  visibility: hidden;
  display: flex;
  align-items: center;
  white-space: nowrap;
  cursor: pointer;
  color: #4259ce;
  position: absolute;
  right: 20px;
  top: 40px;
  font-size: 12px;
}
.agent-doc {
  padding: 15px;
}
</style>
