<template>
  <section class="markdown-body">
    <el-steps
      :active="active"
      finish-status="success"
    >
      <el-step :title="$t('dataIn.downloadInstall')"> </el-step>
      <el-step :title="$t('generateToken')"></el-step>
      <el-step :title="$t('configure')"></el-step>
      <el-step :title="$t('dataIn.runAgent')"></el-step>
    </el-steps>
    <section v-if="active == 1">
      <p v-dompurify-html="$t('docs.taosxAgent.1', downloadUrl)"></p>
      <pre v-highlight><code>taosx-agent -V</code></pre>
    </section>
    <section v-else-if="active == 2">
      <p v-dompurify-html="$t('docs.taosxAgent.2')"></p>
      <el-input
        v-model="name"
        maxlength="32"
        size="mini"
        :placeholder="$t('pInName')"
        class="mb10"
      ></el-input>
      <p
        v-if="nameValid()"
        class="errorText"
      >
        {{ $t('dataIn.agentNameExist', [this.name]) }}
      </p>
    </section>
    <section v-else-if="active == 3">
      <p v-dompurify-html="$t('docs.taosxAgent.3')"></p>
      <pre v-highlight><code>endpoint="{{ taoxAddress }}"
token="{{ token }}"</code></pre>
      <p v-dompurify-html="$t('docs.taosxAgent.6')"></p>
    </section>
    <section
      v-else-if="active == 4"
      class="step4"
    >
      <p v-dompurify-html="$t('docs.taosxAgent.4')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane
          label="Linux"
          name="0"
        >
          <pre v-highlight><code>systemctl start taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane
          label="Windows"
          name="1"
        >
          <pre v-highlight><code>sc start taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>
      <p v-dompurify-html="$t('docs.taosxAgent.5')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane
          label="Linux"
          name="0"
        >
          <pre v-highlight><code>systemctl status taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane
          label="Windows"
          name="1"
        >
          <pre v-highlight><code>sc query taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>
   
      <el-button
        class="mb20"
        size="small"
        @click="checkAgentStatus"
        :type="checkBtnType"
        >{{ checkBtnText }}</el-button
      >
      <el-tag
        v-if="statusMap[agentStatus]"
        class="ml20"
        :type="statusMap[agentStatus].type"
      >
        {{ $t(statusMap[agentStatus].label) }}
      </el-tag>
      <template v-if="agentStatus == 'failed'">
        <p v-dompurify-html="$t('docs.taosxAgent.11')"></p>
        <el-tabs v-model="tabActive">
          <el-tab-pane
            label="Linux"
            name="0"
          >
            <pre v-highlight><code>journalctl -u taosx-agent</code></pre>
          </el-tab-pane>
          <el-tab-pane
            label="Windows"
            name="1"
          >
            <pre v-highlight><code>C:\Program Files\taosX\log\agent\</code></pre>
          </el-tab-pane>
        </el-tabs>
        <p
          style="margin-bottom: 16px"
          v-dompurify-html="$t('docs.taosxAgent.12')"
        ></p>
      </template>
    </section>
    <section class="flexCenter">
      <el-button
        :disabled="active == 1"
        size="mini"
        @click="active--"
        >{{ $t('prev') }}</el-button
      >
      <el-button
        size="mini"
        type="primary"
        :loading="loading"
        @click="next"
        :disabled="nextButton"
        >{{ nextButtonText }}</el-button
      >
    </section>
  </section>
</template>

<script>
import 'github-markdown-css/github-markdown-light.css';
import { OfficialSite } from '@/const';
import {
  addNewAgent,
  editAgent,
} from "@/api/explorer/agent";
export default {
  props: {
    agent: {
      type: Object,
      default: () => ({})
    }
  },
  name:'AddAgent',
  components: {},
  data() {
    this.statusMap = {
      failed: {
        label: 'docs.taosxAgent.9',
        type: 'danger'
      },
      success: {
        label: 'docs.taosxAgent.8',
        type: 'success'
      }
    };
    return {
      oldActive:0,
      active: 1,
      name: '',
      tokenMap: {},
      loading: false,
      tabActive: '0',
      agentStatus: '',
      requestIng: false,
      checkIng: false
    };
  },
  computed: {
    checkBtnText() {
      return this.$t('docs.taosxAgent.' + (this.checkIng ? '10' : '7'));
    },
    checkBtnType() {
      return this.checkIng ? 'primary' : '';
    },
    agentList() {
      return this.$store.state.app.agentLists.filter(item => item.id !== this.agent?.id);
    },
    downloadUrl() {
      const assetsUrl = OfficialSite + '/assets-download/3.0/taosx-agent-1.2.4-';
      return {
        linuxDL: assetsUrl + 'linux-x64.tar.gz',
        windowDL: assetsUrl + 'windows-x64-installer.exe'
      };
    },
    nameError() {
      return this.nameValid();
    },
    token() {
      return this.tokenMap[this.name] ?? '';
    },
    taoxAddress() {
      return localStorage.getItem("local_endpoint") ?? '';
    },
    nextButton() {
      if (this.loading) return true;
      if (this.active == 2) {
        if (!this.name || this.nameError) {
          return true;
        }
      }
      return false;
    },
    nextButtonText() {
      return this.active == 4 ? this.$t('dataIn.finish') : this.$t('next');
    }
  },
  mounted() {
    if (this.agent?.id) {
      this.name = this.agent.name;
      this.active = 2;
    }
  },
  methods: {
    checkAgentStatus() {
      if (this.checkIng) return;
      this.checkIng = true;
      this.$store
        .dispatch('app/getAgentList')
        .then(() => {
          const status = this.$store.state.app.agentLists.find(item => item.name == this.name)?.status;
          this.agentStatus = ['idle', 'busy'].includes(status) ? 'success' : 'failed';
        })
        .catch(() => {
          this.agentStatus = '';
        })
        .finally(() => {
          this.checkIng = false;
        });
    },
    submit() {
      if (this.loading) return;
      this.loading = true;
      const fn = this.agent?.id ? editAgent : addNewAgent;
      fn(this.name, this.agent?.id)
        .then(({ token,id }) => {
          this.$set(this.tokenMap, this.name, token);
          this.active++;
          this.$store.dispatch('app/getAgentList');
          Object.assign(
            this.agent,
            this.$store.state.app.agentList.find(item => item.id == id)
          );
          this.$emit('submit');
        })
        .catch(() => {})
        .finally(() => {
          this.loading = false;
        });
    },
    next() {

      if (this.active == 4) {
        this.agentStatus = 'noCheck';
        this.$parent.$parent.showAgent=false
      }
      if (this.active == 2) {
        this.submit();
      } else {
        this.active++;
      }
    },
    nameValid() {
      if ((this.name&&this.oldActive!=3)&&this.oldActive!=1) {
        return this.agentList.some(item => item.name == this.name);
      } else {
        return false;
      }
    }
  },
  watch:{
    active:{
      deep:true,
      handler(val,oldval){
        this.oldActive=oldval
      }
    }
  }
};
</script>

<style scoped lang="scss">
.markdown-body {
  padding: 0;
  word-break: break-word;
  .step4 p {
    margin-bottom: 0;
  }
  p {
    line-height: 24px;
  }
  .mb10{
    margin-bottom:10px;
  }
  .mb20{
    margin-bottom:20px;
  }
  .ml20{
    margin-left:20px;
  }
}
</style>
