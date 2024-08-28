<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :editId="editId"
      :isEditable="isEditable"
      :isCopyable="isCopyable"
      :isViewable="isViewable"
      ref="table"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import SourceConfig from "./sourceConfig.vue";
import SourceInfo from "./sourceInfo.vue";
import { getUIData, getTask } from "@/api/explorer/datain";
import { getDataSources } from "@/api/explorer/community";
import { sendSQLReq } from "@/api/gateway/console";
import { compareVersion } from "@/utils";

export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    sourceConfig: SourceConfig,
    sourceInfo: SourceInfo
  },
  data() {
    return {
      currentName: "",
      editId: 0,
      isEditable: false,
      isCopyable: false,
      isViewable: false,
      agentID: "",
      currentTaskStatus: "",
    };
  },
  created() {
    this.getData();
  },
  computed: {
    TDengineVersion() {
      return localStorage.getItem('agent_version');
    },
    isLessThen3_3_3_0() {
      return compareVersion(this.TDengineVersion, '<3.3.3.0')
    }
  },
  watch: {
    "$i18n.locale": {
      deep: true,
      async handler(val) {
        if (!this.isEditable) {
          await this.getData();
        }
      },
    },
  },
  methods: {
    async getData() {
      try {
        let result = getDataSources(this.$i18n.locale);
        let allData = [];
        if (this.$INDUSTRY) {
          let array = JSON.parse(localStorage.getItem('allLicenseNameData')) || [];
          const filterRes = array.filter(item => ['csv'].includes(item.grant_name));
          if (this.isLessThen3_3_3_0) {
            // 3_3_3_0 之前默认保留 csv 数据源，不参与授权
            if (filterRes.length < 0) {
              array.push({
                grant_name: "csv"
              })
            }
          }
          let allLicenseNameData = array.map((item) => item.grant_name);
          result = result.filter(item => allLicenseNameData.includes(item.license_id))
        }
        this.$store.commit("app/SET_DEFINITIONS", result);
      } catch (error) {
        console.log(error);
      }
    },
    changeEditable(val) {
      this.isEditable = val;
    },
    setEditID(val) {
      this.editId = val;
    },
    async toggleComponent(type, id, editid, dbname) {
      console.log('this.isViewable',this.isViewable);
      if (type && !this.isEditable) {
        //新增
        this.isEditable = false;
        this.setEditID('')
        this.currentName = "sourceConfig";
      } else {
        if (this.isViewable) {
          this.currentName = "sourceInfo";
          this.getData();
        } else {
          this.currentName = "sourceConfig";
          this.isEditable = true;
          this.getData();
        }
      }
    },
    hasProp(obj, key) {
      return Object.hasOwnProperty.call(obj, key);
    },
    reloadTable() {
      if (this.currentName == "dbsource" && !this.$COMMUNITY) {
        this.$nextTick(() => {
          this.$refs.table.refresh();
        });
      }
    },
  }
};
</script>
<style lang="scss" scoped></style>
