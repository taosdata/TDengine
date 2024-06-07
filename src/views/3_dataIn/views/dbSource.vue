<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :editId="editId"
      :isEditable="isEditable"
      :isCopyable="isCopyable"
      ref="table"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import SourceConfig from "./sourceConfig.vue"
import { getDataSources } from "@/api/explorer/community";
import { sendSQLReq } from "@/api/gateway/console";

export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    sourceConfig: SourceConfig
  },
  data() {
    return {
      currentName: "",
      editId: 0,
      isEditable: false,
      isCopyable: false,
      agentID: "",
      currentTaskStatus: "",
    };
  },
  created() {
    this.getData();
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
        let version = localStorage.getItem("agent_version");
        let [a, b, c, d] = version.split(".");
        if (a > 3 || (a == 3 && b > 3) || (a == 3 && b == 3 && c >= 1)) {
          await sendSQLReq(`show grants full;`).then((res) => {
            let array = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
            allData = array.map((item) => item.grant_name)
            result = result.filter(item => allData.includes(item.license_id))
          })
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
      if (type && !this.isEditable) {
        //新增
        this.isEditable = false;
        this.setEditID('')
        this.currentName = "sourceConfig";
      } else {
        this.currentName = "sourceConfig";
        this.isEditable = true;
        this.getData();
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
