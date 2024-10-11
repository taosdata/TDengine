import { getDataSources } from "@/api/explorer/community";
import { sendSQLReq } from "@/api/gateway/console";

export default {
  data() {
    return {};
  },
  created() {},
  computed: {},
  methods: {
    async getGrantsFull() {
      // 行业版才调用接口
      if (this.$INDUSTRY) {
        let res = await sendSQLReq('show grants full;')
        let array = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        localStorage.setItem("allLicenseNameData",JSON.stringify(array))
      }
    },
    getMetaShow(grantName) {
      let array = JSON.parse(localStorage.getItem('allLicenseNameData')) || [];
      let allLicenseNameData = array.map((item) => item.grant_name);
      switch (grantName) {
        case 'dataIn':
          let result = getDataSources(this.$i18n.locale);
          result = result.filter(item => allLicenseNameData.includes(item.license_id))
          return this.$INDUSTRY ? result?.length > 1 : true;
        case 'dnodes':
          let dnodes = array.filter((item) => item.grant_name == grantName);
          let dnodeNum = dnodes[0]?.limits?.split('/')[0]
          return this.$INDUSTRY ? dnodeNum > 1 : true;
        default:
          return this.$INDUSTRY ? allLicenseNameData.includes(grantName) : true;
      }
    } 
  }
};
