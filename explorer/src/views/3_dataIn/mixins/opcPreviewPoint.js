import { getTicket, checkReadyFile } from '@/api/explorer/datain';
import { getDsnData,getFieldClassMarkName } from '../utils';

export default {
    data() {
      return {
        loading: false,
        complete: false,
        ticket: "",
        categoryOpc: 'PointList'
      }
    },
    computed: {
      validFieldList() {
        const result = [];
        this.getValidFieldList(this.sourceParent.currentDefinition.config, result);
        return result;
      }
    },
    watch: {
      "$store.state.app.complete"(val) {
        if (val) {
          this.timer && clearInterval(this.timer)
          this.loading = false
        }
      }
    },
    beforeMount() { 
    },
    beforeDestroy() {
      this.timer && clearInterval(this.timer)
    },
    mounted() {
    },
    methods: {
      search() {
        const errorMsg = [];
        const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
        this.sourceParent.$refs.form.validateField(validFieldList, valid => {
          errorMsg.push(valid);
          if (errorMsg.length == validFieldList.length && errorMsg.every(item => !item)) {
            let type = this.sourceParent.sourceForm.type
            let form = type + getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
            let via = this.sourceParent.sourceForm.agent;
           
            this.searchDatasets(form, via);
          } else {
            this.$nextTick(() => {
              document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
            });
          }
        }); 
      },
   
      async searchDatasets(from, via) {
        if (this.loading) return;
        try {
          this.loading = true;
          let result = await getTicket(from, via, this.categoryOpc)
          this.ticket = result.ticket
          this.$store.commit("app/SET_TICKET",this.ticket);
    
          this.timer = setInterval(async () => {
            let { complete } = await checkReadyFile(result.ticket)
            this.complete = complete
            this.$store.commit("app/SET_COMPLETE",complete)
          }, 2000);
        } catch (error) {
          this.timer && clearInterval(this.timer)
        }
      },

      getValidFieldList(data, result, parent = 'data') {
        for (const val of data) {
          if (val.field == 'checkConnectivity') break;
          if (val.children) {
            this.getValidFieldList(val.children, result, parent + '.' + val.field);
          } else {
            if (val.required) {
              result.push(parent + '.' + val.field);
            }
          }
        }
      },
    }
}