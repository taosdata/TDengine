export default {
    data() {
        return {
            pageSize: 10,
            currentPage: 1,
            total: 10,
            dialog: false,
            ruleForm: {
                endpoint: "",
                DNodes:''
            },
            rules: {
                endpoint: [
                    {
                        message: "Please enter the end point",
                        trigger: "blur",
                    },
                ],
                DNodes:[
                    {
                        required:true,message:"Please select the dnode",trigger:'change'
                    }
                ]
            },
        }
    },
    computed: {
        confirmStatus() {
            if (!this.ruleForm.endpoint) {
                return true
            }
            return false
        }
    },
    methods: {
        del(data) {
            this.$confirm("Are you sure  to delete "+data.endpoint + '?', "Warning", {
              confirmButtonText: "Ok",
              cancelButtonText: "Cancel",
              type: "warning",
            });
          },
        add() {
            this.dialog = true
        },
        closeDialog(){
            this.$refs.ruleForm.resetFields();
            this.$refs.ruleForm.clearValidate()
             this.dialog=false
         },
    }
}