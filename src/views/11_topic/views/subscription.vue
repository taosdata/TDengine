<template>
  <div>
    <div class="flexEnd">
      <el-button
        class="big-button"
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("topic.addShareTopicUser") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" size="mini" :data="subscriptionList">
      <el-table-column
        :label="$t('topic.user_name')"
        prop="user_name"
      ></el-table-column>
      <!-- <el-table-column label="Token" prop="id"></el-table-column>
      <el-table-column :label="$t('topic.topic')" prop="id"></el-table-column>
      <el-table-column :label="$t('createTime')" prop="id"></el-table-column>
      <el-table-column
        :label="$t('topic.database')"
        prop="id"
      ></el-table-column> -->
      <!-- <el-table-column fixed="right" width="50">
        <template slot-scope="{ row }">
          <el-button
            size="mini"
            @click="del(row)"
            plain
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column> -->
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <el-dialog
      align="center"
      :title="$t('topic.add_new_user')"
      width="400px"
      :visible.sync="dialog"
      :destroy-on-close="true"
    >
      <el-form
        :model="ruleForm"
        ref="ruleForm"
        label-width="120px"
        class="demo-ruleForm"
      >
        <el-form-item :label="$t('topic.user_name')" prop="user_name" required>
          <el-select v-model="ruleForm.user_name" style="width: 100%">
            <el-option
              v-for="item in userList"
              :key="item.name"
              :label="item.name"
              :value="item.name"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('topic.expire_time')" prop="expire_time">
          <el-date-picker
            v-model="ruleForm.expire_time"
            style="width: 100%"
            :picker-options="expireTimeOPtion"
            type="datetime"
          ></el-date-picker>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" style="width: 100%" @click="submotForm('ruleForm')"
            >新增</el-button
          >
        </el-form-item>
      </el-form>
    </el-dialog>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  props: {
    topicId: {
      type: String,
      default: "",
    }
  },
  data() {
    return {
      expireTimeOPtion: {
        disabledDate(time) {
          return time.getTime() < Date.now();
        },
      },
      subscriptionList: [],
      dialog: false,
      userList: [],
      currentPage: 1,
      pageSize: 10,
      total: 0,
      requestIng: false,
      ruleForm: {
        user_name: "",
        expire_time: "",
      },
    };
  },
  mounted() {
    this.getData();
    this.getUserList();
  },
  watch:{
    topicId:{
      deep:true,
      handler(val){
        this.getData()
        console.log(val,'舰艇');
      }
    }
  },
  methods: {
    async getData() {
      try {
        await sendSQLReq(
          `select user_name from information_schema.ins_user_privileges where privilege in ('all', 'subscribe') and object_name in ('${this.topicId}', 'all');`
        ).then((res) => {
          this.subscriptionList = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          console.log(this.subscriptionList, "查询消费用户");
        });
      } catch (error) {
        console.log(error);
      }
    },

    async getUserList() {
      try {
        await sendSQLReq(`show users;`)
          .then((res) => {
            this.userList = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });

            console.log(this.userList, "全部的users");
          })
          .catch((err) => {
            err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
        Message.error(error.desc);
      }
    },

    async addUser() {
      try {
        if (this.topicId) {
          await sendSQLReq(`grant subscribe on ${this.topicId}.* to ${this.ruleForm.user_name};`).then(res=>{
            if(res.rows){
              Message.success('Opeartion Successfully')
              this.getData()
            }
            
          });
        }else{
          Message({
            type:'error',
            message:this.$t('topic.select_topic_tip')
          })
        }
        this.dialog=false
      } catch (error) {
        console.log(error);
      }
    },
    submotForm(formName){
      this.$refs[formName].validate(valid=>{
        if(valid){
          this.addUser()
        }else{
          return false
        }
      })
    },
    del() {},
    handlePageChange() {},
  },
};
</script>

<style style='scss'>
.el-picker-panel__footer .el-button--text.el-picker-panel__link-btn {
  display: none;
}
</style>
