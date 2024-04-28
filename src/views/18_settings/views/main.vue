<template>
  <div class="setting-content">
    <!-- <section class="theme">
      <span class="title">Theme：</span>
      <ul class="theme-list">
        <li v-for="(item, index) in themes" :key="index">
          <img
            :src="item.src"
            alt=""
            @click="chooseTheme(index)"
            :class="[item.isActive ? 'active' : '']"
          />
          <p>
            <span>{{ item.name }}</span>
          </p>
        </li>
      </ul>
    </section>
    <section class="logo">
      <span class="title">Logo：</span>
      <el-upload
        class="upload-demo"
        drag
        action="https://jsonplaceholder.typicode.com/posts/"
        multiple
      >
        <i class="el-icon-upload"></i>
        <div class="el-upload__text">
          Drag the file here, or <em> click Upload</em>
        </div>
        <div class="el-upload__tip" slot="tip">
          Only jpg/png files can be uploaded, and the size does not exceed 500kb
        </div>
      </el-upload>
    </section>
    <section class="welcoming">
      <span class="title">Welcoming Speech：</span>
      <el-input
        type="textarea"
        :rows="2"
        placeholder="Please enter a welcome message"
        v-model="textarea"
      >
      </el-input>
    </section> -->
    <section class="grafana">
      <span class="title" style="white-space: nowrap">{{$t('taossetting.grafanaurl')}}</span>
      <el-input placeholder="" v-model="grafanaUrl"></el-input>
    </section>
    <section class="bottom">
      <el-button type="primary" @click="saveGrafana">{{$t('taossetting.save')}}</el-button>
    </section>
  </div>
</template>
<script>
import { Message } from 'element-ui';
export default {
  data() {
    return {
      textarea: "",
      grafanaUrl:'',
      themes: [
        {
          name: "Default",
          isActive: false,
          src: "https://gw.alipayobjects.com/zos/bmw-prod/ae669a89-0c65-46db-b14b-72d1c7dd46d6.svg",
        },
        {
          name: "Dark",
          isActive: false,
          src: "https://gw.alipayobjects.com/zos/bmw-prod/0f93c777-5320-446b-9bb7-4d4b499f346d.svg",
        },
        {
          name: "Green",
          isActive: false,
          src: "https://gw.alipayobjects.com/zos/bmw-prod/3e899b2b-4eb4-4771-a7fc-14c7ff078aed.svg",
        },
      ],
    };
  },
  methods: {
    chooseTheme(index) {
      this.themes = this.themes.map((item, ind) => {
        if (ind === index) {
          item.isActive = true;
        } else {
          item.isActive = false;
        }
        return item;
      });
    },
    saveGrafana(){
      let reg =/^(http|https):\/\/([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*/;
      if(reg.test(this.grafanaUrl)){
        localStorage.setItem('local_grafana',this.grafanaUrl)
        Message.success('Save Scuessfully')
      }else{
        if(this.grafanaUrl){
          this.$error('Please enter the correct  url.')
        }else{
          localStorage.removeItem('local_grafana')
        }
        
      }
    }
  },
};
</script>
<style lang="scss" scoped>
.setting-content {
  display: flex;
  flex-direction: column;
  //   border: 1px solid #f0f0f0;
  padding: 20px;
  section {
    display: flex;
    margin-bottom: 20px;
  }
  .theme {
    display: flex;
    flex-direction: row;
    align-content: center;
    padding-left: 82px;
    .title {
      display: inline-block;
      // display: flex;
      // align-items: center;
      margin-right: 15px;
    }
    .theme-list {
      display: flex;
      li {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        margin-right: 20px;
        img {
          border-radius: 20px;
          cursor: pointer;
          box-shadow: rgba(0, 0, 0, 0.05) 0px -9px 9px;
          &:hover {
            scale: (1.1);
          }
        }
        .active {
          box-shadow: 0 0 0 1px #ffffff, 0 0 0 5px #1677ff;
        }
        p {
          margin-top: 15px;
        }
      }
    }
  }
  .logo {
    padding-left: 88px;
    .title {
      display: inline-block;
      margin-right: 15px;
      white-space: nowrap;
    }
    .el-upload-dragger {
      width: 200px;
      height: 180px;
    }
  }
  .welcoming {
    .title {
      display: inline-block;
      white-space: nowrap;
      margin-right: 15px;
    }
    .el-textarea__inner {
      width: 50%;
    }
  }
  .grafana {
    .title {
      display: inline-block;
      width: 150px;
      flex-shrink: 0;
    }
  }
  .bottom{
    justify-content: center;
    .el-button{
      flex:0.5;
    }
  }
}
</style>