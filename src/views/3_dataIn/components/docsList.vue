<template>
  <div class="connector">
    <ul class="client-list">
      <li v-for="(item, index) in localDocList" :title="item.name" :key="index">
        
        <router-link class="client-item" :to="getUrl(item.name, item.icon, item.path)">
          <h2 class="title">
            
            <img class="image" :src="getImg(item.name, item.icon)" alt="" />
            <span>{{ item.title || item.name }}</span>
          </h2>
          <p class="desc nowrap">
            {{ item.desc }}
          </p>
        </router-link>
      </li>
    </ul>
  </div>
</template>

<script>
export default {
  props: {
    parentUrl: {
      type: String,
      default: "",
    },
    docsList: {
      type: Function,
      default: () => [],
    },
    urlPre: {
      type: String,
      default: "",
    },
  },
  data(){
    return {
      localDocList:[]
    }
  },
  computed: {
    language() {
      return this.$i18n.locale;
    },
  },
  watch: {
    language() {
      this.localDocList=this.docsList()
    },
  },
  mounted(){
    this.localDocList=this.docsList()
  },
  methods: {
    getUrl(name, _, path) {
      return this.parentUrl + this.urlPre + encodeURIComponent(path ?? name);
    },
    getImg(name, icon) {
      if(name=='REST API'){
        name='restapi'
      }
      if(name=='TDengine CLI'){
        name='tdenginecli'
      }
      if(name=='Google Data Studio'){
          name='gdStudio'
        }
      try {
          return require(`@/assets/images/${icon || name}.svg`);
        } catch (err) {
          return require(`@/assets/logo.svg`);
        }
    },
  },
};
</script>
<style lang="scss" scoped>
.connector {
  $item-width: 150px;
  $margin-size: 20px;
  .client-list {
    display: flex;
    flex-wrap: wrap;
    margin-bottom: 30px;
    li {
      width: calc((100% - #{$margin-size} * 3) / 3);

      .client-item {
        padding: 30px;
        display: block;
      }
      border: 1px solid $item-border-color;
      margin-right: $margin-size;
      margin-top: $margin-size;
      border-radius: 15px;
      $img-size: 30px;
      color: rgb(96, 103, 112);
      h2 {
        font-size: 20px;
        font-weight: bold;
        line-height: $img-size;
        span {
          margin-left: 10px;
        }
      }
      .image {
        width: $img-size;
        height: $img-size;
        object-fit: contain;
        vertical-align: middle;
      }
      .desc {
        font-size: 13px;
        line-height: 22px;
      }
      &:hover {
        border: 1px solid $color-primary;
        box-shadow: rgba(0, 0, 0, 0.05) 0px -9px 9px;
      }
    }
  }
}
</style>
