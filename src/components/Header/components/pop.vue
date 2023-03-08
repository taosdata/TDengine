<template>
    <div class="popsearch" v-if="hidden" @click="closeBtn">
        <div class="popsearch-container">        
          <div class='searchBar'>        
            <input placeholder="Search" v-model="searchval" class="input" @click.stop  @keyup.enter="search" />
            <a @click.stop="search" >
              <label class='iconLabel'>                
                  <svg alt=" Database" role="img" width="20" height="20" class='searchIcon' viewBox="0 0 20 20"><path d="M14.386 14.386l4.0877 4.0877-4.0877-4.0877c-2.9418 2.9419-7.7115 2.9419-10.6533 0-2.9419-2.9418-2.9419-7.7115 0-10.6533 2.9418-2.9419 7.7115-2.9419 10.6533 0 2.9419 2.9418 2.9419 7.7115 0 10.6533z" stroke="currentColor" fill="none" fillRule="evenodd" strokeLinecap="round" strokeLinejoin="round"></path></svg>
              </label>
            </a>                
          </div>
        </div>        
	</div>
</template>
<script>
export default {
  name:'SearchPop',
  props: {
    hidden: {
      type: Boolean,
      default: false,
    }
  },
  components: {
 
  },
  data() {
    return {
      hiddenSubMenu: true,
      searchval:""
    };
  },
  mounted() {},
  methods: {
    closeBtn(){
      this.$emit("update:hidden", false);
    },
    search(){
       if(this.searchval){
          window.open("https://docs.tdengine.com/search?" + this.searchval)
          this.searchval = ''
          this.$emit("update:hidden", false);
       }
    }
  },
};
</script>

<style scoped>
.popsearch {
	/* 定位 */
	position: fixed;
	top: 0;
	left: 0;
	right: 0;
	bottom: 0;
	/* 显示 */
	z-index: 1000;
	background-color: rgba(0, 0, 0, 0.6);
	transition: z-index 0.4s, opacity 0.4s;  
}
.popsearch-hidden {
  display: none;
	margin-top: -1000%;
	opacity: 0;
	z-index: -101;
}

.popsearch-container{
  position: fixed;
  z-index: 99999;
  top: calc(30%);
  left: calc(25%);
  width: 100%;
}

.searchBar {
  padding: 5px;
  margin-top: 16px;
  border: none;
  border-radius: 0.2rem;
  display: flex;
  flex-direction: row;
  background-color: #FFFFFF;
  width: 50%;
}

.popsearch-container .searchBar  input{
  height: 45px;
  font-size: 20px;
  border: none ;
  border-radius: 0.2rem;
  background-color: #FFFFFF;
  width: 100%;
  outline: none;
}
.popsearch-container .searchBar input:focus {
  border: none ;
}
.searchIcon {
  color: var(--ifm-color-gray-600);
}
.iconLabel {
  height: 100%;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin: 0 0.5rem;
  padding: 0;
}
.iconLabel:hover {
  cursor: pointer;
}
</style>