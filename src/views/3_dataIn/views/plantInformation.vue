<template>
    <div class="plant-info">
        <DbSourceUI :dbsource='dbsource'></DbSourceUI>
    </div>
</template>
<script>
import dbsource from './datasource.json'
import DbSourceUI from './dbSourceUI.vue'
export default {
    name:'PlantInformation',
    components:{
        DbSourceUI
    },
    data(){
        return {
            dbsource:[].concat(dbsource)
        }
    }
}
</script>