<template>
    <div class="plant-info">
        <component :is="currentComp"></component>
        <!-- <DbSourceUI :dbsource='dbsource' :tagName='"pi"'></DbSourceUI> -->
    </div>
</template>
<script>
import dbsource from './datasource.json'
import DbSourceUI from './dbSourceUI.vue'
import PITable from './piTable.vue'
export default {
    name:'PlantInformation',
    components:{
        DbSourceUI,
        PITable
    },
    data(){
        return {
            currentComp:PITable,
            dbsource:[].concat(dbsource)
        }
    }
}
</script>