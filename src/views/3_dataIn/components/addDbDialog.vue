<template>
   <el-dialog 
      :visible.sync="$store.state.dbs.dialogDbVisible" 
      :close-on-press-escape="false" 
      :close-on-click-modal="false"
      width="62%"
      >
      <CretedDb></CretedDb>
    </el-dialog>
</template>

<script>
import CretedDb from '../../2_explorer/views/components/databases/db_create'
export default {
  name: 'DialogCreateDb',
  components: {
    CretedDb
  },
}
</script>
