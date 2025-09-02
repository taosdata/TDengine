<!-- IDMPDownload.vue -->
<template>
    <div class="page-wrapper">
        <!-- <PageHeader :title="$t('idmp.title')"></PageHeader> -->
        <div class="content">
            <el-card class="idmp-card" shadow="hover">
                <div class="idmp-content">
                <p class="idmp-text">
                    {{ $t('idmp.content') }}
                </p>
                <div class="btn-wrapper">
                    <el-button
                    type="primary"
                    class="download-btn"
                    size="large"
                    @click="goDownload"
                    >
                    {{ $t('idmp.download') }}
                    </el-button>
                </div>
                </div>
            </el-card>
        </div>
    </div>
</template>

<script setup>
import { ElCard, ElButton } from 'element-plus';
import { getLocalLang, decrypt } from '@/utils';

function goDownload() {
    if (getLocalLang() === 'en') {
        window.open('https://idmpdocs.tdengine.com/en/get-started/get-started-docker ', '_blank')
    } else {
        window.open('https://idmpdocs.taosdata.com/get-started/get-started-docker', '_blank')
    }
}
</script>

<style scoped>
.idmp-card {
  max-width: 700px;
  margin: 48px auto;
  padding: 40px 32px;
  border-radius: 12px;
  background: #fff;
  box-shadow: 0 2px 12px rgba(0,0,0,0.06);
}

.idmp-content {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
}

.idmp-text {
  font-size: 22px;
  color: #222;
  line-height: 1.7;
  margin-bottom: 24px;
  font-weight: 500;
}

.btn-wrapper {
  width: 100%;
  display: flex;
  justify-content: center;
}

.download-btn {
  font-size: 20px;
  font-weight: 600;
  padding: 12px 32px;
}
</style>
