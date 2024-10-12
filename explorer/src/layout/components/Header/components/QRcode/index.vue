<template>
  <div class="qrcode_wrapper">
    <el-popover
      trigger="hover">
        <div class="qr_img_wrapper">
          <img :src="qrImg" class="qr_img"/>
        </div>
      <div slot="reference">
        <Icon name="qrcode" class="qrcode_svg"></Icon>
      </div>
    </el-popover>

  </div>
</template>

<script>
import Icon from "@/components/Icon";
import qrImg from '@/assets/images/qr_img.png'

export default {
  name: 'qrcode',
  components: { Icon },
  data() {
    return {
      qrImg: qrImg,
    };
  }
};
</script>

<style scoped>
.qrcode_wrapper{
  cursor: pointer;
}

.qrcode_svg{
  width: 22px;
  height: 22px;
  margin-top: 6px;
}

.qr_img_wrapper{
  display: flex;
  flex-direction: column;
  align-items: center;
}

.qr_img{
  width: 120px;
  height: 120px;
}
</style>
