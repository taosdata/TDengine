import { getDataSources } from '@/api/community';
import i18n from '@/lang/index.ts';
import { sendSQLReq } from '@/api/explorer';

export default function () {
  const globalCustomProperties: any = inject('globalCustomProperties');
  const { $INDUSTRY } = globalCustomProperties;

  function getMetaShow(grantName: string) {
    const array = JSON.parse(localStorage.getItem('allLicenseNameData')) || [];
    const allLicenseNameData = array.map((item: { grant_name: any }) => item.grant_name);
    let result = getDataSources(i18n.global.locale.value);
    result = result.filter((item: { license_id: any }) => allLicenseNameData.includes(item.license_id));
    const dnodes = array.filter((item: { grant_name: string }) => item.grant_name == grantName);
    const dnodeNum = dnodes[0]?.limits?.split('/')[0];

    switch (grantName) {
      case 'dataIn':
        return $INDUSTRY ? result?.length > 1 : true;
      case 'dnodes':
        return $INDUSTRY ? dnodeNum > 1 : true;
      default:
        return $INDUSTRY ? allLicenseNameData.includes(grantName) : true;
    }
  }

  async function getGrantsFull() {
    // 行业版才调用接口
    if ($INDUSTRY) {
      const res = await sendSQLReq('show grants full;');
      const array = res.data.map((data: { [x: string]: any }) => {
        return Object.fromEntries(
          res.column_meta.map((item: any[], index: string | number) => {
            return [item[0], data[index]];
          })
        );
      });
      localStorage.setItem('allLicenseNameData', JSON.stringify(array));
    }
  }
  // 页面要用到的数据，都返回
  return { getMetaShow, getGrantsFull };
}
