import { getPaginationData } from "@/api/gateway/console";
export function getAudits(params) {
  let { currentPage, pageSize, conditions } = params;
  const countSql = `select count(*) from audit.operations ${conditions ? 'where' + conditions : ''};`;
  const dataSql = `SELECT * FROM audit.operations ${conditions ? 'where' + conditions : ''} ORDER BY ts DESC`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}