import { getPaginationData } from "@/api/gateway/console";
export function getAudits(params) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from audit.operations;`;
  const dataSql = `SELECT * FROM audit.operations ORDER BY ts DESC`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}