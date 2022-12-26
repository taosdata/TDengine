import { sendSQLReq, getPaginationData } from "@/api/gateway/console";
export function getStreams(params) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_streams;`;
  const dataSql = `select * from information_schema.ins_streams`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function createStream(sql) {
  return sendSQLReq(sql);
}

export function delStream(name) {
  return sendSQLReq("DROP STREAM " + name);
}
