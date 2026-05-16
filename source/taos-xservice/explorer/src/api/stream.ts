import { sendSQLReq, getPaginationData } from "@/api/explorer";
interface Params {
  currentPage: number | string;
  pageSize: number | string;
}
export function getStreams(params: Params) {
  const { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_streams;`;
  const dataSql = `select * from information_schema.ins_streams`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function createStream(sql: string) {
  return sendSQLReq(sql);
}

export function delStream(name: string) {
  return sendSQLReq("DROP STREAM " + name);
}
