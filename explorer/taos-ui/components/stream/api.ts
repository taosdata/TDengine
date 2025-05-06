import { executeSqlFn } from '../api';

export function getStreams() {
  return executeSqlFn!(`select * from information_schema.ins_streams`, true);
}

export function createStream(sql: string) {
  return executeSqlFn!(sql);
}

export function delStream(name: string) {
  return executeSqlFn!(`DROP STREAM \`${name}\``);
}

export const streamList = ref<Recordable[]>([]);
