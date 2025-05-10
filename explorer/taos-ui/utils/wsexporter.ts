import streamSaver from 'streamsaver';
import { connect, TaosResult } from '@tdengine/websocket';
import { json2csv } from 'json-2-csv';
import FileSaver from 'file-saver';

declare global {
  interface Window {
    showSaveFilePicker: () => Promise<any>;
  }
}

/**
 * 因 json2csv 库不支持 BigInt 类型，而连接器用 bigint 表示时间戳，所以需要提前将 BigInt 转换为 Number。
 */
export function convertData(rows: Recordable[]) {
  for (let i = 0; i < rows.length; ++i) {
    const row = rows[i];
    for (let j = 0; j < row.length; ++j) {
      if (typeof row[j] === 'bigint') {
        row[j] = Number(row[j]);
      }
    }
  }
  return rows;
}

function getFileName() {
  const d = new Date();
  const month = d.getMonth() < 9 ? '0' + (d.getMonth() + 1) : d.getMonth() + 1;
  const date = d.getDate() < 10 ? '0' + d.getDate() : d.getDate();
  const hours = d.getHours() < 10 ? '0' + d.getHours() : d.getHours();
  const minutes = d.getMinutes() < 10 ? '0' + d.getMinutes() : d.getMinutes();
  const seconds = d.getSeconds() < 10 ? '0' + d.getSeconds() : d.getSeconds();
  return 'data-' + d.getFullYear() + month + date + hours + minutes + seconds + '.csv';
}

/**
 * 使用 websocket 协议连接 TDengine 实例，执行查询并将查询结果转为 csv 格式写入本地文件。
 * @param {*} gatewayURL
 * @param {*} token
 * @param {*} sql 查询语句。如果不是查询语句会弹出警告
 */
export async function wsExport(gatewayURL: string, token: string, sql: string, withHeaders: boolean) {
  const fileStream = streamSaver.createWriteStream(getFileName());
  const writer = fileStream.getWriter();
  const dsn = gatewayURL.replace('http', 'ws') + '/rest/ws?token=' + token;
  const ws = connect(dsn);
  let wsQueryResponse, result, wsInterface;
  try {
    await ws.connect();
    wsInterface = ws._wsInterface;
    wsQueryResponse = await wsInterface.query(sql);
    let wsFetchResponse = await wsInterface.fetch(wsQueryResponse);
    // generate the CSV header to the file
    let header = withHeaders;

    while (!wsFetchResponse.completed) {
      const taosResult = new TaosResult(wsQueryResponse);
      taosResult.setRows(wsFetchResponse);
      await wsInterface.fetchBlock(wsFetchResponse, taosResult);
      const data = convertData(taosResult.data!);
      const csvData = json2csv(data, {
        prependHeader: header
      });
      const csvBlob = new Blob([csvData], {
        type: 'text/csv;charset=utf-8;'
      });
      const readableStream = csvBlob.stream();
      const reader = readableStream.getReader();
      let res = await reader.read();
      while (!res.done) {
        await writer.write(res.value);
        res = await reader.read();
      }
      wsFetchResponse = await wsInterface.fetch(wsQueryResponse);
      if (header) {
        header = false;
      }
    }
  } catch (err) {
    writer.abort(err);
    result = err;
    console.log('download error:', err);
    return Promise.reject(err);
  } finally {
    wsQueryResponse && wsInterface!.freeResult(wsQueryResponse);
    !result && writer.close();
    ws.close();
  }
}

/**
 * 将本地数据直接导出
 */
export function localExport(queryResult: any) {
  const FileName = getFileName();
  const data = convertToCsvData(queryResult.data, queryResult.head);
  const blob = new Blob([data], {
    type: 'text/csv;charset=utf-8;'
  });
  FileSaver.saveAs(blob, FileName);
}

/**
 * 将table数据转成csv数据
 * @param {Array<Record<string, any>>} data 表格数据
 * @param {Array<string>} head 表头数据
 * @returns
 */
function convertToCsvData(data: any, head: any) {
  const csvHeader = handlerData(head);
  const csvRows = data.map((row: any) => {
    return handlerData(row);
  });
  return csvHeader + '\n' + csvRows.join('\n');
}

function handlerData(data: any) {
  return data
    .map((item: any) => {
      // 如果字段中包含逗号或双引号，则用双引号包裹，并且内部的双引号需要转义
      let field = item;
      if (item?.field) {
        field = item.field;
      }
      if (typeof field === 'string' && (field.includes(',') || field.includes('"'))) {
        return `"${field.replace(/"/g, '""')}"`;
      } else {
        return field;
      }
    })
    .join(',');
}
